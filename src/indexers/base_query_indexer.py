import os
import math
import json
import logging
import datetime
import time
from typing import List, Dict, Generator, Optional, Tuple
from abc import ABC, abstractmethod

import requests
from vfb_connect.neo.neo4j_tools import Neo4jConnect, dict_cursor
from src.vfb.vfb_query_builder.query_roller import QueryLibrary
from src.solr_client import send_solr_docs, send_final_commit, fetch_ids_with_field
from tqdm import tqdm
from socket import error as SocketError

log = logging.getLogger(__name__)


NEO4J_MAX_RETRY_WAIT_SECONDS = int(os.getenv("NEO4J_MAX_RETRY_WAIT_SECONDS", 1200))  # 20 min
NEO4J_RETRY_INITIAL_DELAY_SECONDS = int(os.getenv("NEO4J_RETRY_INITIAL_DELAY_SECONDS", 5))
NEO4J_RETRY_MAX_DELAY_SECONDS = int(os.getenv("NEO4J_RETRY_MAX_DELAY_SECONDS", 120))
NEO4J_HEALTH_CHECK_INTERVAL = 30  # seconds between pings


class BaseQueryIndexer(ABC):
    """
    Base query class that provides an abstraction for the concrete service crawlers.
    Crawls a VFB_json_schema service with all possible parameters and generates related Solr indexes.
    """

    REQUEST_BATCH_SIZE = int(os.getenv('BATCH_SIZE', 2000))
    def __init__(self) -> None:
        self.ql = QueryLibrary()
        self.nc = Neo4jConnect(os.environ["PDBserver"], os.environ["PDBuser"], os.environ["PDBpassword"])
        self._id_partition: Optional[Tuple[List[str], List[str]]] = None

    def partition_ids(self) -> Tuple[List[str], List[str]]:
        """Partition this indexer's parameter ids into (missing, stale) by
        probing Solr once. Cached on the instance so repeat calls (phase 1 and
        phase 2 of the global run) don't re-probe — and so phase 2 still sees
        the ORIGINAL stale set even though phase 1 has since populated those
        ids.
        """
        if self._id_partition is None:
            ids = self.get_query_parameters()
            service_name = self.get_service_name()
            existing = fetch_ids_with_field(ids, service_name)
            missing = [i for i in ids if i not in existing]
            stale = [i for i in ids if i in existing]
            log.info(
                f"{service_name}: {len(missing)} missing, {len(stale)} existing."
            )
            self._id_partition = (missing, stale)
        return self._id_partition

    def generate_index(self, phase: str = "all") -> Dict[str, Dict]:
        """
        Crawls VFB_json_schema API and generates Solr index data for the
        requested phase.

        :param phase: "missing" (docs absent from Solr), "stale" (docs present
            in Solr, refreshing them), or "all" (every id, no probing).
        :return: Dictionary of Solr data. Short_form as key, Solr data as value
        """
        if phase == "all":
            ids = self.get_query_parameters()
        else:
            missing, stale = self.partition_ids()
            if phase == "missing":
                ids = missing
            elif phase == "stale":
                ids = stale
            else:
                raise ValueError(f"Unknown phase: {phase}")

        if not ids:
            log.info(
                f"{self.get_service_name()}: nothing to do for phase '{phase}'."
            )
            return {}

        return self.crawl_vfb_json_data(ids)

    def crawl_vfb_json_data(self, ids: List[str]) -> Dict[str, Dict]:
        start_time = datetime.datetime.now()
        batch_size = self.REQUEST_BATCH_SIZE
        service_name = self.get_service_name()
        log.info(f"Crawling: '{service_name}' ({self.__class__.__name__}), "
                 f"Batch size: {batch_size}, ids: {len(ids)}, Start time: {start_time}")

        chunks = get_chunks(ids, batch_size)
        vfb_json_query_template = self.get_vfb_json_query(['$ID'])

        neo4j_import_dir = '/import'  # Neo4j server import directory
        jenkins_import_dir = '/PDBupgrade/import'  # Jenkins accessible directory

        all_data = {}  # Initialize the dictionary to collect Solr documents

        for i, chunk in enumerate(tqdm(chunks, total=int(math.ceil(len(ids) / batch_size)), desc=self.get_service_name())):
            # Prepare the query
            vfb_json_query = vfb_json_query_template.replace("['$ID']", "$ids")

            # Set the output file path
            output_filename = f"output_{i}.json"
            output_file_path = f"file://{neo4j_import_dir}/{output_filename}"

            # Execute the query and export to file
            self.execute_query(vfb_json_query, params={'ids': chunk}, output_file=output_file_path)

            # Wait for the file to be written
            exported_file_path = os.path.join(jenkins_import_dir, output_filename)
            while not os.path.exists(exported_file_path):
                time.sleep(1)  # Wait for 1 second before checking again

            # Process the file and collect the data
            batch_data = self.process_exported_file(exported_file_path)

            # Write batch_data to Solr
            self.write_to_solr(batch_data)

            # Update the all_data dictionary with the batch data
            all_data.update(batch_data)

            # Remove the file after processing
            os.remove(exported_file_path)

        # Issue a single hard commit for this service now that all batches have
        # been POSTed. Individual batches use commitWithin (see solr_client.py)
        # so we avoid thousands of per-batch Lucene flushes.
        send_final_commit(self.get_service_name())

        end_time = datetime.datetime.now()
        diff = end_time - start_time
        log.info(f"All data crawled and indexed in {diff.total_seconds() / 60.0} minutes")

        return all_data  # Return the combined dictionary

    def process_exported_file(self, file_path: str) -> Dict[str, Dict]:
        """
        Processes the exported JSON file and generates Solr documents.
        :param file_path: Path to the exported JSON file
        :return: Dictionary of Solr documents
        """
        batch_data = {}
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_number, line in enumerate(f, start=1):
                line = line.strip()
                if line:
                    try:
                        result = json.loads(line)
                        solr_data = self.generate_solr_doc(result, request=None)
                        batch_data[solr_data["id"]] = solr_data
                    except json.JSONDecodeError as e:
                        log.error(f"JSON decoding error in file {file_path} at line {line_number}: {e}")
        return batch_data

    def generate_solr_doc(self, result: Dict, request: List[str]) -> Dict[str, Dict]:
        """
        Parses results and generates a Solr document to index using atomic updates.
        :param result: service response
        :param request: requests to get requested entity id if provided
        :return: Solr document in atomic update format
        """
        if self.REQUEST_BATCH_SIZE == 1 and request and request[0]:
            doc_id = request[0]
        elif "term" in result:
            doc_id = result["term"]["core"]["short_form"]
        elif "dataset" in result:
            doc_id = result["dataset"]["short_form"]
        elif "anatomy" in result:
            doc_id = result["anatomy"]["short_form"]
        else:
            raise ValueError("Unrecognised response data: " + json.dumps(result)[:50] + " ...")

        solr_doc = {
            "id": doc_id,
            self.get_service_name(): {"set": json.dumps(result)}
        }
        return solr_doc

    def get_query_parameters(self) -> List[str]:
        parameters = []
        query = self.get_parameters_query()
        if query:
            response = self.run_query(query)
            if response and "ids" in response[0]:
                parameters = response[0]["ids"]
        else:
            parameters.append("")
        return parameters

    def _wait_for_neo4j(self, deadline: float) -> bool:
        """Lightweight ping — polls Neo4j until it responds or deadline passes."""
        url = f"{self.nc.base_uri}{self.nc.commit}"
        # Send a trivial read-only query as a health check.
        ping_payload = json.dumps({'statements': [{'statement': 'RETURN 1'}]})
        headers = {'Content-Type': 'application/json'}

        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return False
            try:
                r = requests.post(
                    url=url,
                    auth=(self.nc.usr, self.nc.pwd),
                    data=ping_payload,
                    headers=headers,
                    timeout=10,
                )
                if r.status_code == 200:
                    return True
                log.info(f"Neo4j ping returned {r.status_code}, waiting...")
            except requests.exceptions.RequestException:
                pass

            remaining = deadline - time.time()
            if remaining <= 0:
                return False
            sleep_time = min(NEO4J_HEALTH_CHECK_INTERVAL, remaining)
            log.info(
                f"Waiting {sleep_time:.0f}s for Neo4j to become available... "
                f"({remaining:.0f}s remaining before giving up)"
            )
            time.sleep(sleep_time)

    def execute_query(self, query: str, params: Dict = None, output_file: str = None) -> None:
        """
        Executes given Cypher query in Neo4j and exports the result to a file on the server.
        :param query: Cypher query to execute
        :param params: Query parameters
        :param output_file: Path to the output file on the Neo4j server (e.g., "file:///import/output.json")
        """
        # Escape double quotes in the query
        escaped_query = query.replace('"', '\\"')

        # Construct the export query
        export_query = f"""
        CALL apoc.export.json.query(
            "{escaped_query}",
            "{output_file}",
            {{batchSize: 20, params: $params}}
        )
        """

        # Prepare the statement with parameters
        cstatements = [{
            'statement': export_query,
            'parameters': {'params': params} if params else {}
        }]

        payload = json.dumps({'statements': cstatements})
        headers = {'Content-Type': 'application/json'}

        deadline = time.time() + NEO4J_MAX_RETRY_WAIT_SECONDS
        attempt = 0

        while True:
            try:
                response = requests.post(
                    url=f"{self.nc.base_uri}{self.nc.commit}",
                    auth=(self.nc.usr, self.nc.pwd),
                    data=payload,
                    headers=headers
                )
                response.raise_for_status()
                response_json = response.json()
                if 'errors' in response_json and response_json['errors']:
                    log.error(f"Neo4j returned errors: {response_json['errors']}")
                    raise Neo4jQueryException(f"Query failed with errors: {response_json['errors']}")
                log.info(f"Exported data For {params['ids'][0]} to {params['ids'][-1]}")
                return
            except requests.exceptions.RequestException as e:
                attempt += 1
                log.warning(str(e))

                remaining = deadline - time.time()
                if remaining <= 0:
                    raise Neo4jQueryException(
                        f"{self.get_service_name()} query failed after {attempt} attempts "
                        f"({NEO4J_MAX_RETRY_WAIT_SECONDS}s deadline exceeded): {e}"
                    )

                log.info(
                    f"Waiting for Neo4j to become available before retrying "
                    f"({remaining:.0f}s remaining)..."
                )
                if not self._wait_for_neo4j(deadline):
                    raise Neo4jQueryException(
                        f"Neo4j did not become available within "
                        f"{NEO4J_MAX_RETRY_WAIT_SECONDS}s for {self.get_service_name()}: {e}"
                    )

                # Brief back-off after ping succeeds.
                backoff = min(
                    NEO4J_RETRY_INITIAL_DELAY_SECONDS * (2 ** (attempt - 1)),
                    NEO4J_RETRY_MAX_DELAY_SECONDS,
                )
                backoff = min(backoff, deadline - time.time())
                if backoff > 0:
                    log.info(
                        f"Neo4j is back. Retrying query for {self.get_service_name()} "
                        f"in {backoff:.0f}s (attempt {attempt + 1})."
                    )
                    time.sleep(backoff)

    def run_query(self, query: str, params: Dict = None) -> List[Dict]:
        """
        Executes given Cypher query in Neo4j and returns the results.
        """
        cstatements = [{
            'statement': query,
            'parameters': params or {}
        }]

        payload = json.dumps({'statements': cstatements})
        headers = {'Content-Type': 'application/json'}

        deadline = time.time() + NEO4J_MAX_RETRY_WAIT_SECONDS
        attempt = 0

        while True:
            try:
                response = requests.post(
                    url=f"{self.nc.base_uri}{self.nc.commit}",
                    auth=(self.nc.usr, self.nc.pwd),
                    data=payload,
                    headers=headers
                )
                response.raise_for_status()
                response_json = response.json()
                if 'errors' in response_json and response_json['errors']:
                    log.error(f"Neo4j returned errors: {response_json['errors']}")
                    raise Neo4jQueryException(f"Query failed with errors: {response_json['errors']}")

                results = []
                for result in response_json['results']:
                    columns = result['columns']
                    for data_row in result['data']:
                        row = data_row['row']
                        result_dict = dict(zip(columns, row))
                        results.append(result_dict)
                return results
            except requests.exceptions.RequestException as e:
                attempt += 1
                log.warning(str(e))

                remaining = deadline - time.time()
                if remaining <= 0:
                    raise Neo4jQueryException(
                        f"{self.get_service_name()} query failed after {attempt} attempts "
                        f"({NEO4J_MAX_RETRY_WAIT_SECONDS}s deadline exceeded): {e}"
                    )

                log.info(
                    f"Waiting for Neo4j to become available before retrying "
                    f"({remaining:.0f}s remaining)..."
                )
                if not self._wait_for_neo4j(deadline):
                    raise Neo4jQueryException(
                        f"Neo4j did not become available within "
                        f"{NEO4J_MAX_RETRY_WAIT_SECONDS}s for {self.get_service_name()}: {e}"
                    )

                backoff = min(
                    NEO4J_RETRY_INITIAL_DELAY_SECONDS * (2 ** (attempt - 1)),
                    NEO4J_RETRY_MAX_DELAY_SECONDS,
                )
                backoff = min(backoff, deadline - time.time())
                if backoff > 0:
                    log.info(
                        f"Neo4j is back. Retrying query for {self.get_service_name()} "
                        f"in {backoff:.0f}s (attempt {attempt + 1})."
                    )
                    time.sleep(backoff)

    def write_to_solr(self, solr_docs: Dict[str, Dict]) -> None:
        """
        Writes a batch of Solr documents to the Solr server using atomic updates.
        :param solr_docs: Dictionary of Solr documents to index.
        """
        send_solr_docs(solr_docs.values(), self.get_service_name())

    @abstractmethod
    def get_parameters_query(self) -> Optional[str]:
        """
        Cypher query to list short forms of all nodes that can be passed as parameters to this service. Query should
        return 'ids' as result such as 'RETURN collect(distinct n.short_form) as ids'.
        :return: Cypher query string
        """
        pass

    @abstractmethod
    def get_vfb_json_query(self, ids: List[str]) -> str:
        """
        Returns the query rolled by the vfb_json_schema.
        :param ids: ids to query
        :return: query string
        """
        pass

    @abstractmethod
    def get_service_name(self) -> str:
        """
        Returns the name of the current service. This name is used as part of the index to provide faster access.
        :return: name of the current service to index
        """
        pass


class Neo4jQueryException(Exception):
    """
    Custom exception class.
    """
    pass


def get_chunks(lst: List, n: int) -> Generator:
    """
    Yield successive n-sized chunks from lst.
    :param lst: list to read by chunks
    :param n: max chunk size
    :return: chunk generator
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
