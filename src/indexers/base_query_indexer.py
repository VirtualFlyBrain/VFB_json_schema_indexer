import os
import math
import json
import logging
import datetime
import time
from typing import List, Dict, Generator, Optional
from abc import ABC, abstractmethod

import requests
from vfb_connect.neo.neo4j_tools import Neo4jConnect, dict_cursor
from src.vfb.vfb_query_builder.query_roller import QueryLibrary
from tqdm import tqdm
from socket import error as SocketError

log = logging.getLogger(__name__)


class BaseQueryIndexer(ABC):
    """
    Base query class that provides an abstraction for the concrete service crawlers.
    Crawls a VFB_json_schema service with all possible parameters and generates related Solr indexes.
    """

    REQUEST_BATCH_SIZE = int(os.getenv('BATCH_SIZE', 500))

    def __init__(self) -> None:
        self.ql = QueryLibrary()
        self.nc = Neo4jConnect(os.environ["PDBserver"], os.environ["PDBuser"], os.environ["PDBpassword"])

    def generate_index(self) -> Dict[str, Dict]:
        """
        Crawls VFB_json_schema API with all possible parameters and generates the Solr index data.
        :return: Dictionary of Solr data. Short_form as key, Solr data as value
        """
        ids = self.get_query_parameters()
        index_data = self.crawl_vfb_json_data(ids)
        return index_data

    def crawl_vfb_json_data(self, ids: List[str]) -> Dict[str, Dict]:
        start_time = datetime.datetime.now()
        batch_size = self.REQUEST_BATCH_SIZE
        log.info(f"Crawling: '{self.get_service_name()}' ({self.__class__.__name__}), "
                 f"Batch size: {batch_size}, Start time: {start_time}")
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

    def execute_query(self, query: str, params: Dict = None, output_file: str = None, try_count=0) -> None:
        """
        Executes given Cypher query in Neo4j and exports the result to a file on the server.
        :param query: Cypher query to execute
        :param params: Query parameters
        :param output_file: Path to the output file on the Neo4j server (e.g., "file:///import/output.json")
        :param try_count: Retry count
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

        payload = {'statements': cstatements}
        headers = {'Content-Type': 'application/json'}

        try:
            response = requests.post(
                url=f"{self.nc.base_uri}{self.nc.commit}",
                auth=(self.nc.usr, self.nc.pwd),
                data=json.dumps(payload),
                headers=headers
            )
            response.raise_for_status()
            response_json = response.json()
            if 'errors' in response_json and response_json['errors']:
                log.error(f"Neo4j returned errors: {response_json['errors']}")
                raise Neo4jQueryException(f"Query failed with errors: {response_json['errors']}")
            else:
                log.info(f"Exported data For {params['ids'][0]} to {params['ids'][-1]}")
        except requests.exceptions.RequestException as e:
            log.warning(str(e))
            if try_count < 10:
                time.sleep(30 + try_count * 15)
                return self.execute_query(query, params=params, output_file=output_file, try_count=try_count + 1)
            else:
                raise Neo4jQueryException(self.get_service_name() + " query failed: " + str(e))

    def run_query(self, query: str, params: Dict = None, try_count=0) -> List[Dict]:
        """
        Executes given Cypher query in Neo4j and returns the results.
        """
        results = []
        cstatements = [{
            'statement': query,
            'parameters': params or {}
        }]

        payload = {'statements': cstatements}
        headers = {'Content-Type': 'application/json'}

        try:
            response = requests.post(
                url=f"{self.nc.base_uri}{self.nc.commit}",
                auth=(self.nc.usr, self.nc.pwd),
                data=json.dumps(payload),
                headers=headers
            )
            response.raise_for_status()
            response_json = response.json()
            if 'errors' in response_json and response_json['errors']:
                log.error(f"Neo4j returned errors: {response_json['errors']}")
                raise Neo4jQueryException(f"Query failed with errors: {response_json['errors']}")
            else:
                # Process results into a list of dicts
                for result in response_json['results']:
                    columns = result['columns']
                    for data_row in result['data']:
                        row = data_row['row']
                        result_dict = dict(zip(columns, row))
                        results.append(result_dict)
        except requests.exceptions.RequestException as e:
            log.warning(str(e))
            if try_count < 10:
                time.sleep(30 + try_count * 15)
                return self.run_query(query, params=params, try_count=try_count + 1)
            else:
                raise Neo4jQueryException(self.get_service_name() + " query failed: " + str(e))
        return results

    def write_to_solr(self, solr_docs: Dict[str, Dict]) -> None:
        """
        Writes a batch of Solr documents to the Solr server using atomic updates.
        :param solr_docs: Dictionary of Solr documents to index.
        """
        solr_server = os.getenv('SOLRserver')
        solr_collection = os.getenv('SOLRcollection')

        if not solr_server or not solr_collection:
            log.error("SOLRserver or SOLRcollection environment variable is not set.")
            return

        solr_update_url = f"{solr_server.rstrip('/')}/{solr_collection}/update"

        headers = {'Content-Type': 'application/json'}
        solr_data_list = list(solr_docs.values())

        # Include commit within the same POST request
        params = {'commit': 'true'}

        # Log the Solr update URL and data being sent for debugging
        log.debug(f"Solr update URL: {solr_update_url}")
        log.debug(f"Data being sent to Solr: {json.dumps(solr_data_list, indent=2)}")

        try:
            response = requests.post(
                solr_update_url,
                params=params,
                data=json.dumps(solr_data_list),
                headers=headers,
                timeout=60  # Increase timeout if necessary
            )
            response.raise_for_status()
            log.info(f"Indexed {len(solr_data_list)} documents to Solr using atomic updates and committed changes.")
            log.debug(f"Solr response: {response.text}")
        except requests.exceptions.RequestException as e:
            log.error(f"Failed to index documents to Solr: {e}")
            if response is not None:
                log.error(f"Solr response: {response.text}")

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
