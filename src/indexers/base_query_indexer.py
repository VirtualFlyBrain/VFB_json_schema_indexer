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
    Crawls a VFB_json_schema service with all possible parameters and generates related solr indexes.
    """

    REQUEST_BATCH_SIZE = int(os.getenv('BATCH_SIZE', 500))

    def __init__(self) -> None:
        self.ql = QueryLibrary()
        self.nc = Neo4jConnect(os.environ["PDBserver"], os.environ["PDBuser"], os.environ["PDBpassword"])

    def generate_index(self) -> Dict[str, Dict]:
        """
        Crawls VFB_json_schema api with all possible parameters and generates the solr index data.
        :return: dictionary of solr data. Short_form as key, solr data as value
        """
        ids = self.get_query_parameters()
        return self.crawl_vfb_json_data(ids)

    def crawl_vfb_json_data(self, ids: List[str]) -> None:
        start_time = datetime.datetime.now()
        batch_size = self.REQUEST_BATCH_SIZE
        log.info(f"Crawling: '{self.get_service_name()}' ({self.__class__.__name__}), "
                f"Batch size: {batch_size}, Start time: {start_time}")
        chunks = get_chunks(ids, batch_size)
        vfb_json_query_template = self.get_vfb_json_query(['$ID'])

        neo4j_import_dir = '/import'  # Neo4j server import directory
        jenkins_import_dir = '/PDBupgrade/import'  # Jenkins accessible directory

        for i, chunk in enumerate(tqdm(chunks, total=int(math.ceil(len(ids) / batch_size)))):
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

            # Process the file
            self.process_exported_file(exported_file_path)

            # Remove the file after processing
            os.remove(exported_file_path)

        end_time = datetime.datetime.now()
        diff = end_time - start_time
        log.info(f"All data crawled in {diff.total_seconds() / 60.0} minutes")

    def process_exported_file(self, file_path: str) -> None:
        """
        Processes the exported JSON file and generates solr documents.
        :param file_path: Path to the exported JSON file
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)  # Load the JSON data
            # If the data is a list of records, process each one
            if isinstance(data, list):
                for result in data:
                    solr_data = self.generate_solr_doc(result, request=None)  # Adjust 'request' if needed
                    # Here, you can write solr_data to a local file or handle it as needed
                    self.write_solr_data(solr_data)
            else:
                # If data is a single record
                solr_data = self.generate_solr_doc(data, request=None)
                self.write_solr_data(solr_data)

    def write_solr_data(self, solr_data: Dict) -> None:
        """
        Handles the solr data generated from the result.
        :param solr_data: Solr document data
        """
        # For example, write to a local file
        with open('solr_data.jsonl', 'a', encoding='utf-8') as f:
            json_line = json.dumps(solr_data)
            f.write(json_line + '\n')


    def generate_solr_doc(self, result: Dict, request: List[str]) -> Dict[str, str]:
        """
        Parses results and generates a solr doc to index.
        :param result: service response
        :param request: requests to get requested entity id if provided
        :return: solr document
        """
        solr_doc = dict()
        if self.REQUEST_BATCH_SIZE == 1 and request[0]:
            solr_doc["id"] = request[0]
        elif "term" in result:
            solr_doc["id"] = result["term"]["core"]["short_form"]
        elif "dataset" in result:
            solr_doc["id"] = result["dataset"]["short_form"]
        else:
            raise ValueError("Unrecognised response data: " + json.dumps(result)[:50] + " ...")
        solr_doc[self.get_service_name()] = json.dumps(result)
        return solr_doc

    def get_query_parameters(self) -> List[str]:
        """
        Executes get_parameters_query to retrieve all possible service parameters and unpacks the response.
        :return: list of short_forms
        """
        parameters = list()
        if self.get_parameters_query():
            response = self.execute_query(self.get_parameters_query())
            parameters = response[0]["ids"]
        else:
            # add dummy param to trigger single execution
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
            {{batchSize: 1000}}
        )
        """

        # Prepare the statement with parameters
        cstatements = [{
            'statement': export_query,
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
                log.info(f"Exported data to {output_file}")
        except requests.exceptions.RequestException as e:
            log.warning(str(e))
            if try_count < 10:
                time.sleep(30 + try_count * 15)
                return self.execute_query(query, params=params, output_file=output_file, try_count=try_count + 1)
            else:
                raise Neo4jQueryException(self.get_service_name() + " query failed: " + str(e))


    @abstractmethod
    def get_parameters_query(self) -> Optional[str]:
        """
        Cypyher query to to list short forms of all nodes that can be passed as parameter to this service. Query should
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
