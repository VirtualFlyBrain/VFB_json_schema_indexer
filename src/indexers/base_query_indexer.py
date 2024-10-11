import os
import math
import json
import logging
import datetime
import time
from typing import List, Dict, Generator, Optional
from abc import ABC, abstractmethod
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

    def crawl_vfb_json_data(self, ids: List[str]) -> Dict[str, Dict]:
        """
        Crawls the VFB_json_schema service and generates solr indexes.
        :param ids: list of short_forms to query
        :return: dictionary of solr data. Short_form as key, solr data as value
        """
        start_time = datetime.datetime.now()
        batch_size = self.REQUEST_BATCH_SIZE
        log.info("Crawling: '" + self.get_service_name() + "' (" + self.__class__.__name__ + ")"
                 + ", Batch size:" + str(batch_size) + ", Start time: " + str(start_time))
        all_data = dict()
        chunks = get_chunks(ids, batch_size)
        vfb_json_query_template = self.get_vfb_json_query(['$ID'])
        vfb_json_query = vfb_json_query_template.replace("['$ID']", "$ids")
        for chunk in tqdm(chunks, total=int(math.ceil(len(ids) / batch_size))):
            results = self.execute_query(vfb_json_query, params={'ids': chunk})
            if results:
                for result in results:
                    solr_data = self.generate_solr_doc(result, chunk)
                    all_data[solr_data["id"]] = solr_data
            else:
                log.error("No results for chunk: " + str(chunk))
        end_time = datetime.datetime.now()
        diff = end_time - start_time
        log.info("All data crawled in " + str(diff.total_seconds() / 60.0) + " minutes")
        return all_data

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

    def execute_query(self, query: str, try_count=0) -> List[Dict]:
        """
        Executes given cypher query in the neo4j
        :param query: query to execute
        :param try_count: try count
        :return: query results as a list of dicts
        """
        results = list()
        try:
            s = self.nc.commit_list([query])
        except SocketError as e:
            log.warning(str(e))
            if try_count < 10:
                time.sleep(30 + try_count * 15)
                self.nc = Neo4jConnect(os.environ["PDBserver"], os.environ["PDBuser"], os.environ["PDBpassword"])
                return self.execute_query(query, try_count + 1)
            else:
                raise Neo4jQueryException(self.get_service_name() + " query failed :" + str(e))

        if s:
            results = dict_cursor(s)
        return results

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
