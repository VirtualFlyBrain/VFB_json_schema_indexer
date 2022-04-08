import os
import math
import json
import logging
import datetime
from abc import ABC, abstractmethod
from vfb_connect.neo.neo4j_tools import Neo4jConnect, dict_cursor
from src.vfb.vfb_query_builder.query_roller import QueryLibrary
from tqdm import tqdm

log = logging.getLogger(__name__)


class BaseQueryIndexer(ABC):
    """
    Base query class that provides an abstraction for the concrete service crawlers.
    Crawls a VFB_json_schema service with all possible parameters and generates related solr indexes.
    """

    REQUEST_BATCH_SIZE = 500

    def __init__(self):
        self.ql = QueryLibrary()
        self.nc = Neo4jConnect(os.environ["PDBserver"], os.environ["PDBuser"], os.environ["PDBpassword"])

    def generate_index(self):
        """
        Crawls VFB_json_schema api with all possible parameters and generates the solr index data.
        :return: dictionary of solr data. Short_form as key, solr data as value
        """
        ids = self.get_query_parameters()
        return self.crawl_vfb_json_data(ids)

    def crawl_vfb_json_data(self, ids):
        """
        Crawls the VFB_json_schema service and generates solr indexes.
        :param ids: list of short_forms to query
        :return: dictionary of solr data. Short_form as key, solr data as value
        """
        start_time = datetime.datetime.now()
        batch_size = os.getenv('BatchSize', self.REQUEST_BATCH_SIZE)
        log.info("Crawling: '" + self.get_service_name() + "', Batch size:" + str(batch_size) +
                 ", Start time: " + str(start_time))
        all_data = dict()

        chunks = get_chunks(ids, batch_size)
        for chunk in tqdm(chunks, total=int(math.ceil(len(ids) / batch_size))):
            vfb_json_query = self.get_vfb_json_query(chunk)
            results = self.execute_query(vfb_json_query)

            for result in results:
                solr_data = self.generate_solr_doc(result)
                all_data[solr_data["id"]] = solr_data
        end_time = datetime.datetime.now()
        diff = end_time - start_time
        log.info("All data crawled in " + str(diff.total_seconds() / 60.0) + " minutes")
        return all_data

    def generate_solr_doc(self, result):
        """
        Parses results and generates a solr doc to index.
        :param result: service response
        :return: solr document
        """
        solr_doc = dict()
        solr_doc["id"] = result["term"]["core"]["short_form"]
        solr_doc[self.get_service_name()] = json.dumps(result)
        return solr_doc

    def get_query_parameters(self):
        """
        Executes get_parameters_query to retrieve all possible service parameters and unpacks the response.
        :return: list of short_forms
        """
        parameters = self.execute_query(self.get_parameters_query())
        return parameters[0]["ids"]

    def execute_query(self, query):
        """
        Executes given cypher query in the neo4j
        :param query: query to execute
        :return: query results as a list of dicts
        """
        results = list()
        s = self.nc.commit_list([query])
        if s:
            results = dict_cursor(s)
        return results

    @abstractmethod
    def get_parameters_query(self):
        """
        Cypyher query to to list short forms of all nodes that can be passed as parameter to this service. Query should
        return 'ids' as result such as 'RETURN collect(distinct n.short_form) as ids'.
        :return: Cypher query string
        """
        pass

    @abstractmethod
    def get_vfb_json_query(self, ids):
        """
        Returns the query rolled by the vfb_json_schema.
        :param ids: ids to query
        :return: query string
        """
        pass

    @abstractmethod
    def get_service_name(self):
        """
        Returns the name of the current service. This name is used as part of the index to provide faster access.
        :return: name of the current service to index
        """
        pass


def get_chunks(lst, n):
    """
    Yield successive n-sized chunks from lst.
    :return: chunk generator
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
