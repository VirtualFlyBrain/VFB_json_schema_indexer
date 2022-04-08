import os
import unittest
from src.indexers.base_query_indexer import BaseQueryIndexer

TEST_SERVICE_NAME = "test_query"
TEST_OUTPUT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../indexes/{}_index.json")\
    .format(TEST_SERVICE_NAME)


class TemplateGenerationTest(unittest.TestCase):

    def setUp(self):
        # TODO should I hide password?
        os.environ["PDBserver"] = "http://pdb-test.virtualflybrain.org"
        os.environ["PDBuser"] = "neo4j"

    def test_parameter_preparation(self):
        parameters = TestQueryIndexer().get_query_parameters()

        self.assertTrue(parameters)
        self.assertTrue(len(parameters) == 10)

    def test_vfb_json_preparation(self):
        query = TestQueryIndexer().get_vfb_json_query(["VFB_00002007", "VFB_00002009", "VFB_00002016"])

        self.assertTrue(query)
        self.assertTrue(str(query).startswith("MATCH (primary:Individual) WHERE primary.short_form in ['VFB_00002007', "
                                              "'VFB_00002009', 'VFB_00002016']"))
        self.assertTrue(str(query).endswith(" version , channel_image, types"))

    def test_crawling(self):
        solr_docs = TestQueryIndexer().crawl_vfb_json_data(["VFB_00002007", "VFB_00002009", "VFB_00002016"])

        self.assertTrue(solr_docs)
        self.assertEqual(3, len(list(solr_docs.keys())))
        self.assertTrue("VFB_00002007" in solr_docs)
        self.assertEqual("VFB_00002007", solr_docs["VFB_00002007"]["id"])
        self.assertTrue(TEST_SERVICE_NAME in solr_docs["VFB_00002007"])
        self.assertTrue("VFB_00002009" in solr_docs)
        self.assertEqual("VFB_00002009", solr_docs["VFB_00002009"]["id"])
        self.assertTrue(TEST_SERVICE_NAME in solr_docs["VFB_00002009"])
        self.assertTrue("VFB_00002016" in solr_docs)
        self.assertEqual("VFB_00002016", solr_docs["VFB_00002016"]["id"])
        self.assertTrue(TEST_SERVICE_NAME in solr_docs["VFB_00002016"])


class TestQueryIndexer(BaseQueryIndexer):

    def get_parameters_query(self):
        return "MATCH (n:has_image:Individual) WITH distinct n LIMIT 10 RETURN collect(distinct n.short_form) as ids"

    def get_vfb_json_query(self, ids):
        return self.ql.anat_image_query(short_forms=ids)

    def get_service_name(self):
        return TEST_SERVICE_NAME
