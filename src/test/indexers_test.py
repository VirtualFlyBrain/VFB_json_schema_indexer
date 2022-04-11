import os
import unittest
from src.indexers.base_query_indexer import BaseQueryIndexer
from src.indexers.anat_image_query_indexer import AnatImageQueryIndexer
from src.indexers.anat_query_indexer import AnatQueryIndexer
from src.indexers.anat_2_ep_query_indexer import Anat2EpQueryIndexer
from src.indexers.ep_2_anat_query_indexer import Ep2AnatQueryIndexer
from src.indexers.template_2_datasets_query_indexer import Template2DatasetsQueryIndexer
from src.indexers.all_datasets_query_indexer import AllDatasetsQueryIndexer

TEST_SERVICE_NAME = "test_query"
TEST_OUTPUT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../indexes/{}_index.json")\
    .format(TEST_SERVICE_NAME)


class TemplateGenerationTest(unittest.TestCase):

    def setUp(self):
        # TODO should I hide password?
        os.environ["PDBserver"] = "http://pdb-test.virtualflybrain.org"
        os.environ["PDBuser"] = "neo4j"
        os.environ["PDBpassword"] = "neo4j"

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

    def test_template_2_dataset_crawling(self):
        solr_docs = Template2DatasetsQueryIndexer().crawl_vfb_json_data(["VFB_00017894", "VFB_00200000", "VFB_none"])

        self.assertTrue(solr_docs)
        self.assertEqual(2, len(list(solr_docs.keys())))
        self.assertTrue("VFB_00017894" in solr_docs)
        self.assertEqual("VFB_00017894", solr_docs["VFB_00017894"]["id"])
        self.assertTrue("template_2_datasets_query" in solr_docs["VFB_00017894"])
        self.assertTrue("VFB_00200000" in solr_docs)
        self.assertEqual("VFB_00200000", solr_docs["VFB_00200000"]["id"])
        self.assertTrue("template_2_datasets_query" in solr_docs["VFB_00200000"])

    def test_all_dataset_crawling(self):
        solr_docs = AllDatasetsQueryIndexer().crawl_vfb_json_data([""])

        self.assertTrue(solr_docs)
        self.assertTrue(len(list(solr_docs.keys())) > 100)
        self.assertTrue("Otto2020" in solr_docs)
        self.assertEqual("Otto2020", solr_docs["Otto2020"]["id"])
        self.assertTrue("all_datasets_query" in solr_docs["Otto2020"])
        self.assertTrue("Hampel2015" in solr_docs)
        self.assertEqual("Hampel2015", solr_docs["Hampel2015"]["id"])
        self.assertTrue("all_datasets_query" in solr_docs["Hampel2015"])

    def test_anat_2_ep_dataset_crawling(self):
        solr_docs = Anat2EpQueryIndexer().crawl_vfb_json_data(["FBbt_00048248", "FBbt_00047740", "FBbt_none"])

        self.assertTrue(solr_docs)
        self.assertEqual(2, len(list(solr_docs.keys())))
        self.assertTrue("FBbt_00048248" in solr_docs)
        self.assertEqual("FBbt_00048248", solr_docs["FBbt_00048248"]["id"])
        self.assertTrue("anat_2_ep_query" in solr_docs["FBbt_00048248"])
        self.assertTrue("FBbt_00047740" in solr_docs)
        self.assertEqual("FBbt_00047740", solr_docs["FBbt_00047740"]["id"])
        self.assertTrue("anat_2_ep_query" in solr_docs["FBbt_00047740"])

    def test_anat_image_crawling(self):
        solr_docs = AnatImageQueryIndexer().crawl_vfb_json_data(["VFB_00002007", "VFB_00002009", "VFB_00002016"])

        self.assertTrue(solr_docs)
        self.assertEqual(3, len(list(solr_docs.keys())))
        self.assertTrue("VFB_00002007" in solr_docs)
        self.assertEqual("VFB_00002007", solr_docs["VFB_00002007"]["id"])
        self.assertTrue("anat_image_query" in solr_docs["VFB_00002007"])
        self.assertTrue("VFB_00002009" in solr_docs)
        self.assertEqual("VFB_00002009", solr_docs["VFB_00002009"]["id"])
        self.assertTrue("anat_image_query" in solr_docs["VFB_00002009"])
        self.assertTrue("VFB_00002016" in solr_docs)
        self.assertEqual("VFB_00002016", solr_docs["VFB_00002016"]["id"])
        self.assertTrue("anat_image_query" in solr_docs["VFB_00002016"])

    def test_anat_crawling(self):
        solr_docs = Ep2AnatQueryIndexer().crawl_vfb_json_data(["VFBexp_FBtp0129932FBtp0129970", "VFBexp_FBtp0129932FBtp0129962", "VFBexp_FBtpnone"])

        self.assertTrue(solr_docs)
        self.assertEqual(2, len(list(solr_docs.keys())))
        self.assertTrue("VFBexp_FBtp0129932FBtp0129970" in solr_docs)
        self.assertEqual("VFBexp_FBtp0129932FBtp0129970", solr_docs["VFBexp_FBtp0129932FBtp0129970"]["id"])
        self.assertTrue("ep_2_anat_query" in solr_docs["VFBexp_FBtp0129932FBtp0129970"])
        self.assertTrue("VFBexp_FBtp0129932FBtp0129962" in solr_docs)
        self.assertEqual("VFBexp_FBtp0129932FBtp0129962", solr_docs["VFBexp_FBtp0129932FBtp0129962"]["id"])
        self.assertTrue("ep_2_anat_query" in solr_docs["VFBexp_FBtp0129932FBtp0129962"])

    def test_ep_2_anat_crawling(self):
        solr_docs = AnatQueryIndexer().crawl_vfb_json_data(["FBbt_00048999", "FBbt_00048470", "FBbt_none"])

        self.assertTrue(solr_docs)
        self.assertEqual(2, len(list(solr_docs.keys())))
        self.assertTrue("FBbt_00048999" in solr_docs)
        self.assertEqual("FBbt_00048999", solr_docs["FBbt_00048999"]["id"])
        self.assertTrue("anat_query" in solr_docs["FBbt_00048999"])
        self.assertTrue("FBbt_00048470" in solr_docs)
        self.assertEqual("FBbt_00048470", solr_docs["FBbt_00048470"]["id"])
        self.assertTrue("anat_query" in solr_docs["FBbt_00048470"])


class TestQueryIndexer(BaseQueryIndexer):

    def get_parameters_query(self):
        return "MATCH (n:has_image:Individual) WITH distinct n LIMIT 10 RETURN collect(distinct n.short_form) as ids"

    def get_vfb_json_query(self, ids):
        return self.ql.anat_image_query(short_forms=ids)

    def get_service_name(self):
        return TEST_SERVICE_NAME
