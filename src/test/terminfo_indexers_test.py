import os
import unittest
from src.indexers.term_info.license_term_info_indexer import LicenseTermInfoQueryIndexer
from src.indexers.term_info.anatomical_ind_term_info_indexer import AnatomicalIndTermInfoQueryIndexer
from src.indexers.term_info.class_term_info_indexer import ClassTermInfoQueryIndexer
from src.indexers.term_info.neuron_class_term_info_indexer import NeuronClassTermInfoQueryIndexer
from src.indexers.term_info.split_class_term_info_indexer import SplitClassTermInfoQueryIndexer
from src.indexers.term_info.dataset_term_info_indexer import DatasetTermInfoQueryIndexer
from src.indexers.term_info.pub_term_info_indexer import PubTermInfoQueryIndexer
from src.indexers.term_info.template_term_info_indexer import TemplateTermInfoQueryIndexer


TEST_SERVICE_NAME = "test_query"
TEST_OUTPUT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../indexes/{}_index.json")\
    .format(TEST_SERVICE_NAME)


class TemplateGenerationTest(unittest.TestCase):

    def setUp(self):
        # TODO should I hide password?
        os.environ["PDBserver"] = "http://pdb-test.virtualflybrain.org"
        os.environ["PDBuser"] = "neo4j"
        os.environ["PDBpassword"] = "neo4j"

    def test_anatomical_ind_term_info_crawling(self):
        solr_docs = AnatomicalIndTermInfoQueryIndexer().crawl_vfb_json_data(["VFB_00016125", "l1em_catmaid_api", "VFB_00016101", "Not_exists"])

        print(list(solr_docs.keys()))
        self.assertTrue(solr_docs)
        self.assertEqual(3, len(list(solr_docs.keys())))
        self.assertTrue("VFB_00016125" in solr_docs)
        self.assertEqual("VFB_00016125", solr_docs["VFB_00016125"]["id"])
        self.assertTrue("term_info" in solr_docs["VFB_00016125"])

        self.assertTrue("l1em_catmaid_api" in solr_docs)
        self.assertEqual("l1em_catmaid_api", solr_docs["l1em_catmaid_api"]["id"])
        self.assertTrue("term_info" in solr_docs["l1em_catmaid_api"])

        self.assertTrue("VFB_00016101" in solr_docs)
        self.assertEqual("VFB_00016101", solr_docs["VFB_00016101"]["id"])
        self.assertTrue("term_info" in solr_docs["VFB_00016101"])

    def test_license_terminfo_crawling(self):
        solr_docs = LicenseTermInfoQueryIndexer().crawl_vfb_json_data(["VFBlicense_CC_BY_SA_4_0", "VFBlicense_FlyCircuit_License", "Not_exists"])

        self.assertTrue(solr_docs)
        self.assertEqual(2, len(list(solr_docs.keys())))
        self.assertTrue("VFBlicense_CC_BY_SA_4_0" in solr_docs)
        self.assertEqual("VFBlicense_CC_BY_SA_4_0", solr_docs["VFBlicense_CC_BY_SA_4_0"]["id"])
        self.assertTrue("term_info" in solr_docs["VFBlicense_CC_BY_SA_4_0"])

        self.assertTrue("VFBlicense_FlyCircuit_License" in solr_docs)
        self.assertEqual("VFBlicense_FlyCircuit_License", solr_docs["VFBlicense_FlyCircuit_License"]["id"])
        self.assertTrue("term_info" in solr_docs["VFBlicense_FlyCircuit_License"])

    def test_class_terminfo_crawling(self):
        solr_docs = ClassTermInfoQueryIndexer().crawl_vfb_json_data(["FBbt_00047532", "FBbt_00048531", "FBbt_00048108"])

        self.assertTrue(solr_docs)
        self.assertEqual(3, len(list(solr_docs.keys())))
        self.assertTrue("FBbt_00047532" in solr_docs)
        self.assertEqual("FBbt_00047532", solr_docs["FBbt_00047532"]["id"])
        self.assertTrue("term_info" in solr_docs["FBbt_00047532"])

        self.assertTrue("FBbt_00048531" in solr_docs)
        self.assertEqual("FBbt_00048531", solr_docs["FBbt_00048531"]["id"])
        self.assertTrue("term_info" in solr_docs["FBbt_00048531"])

        self.assertTrue("FBbt_00048108" in solr_docs)
        self.assertEqual("FBbt_00048108", solr_docs["FBbt_00048108"]["id"])
        self.assertTrue("term_info" in solr_docs["FBbt_00048108"])

    def test_neuron_class_terminfo_crawling(self):
        solr_docs = NeuronClassTermInfoQueryIndexer().crawl_vfb_json_data(["FBbt_00048514", "FBbt_00048352", "None"])

        self.assertTrue(solr_docs)
        self.assertEqual(2, len(list(solr_docs.keys())))
        self.assertTrue("FBbt_00048514" in solr_docs)
        self.assertEqual("FBbt_00048514", solr_docs["FBbt_00048514"]["id"])
        self.assertTrue("term_info" in solr_docs["FBbt_00048514"])

        self.assertTrue("FBbt_00048352" in solr_docs)
        self.assertEqual("FBbt_00048352", solr_docs["FBbt_00048352"]["id"])
        self.assertTrue("term_info" in solr_docs["FBbt_00048352"])

    def test_split_class_terminfo_crawling(self):
        solr_docs = SplitClassTermInfoQueryIndexer().crawl_vfb_json_data(["VFBexp_FBtp0122940FBtp0118397", "VFBexp_FBtp0122190FBtp0118368", "None"])

        self.assertTrue(solr_docs)
        self.assertEqual(2, len(list(solr_docs.keys())))
        self.assertTrue("VFBexp_FBtp0122940FBtp0118397" in solr_docs)
        self.assertEqual("VFBexp_FBtp0122940FBtp0118397", solr_docs["VFBexp_FBtp0122940FBtp0118397"]["id"])
        self.assertTrue("term_info" in solr_docs["VFBexp_FBtp0122940FBtp0118397"])

        self.assertTrue("VFBexp_FBtp0122190FBtp0118368" in solr_docs)
        self.assertEqual("VFBexp_FBtp0122190FBtp0118368", solr_docs["VFBexp_FBtp0122190FBtp0118368"]["id"])
        self.assertTrue("term_info" in solr_docs["VFBexp_FBtp0122190FBtp0118368"])

    def test_dataset_term_info_crawling(self):
        solr_docs = DatasetTermInfoQueryIndexer().crawl_vfb_json_data(["Turner_Evans2020", "Hampel2015", "None"])

        self.assertTrue(solr_docs)
        self.assertEqual(2, len(list(solr_docs.keys())))
        self.assertTrue("Turner_Evans2020" in solr_docs)
        self.assertEqual("Turner_Evans2020", solr_docs["Turner_Evans2020"]["id"])
        self.assertTrue("term_info" in solr_docs["Turner_Evans2020"])

        self.assertTrue("Hampel2015" in solr_docs)
        self.assertEqual("Hampel2015", solr_docs["Hampel2015"]["id"])
        self.assertTrue("term_info" in solr_docs["Hampel2015"])

    def test_pub_term_info_crawling(self):
        solr_docs = PubTermInfoQueryIndexer().crawl_vfb_json_data(["FBrf0243986", "doi_10_1016_j_cub_2020_06_042", "None"])

        self.assertTrue(solr_docs)
        self.assertEqual(2, len(list(solr_docs.keys())))
        self.assertTrue("FBrf0243986" in solr_docs)
        self.assertEqual("FBrf0243986", solr_docs["FBrf0243986"]["id"])
        self.assertTrue("term_info" in solr_docs["FBrf0243986"])

        self.assertTrue("doi_10_1016_j_cub_2020_06_042" in solr_docs)
        self.assertEqual("doi_10_1016_j_cub_2020_06_042", solr_docs["doi_10_1016_j_cub_2020_06_042"]["id"])
        self.assertTrue("term_info" in solr_docs["doi_10_1016_j_cub_2020_06_042"])

    def test_template_term_info_crawling(self):
        solr_docs = TemplateTermInfoQueryIndexer().crawl_vfb_json_data(["VFB_00200000", "VFB_00110000", "None"])

        self.assertTrue(solr_docs)
        self.assertEqual(2, len(list(solr_docs.keys())))
        self.assertTrue("VFB_00200000" in solr_docs)
        self.assertEqual("VFB_00200000", solr_docs["VFB_00200000"]["id"])
        self.assertTrue("term_info" in solr_docs["VFB_00200000"])

        self.assertTrue("VFB_00110000" in solr_docs)
        self.assertEqual("VFB_00110000", solr_docs["VFB_00110000"]["id"])
        self.assertTrue("term_info" in solr_docs["VFB_00110000"])


