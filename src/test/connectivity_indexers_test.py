import json
import os
import unittest

from src.indexers.connectivity.neuron_downstream_connectivity_indexer import NeuronDownstreamConnectivityIndexer
from src.indexers.connectivity.neuron_upstream_connectivity_indexer import NeuronUpstreamConnectivityIndexer


class NeuronConnectivityIndexerUnitTest(unittest.TestCase):
    """Unit tests that do not require a live database connection."""

    def setUp(self):
        os.environ["PDBserver"] = "http://pdb-test.virtualflybrain.org"
        os.environ["PDBuser"] = "neo4j"
        os.environ["PDBpassword"] = "neo4j"

    # --- get_parameters_query ---

    def test_downstream_parameters_query(self):
        query = NeuronDownstreamConnectivityIndexer().get_parameters_query()
        self.assertIn("Class:Neuron", query)
        self.assertIn("collect(distinct n.short_form) as ids", query)

    def test_upstream_parameters_query(self):
        query = NeuronUpstreamConnectivityIndexer().get_parameters_query()
        self.assertIn("Class:Neuron", query)
        self.assertIn("collect(distinct n.short_form) as ids", query)

    # --- get_vfb_json_query — template form (called with ['$ID']) ---

    def test_downstream_query_template_contains_placeholder(self):
        query = NeuronDownstreamConnectivityIndexer().get_vfb_json_query(["$ID"])
        self.assertIn("['$ID']", query)

    def test_upstream_query_template_contains_placeholder(self):
        query = NeuronUpstreamConnectivityIndexer().get_vfb_json_query(["$ID"])
        self.assertIn("['$ID']", query)

    def test_downstream_query_key_fragments(self):
        query = NeuronDownstreamConnectivityIndexer().get_vfb_json_query(["$ID"])
        self.assertIn("synapsed_to", query)
        self.assertIn("SUBCLASSOF", query)
        self.assertIn("hb", query)
        self.assertIn("fafb", query)
        self.assertIn("downstream_connections", query)
        self.assertIn("AS downstream", query)

    def test_upstream_query_key_fragments(self):
        query = NeuronUpstreamConnectivityIndexer().get_vfb_json_query(["$ID"])
        self.assertIn("synapsed_to", query)
        self.assertIn("SUBCLASSOF", query)
        self.assertIn("hb", query)
        self.assertIn("fafb", query)
        self.assertIn("upstream_connections", query)
        self.assertIn("AS upstream", query)

    # --- generate_solr_doc ---

    def test_downstream_generate_solr_doc(self):
        result = {
            "downstream": {
                "class_id": "FBbt_00048514",
                "class_label": "kenyon cell",
                "downstream_connections": [
                    {
                        "upstream_class": "kenyon cell",
                        "upstream_class_id": "FBbt_00048514",
                        "downstream_class": "alpha-beta kenyon cell",
                        "downstream_class_id": "FBbt_00047988",
                        "total_upstream_count": 100,
                        "connected_upstream_count": 80,
                        "percent_connected": 80.0,
                        "pairwise_connections": 500,
                        "total_weight": 2000,
                        "average_weight": 4.0
                    }
                ]
            }
        }
        indexer = NeuronDownstreamConnectivityIndexer()
        doc = indexer.generate_solr_doc(result, request=None)

        self.assertEqual("FBbt_00048514", doc["id"])
        self.assertIn("downstream_connectivity", doc)
        payload = json.loads(doc["downstream_connectivity"]["set"])
        self.assertEqual("FBbt_00048514", payload["downstream"]["class_id"])
        connections = payload["downstream"]["downstream_connections"]
        self.assertEqual(1, len(connections))
        conn = connections[0]
        self.assertIn("upstream_class", conn)
        self.assertIn("downstream_class", conn)
        self.assertIn("pairwise_connections", conn)
        self.assertIn("percent_connected", conn)
        self.assertIn("total_weight", conn)
        self.assertIn("average_weight", conn)

    def test_upstream_generate_solr_doc(self):
        result = {
            "upstream": {
                "class_id": "FBbt_00048514",
                "class_label": "kenyon cell",
                "upstream_connections": [
                    {
                        "upstream_class": "projection neuron",
                        "upstream_class_id": "FBbt_00007382",
                        "downstream_class": "kenyon cell",
                        "downstream_class_id": "FBbt_00048514",
                        "total_upstream_count": 200,
                        "connected_upstream_count": 150,
                        "percent_connected": 75.0,
                        "pairwise_connections": 1000,
                        "total_weight": 5000,
                        "average_weight": 5.0
                    }
                ]
            }
        }
        indexer = NeuronUpstreamConnectivityIndexer()
        doc = indexer.generate_solr_doc(result, request=None)

        self.assertEqual("FBbt_00048514", doc["id"])
        self.assertIn("upstream_connectivity", doc)
        payload = json.loads(doc["upstream_connectivity"]["set"])
        self.assertEqual("FBbt_00048514", payload["upstream"]["class_id"])
        connections = payload["upstream"]["upstream_connections"]
        self.assertEqual(1, len(connections))
        conn = connections[0]
        self.assertIn("upstream_class", conn)
        self.assertIn("downstream_class", conn)
        self.assertIn("pairwise_connections", conn)
        self.assertIn("percent_connected", conn)
        self.assertIn("total_weight", conn)
        self.assertIn("average_weight", conn)

    # --- service names and batch size ---

    def test_downstream_service_name(self):
        self.assertEqual("downstream_connectivity", NeuronDownstreamConnectivityIndexer().get_service_name())

    def test_upstream_service_name(self):
        self.assertEqual("upstream_connectivity", NeuronUpstreamConnectivityIndexer().get_service_name())

    def test_downstream_batch_size_is_one(self):
        self.assertEqual(1, NeuronDownstreamConnectivityIndexer.REQUEST_BATCH_SIZE)

    def test_upstream_batch_size_is_one(self):
        self.assertEqual(1, NeuronUpstreamConnectivityIndexer.REQUEST_BATCH_SIZE)


class NeuronConnectivityIndexerIntegrationTest(unittest.TestCase):
    """
    Integration tests that require a live pdb-test connection.
    Excluded from quick test runs via the 'not test_crawling' filter.
    """

    def setUp(self):
        os.environ["PDBserver"] = "http://pdb-test.virtualflybrain.org"
        os.environ["PDBuser"] = "neo4j"
        os.environ["PDBpassword"] = "neo4j"

    def test_downstream_crawling(self):
        solr_docs = NeuronDownstreamConnectivityIndexer().crawl_vfb_json_data(["FBbt_00048514"])
        self.assertTrue(solr_docs)
        self.assertIn("FBbt_00048514", solr_docs)
        doc = solr_docs["FBbt_00048514"]
        self.assertEqual("FBbt_00048514", doc["id"])
        self.assertIn("downstream_connectivity", doc)
        payload = json.loads(doc["downstream_connectivity"]["set"])
        self.assertIn("downstream_connections", payload["downstream"])

    def test_upstream_crawling(self):
        solr_docs = NeuronUpstreamConnectivityIndexer().crawl_vfb_json_data(["FBbt_00048514"])
        self.assertTrue(solr_docs)
        self.assertIn("FBbt_00048514", solr_docs)
        doc = solr_docs["FBbt_00048514"]
        self.assertEqual("FBbt_00048514", doc["id"])
        self.assertIn("upstream_connectivity", doc)
        payload = json.loads(doc["upstream_connectivity"]["set"])
        self.assertIn("upstream_connections", payload["upstream"])


if __name__ == "__main__":
    unittest.main()
