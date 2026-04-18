import os
import sys
import types
import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace

import requests


class _StubNeo4jConnect:
    def __init__(self, *args, **kwargs):
        self.base_uri = "http://pdb-test.virtualflybrain.org/"
        self.commit = "/db/neo4j/tx/commit"
        self.usr = "neo4j"
        self.pwd = "neo4j"


neo4j_tools = types.ModuleType("vfb_connect.neo.neo4j_tools")
neo4j_tools.Neo4jConnect = _StubNeo4jConnect
neo4j_tools.dict_cursor = object
neo_module = types.ModuleType("vfb_connect.neo")
neo_module.neo4j_tools = neo4j_tools
vfb_connect_module = types.ModuleType("vfb_connect")
vfb_connect_module.neo = neo_module
sys.modules.setdefault("vfb_connect", vfb_connect_module)
sys.modules["vfb_connect.neo"] = neo_module
sys.modules["vfb_connect.neo.neo4j_tools"] = neo4j_tools

query_roller_module = types.ModuleType("src.vfb.vfb_query_builder.query_roller")
query_roller_module.QueryLibrary = object
query_builder_module = types.ModuleType("src.vfb.vfb_query_builder")
query_builder_module.query_roller = query_roller_module
sys.modules["src.vfb.vfb_query_builder"] = query_builder_module
sys.modules["src.vfb.vfb_query_builder.query_roller"] = query_roller_module

from src.indexers.base_query_indexer import BaseQueryIndexer
from src.indexers.connectivity.neuron_downstream_connectivity_indexer import NeuronDownstreamConnectivityIndexer


class DummyQueryIndexer(BaseQueryIndexer):
    def __init__(self):
        self.ql = None
        self.nc = SimpleNamespace(
            base_uri="http://pdb-test.virtualflybrain.org/",
            commit="/db/neo4j/tx/commit",
            usr="neo4j",
            pwd="neo4j",
        )
        self._id_partition = None

    def get_parameters_query(self):
        return None

    def get_vfb_json_query(self, ids):
        return "RETURN 1"

    def get_service_name(self):
        return "dummy_query"


class BaseQueryIndexerRetryTest(unittest.TestCase):
    def test_downstream_query_counts_total_instances_once_before_connection_aggregation(self):
        os.environ["PDBserver"] = "http://pdb-test.virtualflybrain.org"
        os.environ["PDBuser"] = "neo4j"
        os.environ["PDBpassword"] = "neo4j"

        query = NeuronDownstreamConnectivityIndexer().get_vfb_json_query(["$ID"])

        total_count_fragment = "WITH c1, count(DISTINCT all_n1) AS total_c1_instances"
        pairwise_fragment = "count(*) AS pairwise_connections"

        self.assertIn(total_count_fragment, query)
        self.assertIn(pairwise_fragment, query)
        self.assertLess(query.index(total_count_fragment), query.index(pairwise_fragment))
        self.assertNotIn(
            "connected_count, count(DISTINCT all_n1) AS total_c1_instances",
            query,
        )

    @patch("src.indexers.base_query_indexer.time.sleep")
    @patch("src.indexers.base_query_indexer.requests.post")
    def test_execute_query_starts_retry_window_after_long_first_failure(self, mock_post, mock_sleep):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"results": [], "errors": []}

        clock = {"now": 0.0}

        def fake_time():
            return clock["now"]

        def first_then_success(*args, **kwargs):
            if mock_post.call_count == 1:
                clock["now"] = 3600.0
                raise requests.exceptions.Timeout("query timed out")
            return response

        indexer = DummyQueryIndexer()
        mock_post.side_effect = first_then_success

        with patch("src.indexers.base_query_indexer.time.time", side_effect=fake_time), \
                patch.object(DummyQueryIndexer, "_wait_for_neo4j", return_value=True) as mock_wait:
            indexer.execute_query(
                "RETURN 1",
                params={"ids": ["FBbt_00000001"]},
                output_file="file:///import/output.json",
            )

        self.assertEqual(2, mock_post.call_count)
        mock_wait.assert_called_once()
        mock_sleep.assert_called_once_with(5)

    @patch("src.indexers.base_query_indexer.time.sleep")
    @patch("src.indexers.base_query_indexer.requests.post")
    def test_run_query_starts_retry_window_after_long_first_failure(self, mock_post, mock_sleep):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "results": [{"columns": ["ids"], "data": [{"row": [1]}]}],
            "errors": [],
        }

        clock = {"now": 0.0}

        def fake_time():
            return clock["now"]

        def first_then_success(*args, **kwargs):
            if mock_post.call_count == 1:
                clock["now"] = 3600.0
                raise requests.exceptions.Timeout("query timed out")
            return response

        indexer = DummyQueryIndexer()
        mock_post.side_effect = first_then_success

        with patch("src.indexers.base_query_indexer.time.time", side_effect=fake_time), \
                patch.object(DummyQueryIndexer, "_wait_for_neo4j", return_value=True) as mock_wait:
            results = indexer.run_query("RETURN 1 AS ids")

        self.assertEqual([{"ids": 1}], results)
        self.assertEqual(2, mock_post.call_count)
        mock_wait.assert_called_once()
        mock_sleep.assert_called_once_with(5)


if __name__ == "__main__":
    unittest.main()
