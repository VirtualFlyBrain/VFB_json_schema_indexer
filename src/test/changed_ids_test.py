import os
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

# Same stubs as base_query_indexer_retry_test: no Neo4j client or query builder needed for these tests.
if "src.vfb.vfb_query_builder.query_roller" not in sys.modules:
    neo4j_tools = types.ModuleType("vfb_connect.neo.neo4j_tools")
    neo4j_tools.Neo4jConnect = type("StubNeo4jConnect", (), {"__init__": lambda self, *a, **k: None})
    neo4j_tools.dict_cursor = object
    neo_module = types.ModuleType("vfb_connect.neo")
    neo_module.neo4j_tools = neo4j_tools
    vfb_connect_module = types.ModuleType("vfb_connect")
    vfb_connect_module.neo = neo_module
    sys.modules.setdefault("vfb_connect", vfb_connect_module)
    sys.modules.setdefault("vfb_connect.neo", neo_module)
    sys.modules.setdefault("vfb_connect.neo.neo4j_tools", neo4j_tools)
    query_roller_module = types.ModuleType("src.vfb.vfb_query_builder.query_roller")
    query_roller_module.QueryLibrary = object
    query_builder_module = types.ModuleType("src.vfb.vfb_query_builder")
    query_builder_module.query_roller = query_roller_module
    sys.modules["src.vfb.vfb_query_builder"] = query_builder_module
    sys.modules["src.vfb.vfb_query_builder.query_roller"] = query_roller_module

from src.indexers import base_query_indexer as bqi  # noqa: E402


def _doc(i, value):
    return {"id": i, "term_info": {"set": value}}


class RecordChangesTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.path = os.path.join(self.tmp, "changed.tsv")

    def _read(self):
        with open(self.path) as f:
            return [line.rstrip("\n") for line in f]

    def test_stale_phase_records_only_changed_and_new_docs(self):
        batch = {"a": _doc("a", "same"), "b": _doc("b", "new value"), "c": _doc("c", "brand new")}
        with patch.object(bqi, "CHANGED_IDS_FILE", self.path), patch.object(bqi, "SKIP_UNCHANGED", False), \
                patch.object(bqi, "fetch_field_values", return_value={"a": "same", "b": "old value"}):
            out = bqi._record_changes("term_info", "stale", batch)
        self.assertEqual(batch, out)  # everything still written without SKIP_UNCHANGED
        self.assertEqual(["term_info\tb", "term_info\tc"], self._read())

    def test_skip_unchanged_drops_identical_docs(self):
        batch = {"a": _doc("a", "same"), "b": _doc("b", "new value")}
        with patch.object(bqi, "CHANGED_IDS_FILE", ""), patch.object(bqi, "SKIP_UNCHANGED", True), \
                patch.object(bqi, "fetch_field_values", return_value={"a": "same", "b": "old"}):
            out = bqi._record_changes("term_info", "stale", batch)
        self.assertEqual(["b"], list(out))

    def test_unreadable_solr_counts_every_doc_as_changed(self):
        batch = {"a": _doc("a", "same"), "b": _doc("b", "x")}
        with patch.object(bqi, "CHANGED_IDS_FILE", self.path), patch.object(bqi, "SKIP_UNCHANGED", True), \
                patch.object(bqi, "fetch_field_values", return_value=None):
            out = bqi._record_changes("term_info", "stale", batch)
        self.assertEqual(batch, out)
        self.assertEqual(["term_info\ta", "term_info\tb"], self._read())

    def test_missing_phase_needs_no_solr_read(self):
        batch = {"a": _doc("a", "v")}
        with patch.object(bqi, "CHANGED_IDS_FILE", self.path), \
                patch.object(bqi, "fetch_field_values", side_effect=AssertionError("no read expected")):
            bqi._record_changes("term_info", "missing", batch)
        self.assertEqual(["term_info\ta"], self._read())

    def test_disabled_by_default_is_a_no_op(self):
        batch = {"a": _doc("a", "v")}
        with patch.object(bqi, "CHANGED_IDS_FILE", ""), patch.object(bqi, "SKIP_UNCHANGED", False), \
                patch.object(bqi, "fetch_field_values", side_effect=AssertionError("no read expected")):
            self.assertIs(batch, bqi._record_changes("term_info", "stale", batch))


if __name__ == "__main__":
    unittest.main()
