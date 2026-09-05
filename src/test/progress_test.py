"""The progress file may resume STALE pairs across runs, never MISSING ones.

A missing pair is re-decided against Solr each time it runs, so skipping it
on an earlier build's word hides every record that arrived since. Build #71
(2026-09-04) did exactly that for all nine term_info indexers.
"""
import json
import os
import tempfile
import unittest

from src.progress import (
    ProgressTracker, STATUS_COMPLETED, STATUS_EMPTY, STATUS_IN_PROGRESS,
    STATUS_PENDING, RUN_COMPLETED, RUN_FAILED, SCHEMA_VERSION,
)


def _pair(phase, name, status):
    return {"phase": phase, "indexer": name, "ordinal": 1, "status": status,
            "started_at": None, "finished_at": None, "duration_seconds": None,
            "error": None}


class ProgressResumeTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "indexer_progress.json")

    def tearDown(self):
        self.dir.cleanup()

    def _write(self, pairs, run_status=RUN_FAILED):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump({
                "schema_version": SCHEMA_VERSION,
                "run_started_at": "2026-09-04T06:15:38Z",
                "run_finished_at": None,
                "run_status": run_status,
                "total_pairs": len(pairs),
                "pairs": {f"{p['phase']}::{p['indexer']}": p for p in pairs},
            }, f)

    def _load(self):
        tracker = ProgressTracker(path=self.path)
        tracker.load()
        return tracker

    def test_completed_missing_pair_is_not_skipped_on_the_next_run(self):
        self._write([_pair("missing", "DatasetTermInfoQueryIndexer", STATUS_COMPLETED)])
        tracker = self._load()
        self.assertFalse(tracker.should_skip("missing", "DatasetTermInfoQueryIndexer"))
        pair = tracker.state["pairs"]["missing::DatasetTermInfoQueryIndexer"]
        self.assertEqual(pair["status"], STATUS_PENDING)

    def test_empty_missing_pair_is_not_skipped_either(self):
        # "empty" means nothing was missing last time; something may be now.
        self._write([_pair("missing", "AllDatasetsQueryIndexer", STATUS_EMPTY)])
        tracker = self._load()
        self.assertFalse(tracker.should_skip("missing", "AllDatasetsQueryIndexer"))

    def test_completed_stale_pair_is_still_skipped(self):
        # The stale crawl is the expensive one; resuming it is the whole point.
        self._write([_pair("stale", "AnatomicalIndTermInfoQueryIndexer", STATUS_COMPLETED)])
        tracker = self._load()
        self.assertTrue(tracker.should_skip("stale", "AnatomicalIndTermInfoQueryIndexer"))

    def test_interrupted_pairs_still_reset(self):
        self._write([_pair("stale", "ClassTermInfoQueryIndexer", STATUS_IN_PROGRESS)])
        tracker = self._load()
        self.assertFalse(tracker.should_skip("stale", "ClassTermInfoQueryIndexer"))

    def test_a_clean_previous_run_still_reruns_missing(self):
        self._write([_pair("missing", "DatasetTermInfoQueryIndexer", STATUS_COMPLETED),
                     _pair("stale", "DatasetTermInfoQueryIndexer", STATUS_COMPLETED)],
                    run_status=RUN_COMPLETED)
        tracker = self._load()
        self.assertFalse(tracker.should_skip("missing", "DatasetTermInfoQueryIndexer"))
        self.assertTrue(tracker.should_skip("stale", "DatasetTermInfoQueryIndexer"))


if __name__ == "__main__":
    unittest.main()
