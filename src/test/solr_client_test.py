import os
import unittest
from unittest.mock import Mock, patch

import requests

from src import solr_client


class SolrClientTest(unittest.TestCase):

    def setUp(self):
        os.environ["SOLRserver"] = "http://solr-test.virtualflybrain.org/solr"
        os.environ["SOLRcollection"] = "vfb_json"

    @patch("src.solr_client.time.sleep")
    @patch("src.solr_client.requests.post")
    def test_send_solr_docs_retries_after_connection_error(self, mock_post, mock_sleep):
        response = Mock()
        response.raise_for_status.return_value = None
        response.text = '{"status":"ok"}'

        mock_post.side_effect = [
            requests.exceptions.ConnectionError("connection dropped"),
            response,
        ]

        with patch.object(solr_client, "SOLR_WRITE_MAX_RETRIES", 1), \
                patch.object(solr_client, "SOLR_WRITE_RETRY_INITIAL_DELAY_SECONDS", 0), \
                patch.object(solr_client, "SOLR_WRITE_RETRY_DELAY_INCREMENT_SECONDS", 0):
            result = solr_client.send_solr_docs(
                [{"id": "test_id", "term_info": {"set": '{"value": 1}'}}],
                service_name="term_info",
            )

        self.assertTrue(result)
        self.assertEqual(2, mock_post.call_count)
        mock_sleep.assert_called_once_with(0)

    @patch("src.solr_client.time.sleep")
    @patch("src.solr_client.requests.post")
    def test_send_solr_docs_returns_false_after_exhausting_retries(self, mock_post, mock_sleep):
        mock_post.side_effect = requests.exceptions.ConnectionError("connection dropped")

        with patch.object(solr_client, "SOLR_WRITE_MAX_RETRIES", 1), \
                patch.object(solr_client, "SOLR_WRITE_RETRY_INITIAL_DELAY_SECONDS", 0), \
                patch.object(solr_client, "SOLR_WRITE_RETRY_DELAY_INCREMENT_SECONDS", 0), \
                self.assertLogs("src.solr_client", level="ERROR") as captured_logs:
            result = solr_client.send_solr_docs(
                [{"id": "test_id", "term_info": {"set": '{"value": 1}'}}],
                service_name="term_info",
            )

        self.assertFalse(result)
        self.assertEqual(2, mock_post.call_count)
        self.assertEqual(1, mock_sleep.call_count)
        self.assertTrue(any("Giving up indexing" in message for message in captured_logs.output))

    @patch("src.solr_client.requests.post")
    def test_send_solr_docs_skips_empty_batches(self, mock_post):
        result = solr_client.send_solr_docs([], service_name="term_info")

        self.assertTrue(result)
        mock_post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
