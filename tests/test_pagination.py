"""Integration tests for tap-ujet pagination with mocked data.

Supplies multi-page side_effect lists to UjetClient.get() and verifies that
sync_endpoint continues fetching until next_url is None and that all records
across pages are written.
"""
import unittest
from unittest.mock import patch, MagicMock

try:
    from .base import UjetBaseTest
except ImportError:
    from base import UjetBaseTest

from tap_ujet.sync import sync_endpoint


class UjetPaginationTest(UjetBaseTest, unittest.TestCase):
    """Verify page-based pagination for streams that paginate."""

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _make_mock_client(get_side_effect):
        """Build a minimal UjetClient double with a controlled get() side_effect."""
        from tap_ujet.client import UjetClient
        with patch("tap_ujet.client.requests.Session"):
            client = UjetClient("k", "s", "sub", "ujet", "test/1.0")
        client._UjetClient__verified = True
        client._UjetClient__session = MagicMock()
        mock_get = MagicMock(side_effect=get_side_effect)
        client.get = mock_get
        return client, mock_get

    # ── agents: multi-page ───────────────────────────────────────────────

    @patch("singer.messages.write_record")
    @patch("singer.write_state")
    def test_agents_fetches_multiple_pages(self, _mock_state, mock_write_record):
        """sync_endpoint follows next_url and fetches all pages for 'agents'."""
        page1 = [
            {"id": i, "name": f"Agent {i}", "status_updated_at": "2024-06-01T00:00:00Z"}
            for i in range(1, 101)
        ]
        page2 = [
            {"id": 200, "name": "Agent 200", "status_updated_at": "2024-06-02T00:00:00Z"}
        ]
        next_url = "https://mock.ujet.co/manager/api/v1/agents?page=2"

        client, mock_get = self._make_mock_client([
            (page1, 101, next_url),
            (page2, 101, None),
        ])

        catalog = self._make_catalog()

        with patch("singer.write_schema"):
            sync_endpoint(
                client=client,
                catalog=catalog,
                state={},
                start_date="2024-01-01T00:00:00Z",
                stream_name="agents",
                path="agents",
                static_params={"sort_column": "status_updated_at", "sort_direction": "ASC"},
                bookmark_query_field="status_updated_at[from]",
                bookmark_field="status_updated_at",
                bookmark_type="datetime",
            )

        self.assertEqual(mock_get.call_count, 2,
                         "Should make exactly 2 GET calls (one per page)")
        self.assertEqual(mock_write_record.call_count, 101,
                         "Should write all 101 records across both pages")

    @patch("singer.messages.write_record")
    @patch("singer.write_state")
    def test_agents_single_page_stops_after_one_call(self, _mock_state, mock_write_record):
        """Single-page response: sync_endpoint stops after one request."""
        records = [
            {"id": i, "name": f"A{i}", "status_updated_at": "2024-06-01T00:00:00Z"}
            for i in range(1, 5)
        ]
        client, mock_get = self._make_mock_client([(records, 4, None)])
        catalog = self._make_catalog()

        with patch("singer.write_schema"):
            sync_endpoint(
                client=client,
                catalog=catalog,
                state={},
                start_date="2024-01-01T00:00:00Z",
                stream_name="agents",
                path="agents",
                static_params={},
                bookmark_query_field="status_updated_at[from]",
                bookmark_field="status_updated_at",
                bookmark_type="datetime",
            )

        mock_get.assert_called_once()
        self.assertEqual(mock_write_record.call_count, 4)

    # ── calls: multi-page ────────────────────────────────────────────────

    @patch("singer.messages.write_record")
    @patch("singer.write_state")
    def test_calls_fetches_multiple_pages(self, _mock_state, mock_write_record):
        """sync_endpoint handles multi-page responses for the 'calls' stream."""
        page1 = [
            {"id": i, "updated_at": "2024-05-01T00:00:00Z"}
            for i in range(1, 101)
        ]
        page2 = [{"id": 200, "updated_at": "2024-05-02T00:00:00Z"}]
        next_url = "https://mock.ujet.co/manager/api/v1/calls?page=2"

        client, mock_get = self._make_mock_client([
            (page1, 101, next_url),
            (page2, 101, None),
        ])
        catalog = self._make_catalog()

        with patch("singer.write_schema"):
            sync_endpoint(
                client=client,
                catalog=catalog,
                state={},
                start_date="2024-01-01T00:00:00Z",
                stream_name="calls",
                path="calls",
                static_params={"sort_column": "updated_at", "sort_direction": "ASC"},
                bookmark_query_field="updated_at[from]",
                bookmark_field="updated_at",
                bookmark_type="datetime",
            )

        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(mock_write_record.call_count, 101)

    # ── full-table: teams ─────────────────────────────────────────────────

    @patch("singer.messages.write_record")
    @patch("singer.write_state")
    def test_teams_full_table_multi_page(self, _mock_state, mock_write_record):
        """FULL_TABLE stream 'teams' also handles pagination correctly."""
        page1 = [{"id": i, "name": f"Team {i}"} for i in range(1, 101)]
        page2 = [{"id": 200, "name": "Team 200"}]
        next_url = "https://mock.ujet.co/manager/api/v1/teams?page=2"

        client, mock_get = self._make_mock_client([
            (page1, 101, next_url),
            (page2, 101, None),
        ])
        catalog = self._make_catalog()

        with patch("singer.write_schema"):
            sync_endpoint(
                client=client,
                catalog=catalog,
                state={},
                start_date="2024-01-01T00:00:00Z",
                stream_name="teams",
                path="teams",
                static_params={},
                bookmark_query_field=None,
                bookmark_field=None,
                bookmark_type=None,
            )

        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(mock_write_record.call_count, 101)

    # ── tree streams: no pagination expected ─────────────────────────────

    @patch("singer.messages.write_record")
    @patch("singer.write_state")
    def test_menu_tree_records_written(self, _mock_state, mock_write_record):
        """menu_tree returns flattened records across a single page."""
        # menu_tree records use transform_recursive_tree — provide flat records
        records = [{"id": 1, "name": "Menu A"}, {"id": 2, "name": "Menu B"}]
        client, mock_get = self._make_mock_client([(records, 2, None)])
        catalog = self._make_catalog()

        with patch("singer.write_schema"):
            sync_endpoint(
                client=client,
                catalog=catalog,
                state={},
                start_date="2024-01-01T00:00:00Z",
                stream_name="menu_tree",
                path="menus/tree",
                static_params={},
                bookmark_query_field=None,
                bookmark_field=None,
                bookmark_type=None,
            )

        self.assertEqual(mock_write_record.call_count, 2)

    # ── API_LIMIT respected ───────────────────────────────────────────────

    def test_api_limit_is_set_to_100_for_all_streams(self):
        """All streams must declare API_LIMIT of 100 in expected_metadata."""
        for stream_name, meta in self.expected_metadata().items():
            with self.subTest(stream=stream_name):
                self.assertEqual(meta[self.API_LIMIT], 100)


if __name__ == "__main__":
    unittest.main()
