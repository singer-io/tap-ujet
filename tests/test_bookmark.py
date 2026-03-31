"""Integration tests for tap-ujet bookmark (incremental replication) with mocked data.

Patches UjetClient.get() to supply controlled records; verifies that:
  - Bookmarks are written to state after a sync.
  - The second sync (with a bookmark in state) skips records before the bookmark.
"""
import unittest
from unittest.mock import patch, MagicMock, call
import singer

try:
    from .base import UjetBaseTest
except ImportError:
    from base import UjetBaseTest

from tap_ujet.sync import sync, get_bookmark, write_bookmark


class UjetBookmarkTest(UjetBaseTest, unittest.TestCase):
    """Verify bookmark behaviour for INCREMENTAL streams."""

    # ── Helpers ──────────────────────────────────────────────────────────

    def _make_agent_records(self, dates):
        """Build a list of minimal agent records with the given status_updated_at dates."""
        return [
            {"id": i + 1, "status_updated_at": d, "name": f"Agent {i + 1}"}
            for i, d in enumerate(dates)
        ]

    def _build_client_get_side_effect(self, stream_map):
        """
        Return a side_effect callable for UjetClient.get().

        stream_map: {stream_name: records_list}
          The mock routes by inspecting the 'endpoint' kwarg or the URL path.
        """
        call_counts = {}

        def _get(path=None, url=None, **kwargs):
            # Determine which stream we're serving
            endpoint = kwargs.get("endpoint", "")
            for stream_name, records in stream_map.items():
                path_key = stream_name.replace("_", "/") if "/" in stream_name else stream_name
                if stream_name in (endpoint or "") or stream_name in (url or "") \
                        or stream_name in (path or ""):
                    count = call_counts.get(stream_name, 0)
                    call_counts[stream_name] = count + 1
                    if count == 0 and records:
                        return (records, len(records), None)
                    return ([], 0, None)
            # Default: empty response
            return ([], 0, None)

        return _get

    # ── get_bookmark / write_bookmark unit tests ─────────────────────────

    def test_get_bookmark_returns_start_date_when_state_empty(self):
        """get_bookmark falls back to start_date when state has no bookmark."""
        result = get_bookmark({}, "agents", "2020-01-01T00:00:00Z")
        self.assertEqual(result, "2020-01-01T00:00:00Z")

    def test_get_bookmark_returns_stored_value(self):
        """get_bookmark returns the bookmark stored in state for a stream."""
        state = {"bookmarks": {"agents": "2024-03-01T00:00:00Z"}}
        result = get_bookmark(state, "agents", "2020-01-01T00:00:00Z")
        self.assertEqual(result, "2024-03-01T00:00:00Z")

    @patch("singer.write_state")
    def test_write_bookmark_persists_value_in_state(self, _mock_write):
        """write_bookmark updates state and emits a Singer STATE message."""
        state = {}
        write_bookmark(state, "agents", "2024-06-01T00:00:00Z")
        self.assertEqual(state["bookmarks"]["agents"], "2024-06-01T00:00:00Z")

    @patch("singer.write_state")
    def test_write_bookmark_calls_singer_write_state(self, mock_write):
        """write_bookmark always calls singer.write_state."""
        write_bookmark({}, "agents", "2024-06-01T00:00:00Z")
        mock_write.assert_called_once()

    # ── Bookmark set after sync ───────────────────────────────────────────

    @patch("tap_ujet.client.UjetClient.get")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.messages.write_record")
    def test_bookmark_is_set_after_sync_of_incremental_stream(
            self, mock_write_record, mock_write_state, mock_write_schema, mock_get):
        """After syncing an INCREMENTAL stream, state must contain a bookmark."""
        records = self._make_agent_records(["2024-03-15T00:00:00Z", "2024-06-01T00:00:00Z"])
        mock_get.side_effect = [
            (records, len(records), None),  # agents page 1
            ([], 0, None),                  # other streams
        ] + [([], 0, None)] * 20            # guard against extra calls

        catalog = self._make_catalog()
        # Keep only the agents stream to isolate the test
        catalog.streams = [e for e in catalog.streams if e.stream == "agents"]
        state = {}

        sync(
            client=self._make_mock_client(mock_get),
            config=self.config,
            catalog=catalog,
            state=state,
        )

        self.assertIn("bookmarks", state)
        self.assertIn("agents", state["bookmarks"])

    @patch("tap_ujet.client.UjetClient.get")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.messages.write_record")
    def test_bookmark_value_equals_max_replication_key(
            self, mock_write_record, mock_write_state, mock_write_schema, mock_get):
        """Bookmark written after sync must equal the highest replication_key in the batch."""
        records = self._make_agent_records([
            "2024-03-01T00:00:00Z",
            "2024-06-01T00:00:00Z",  # ← highest
            "2024-04-15T00:00:00Z",
        ])
        mock_get.side_effect = [(records, len(records), None)] + [([], 0, None)] * 20

        catalog = self._make_catalog()
        catalog.streams = [e for e in catalog.streams if e.stream == "agents"]
        state = {}

        sync(
            client=self._make_mock_client(mock_get),
            config=self.config,
            catalog=catalog,
            state=state,
        )

        bookmark = state.get("bookmarks", {}).get("agents")
        self.assertIsNotNone(bookmark)
        self.assertIn("2024-06-01", bookmark)

    # ── Bookmark filtering on second sync ────────────────────────────────

    @patch("tap_ujet.client.UjetClient.get")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.messages.write_record")
    def test_second_sync_skips_records_before_bookmark(
            self, mock_write_record, mock_write_state, mock_write_schema, mock_get):
        """Records with replication_key < bookmark must not be written on second sync."""
        # All records are older than the bookmark — none should be written
        records = self._make_agent_records([
            "2023-01-01T00:00:00Z",
            "2023-06-01T00:00:00Z",
        ])
        mock_get.side_effect = [(records, len(records), None)] + [([], 0, None)] * 20

        catalog = self._make_catalog()
        catalog.streams = [e for e in catalog.streams if e.stream == "agents"]
        state = {"bookmarks": {"agents": "2024-01-01T00:00:00Z"}}

        sync(
            client=self._make_mock_client(mock_get),
            config=self.config,
            catalog=catalog,
            state=state,
        )

        mock_write_record.assert_not_called()

    @patch("tap_ujet.client.UjetClient.get")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.messages.write_record")
    def test_second_sync_includes_records_after_bookmark(
            self, mock_write_record, mock_write_state, mock_write_schema, mock_get):
        """Records with replication_key >= bookmark must be written on second sync."""
        records = self._make_agent_records([
            "2023-06-01T00:00:00Z",  # before bookmark — filtered
            "2024-02-01T00:00:00Z",  # after bookmark — written
            "2024-05-01T00:00:00Z",  # after bookmark — written
        ])
        mock_get.side_effect = [(records, len(records), None)] + [([], 0, None)] * 20

        catalog = self._make_catalog()
        catalog.streams = [e for e in catalog.streams if e.stream == "agents"]
        state = {"bookmarks": {"agents": "2024-01-01T00:00:00Z"}}

        sync(
            client=self._make_mock_client(mock_get),
            config=self.config,
            catalog=catalog,
            state=state,
        )

        self.assertEqual(mock_write_record.call_count, 2)

    # ── Full-table streams have no bookmark ───────────────────────────────

    @patch("tap_ujet.client.UjetClient.get")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.messages.write_record")
    def test_full_table_stream_does_not_write_bookmark(
            self, mock_write_record, mock_write_state, mock_write_schema, mock_get):
        """FULL_TABLE streams must not write a bookmark to state."""
        records = [{"id": 1, "name": "Team A"}, {"id": 2, "name": "Team B"}]
        mock_get.side_effect = [(records, len(records), None)] + [([], 0, None)] * 20

        catalog = self._make_catalog()
        catalog.streams = [e for e in catalog.streams if e.stream == "teams"]
        state = {}

        sync(
            client=self._make_mock_client(mock_get),
            config=self.config,
            catalog=catalog,
            state=state,
        )

        # singer.write_state is only called by write_bookmark; it must NOT be called
        # for FULL_TABLE streams (sync code calls write_bookmark only when bookmark_field set)
        team_bookmark = state.get("bookmarks", {}).get("teams")
        self.assertIsNone(team_bookmark)

    # ── Private helpers ───────────────────────────────────────────────────

    @staticmethod
    def _make_mock_client(mock_get):
        """Build a minimal UjetClient double with get() already patched."""
        from tap_ujet.client import UjetClient
        with patch("tap_ujet.client.requests.Session"):
            client = UjetClient(
                "mock_k", "mock_s", "mock_sub", "ujet", "test-agent/1.0"
            )
        client._UjetClient__verified = True
        client._UjetClient__session = MagicMock()
        client.get = mock_get
        return client
