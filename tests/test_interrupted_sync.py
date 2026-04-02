"""Integration tests for tap-ujet interrupted sync resumption with mocked data.

Verifies that when a sync is interrupted mid-stream and restarted with the
saved state, it resumes from the correct bookmark without duplicating records.
"""
import unittest
from unittest.mock import patch, MagicMock

try:
    from .base import UjetBaseTest
except ImportError:
    from base import UjetBaseTest

from tap_ujet.sync import sync, sync_endpoint


class UjetInterruptedSyncTest(UjetBaseTest, unittest.TestCase):
    """Verify sync resumes correctly after an interruption."""

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _make_mock_client(records, next_url=None):
        """Build a minimal UjetClient double."""
        from tap_ujet.client import UjetClient
        with patch("tap_ujet.client.requests.Session"):
            client = UjetClient("k", "s", "sub", "ujet", "test/1.0")
        client._UjetClient__verified = True
        client._UjetClient__session = MagicMock()
        client.get = MagicMock(side_effect=[
            (records, len(records), next_url),
        ] + [([], 0, None)] * 10)
        return client

    # ── INCREMENTAL: resume from bookmark ────────────────────────────────

    @patch("singer.write_state")
    @patch("singer.write_schema")
    @patch("singer.messages.write_record")
    def test_interrupted_sync_skips_records_before_bookmark(
            self, mock_write_record, mock_write_schema, mock_write_state):
        """Resumed sync with bookmark must NOT replay records before the saved bookmark."""
        # State left by interrupted first sync — bookmark partway through
        interrupted_state = {
            "bookmarks": {
                "agents": "2022-06-15T00:00:00Z"
            }
        }

        # Mix of records — some before and some after the interrupted bookmark
        records = [
            {"id": 1, "name": "Old",    "status_updated_at": "2021-01-01T00:00:00Z"},
            {"id": 2, "name": "Middle", "status_updated_at": "2022-01-01T00:00:00Z"},
            {"id": 3, "name": "New",    "status_updated_at": "2023-01-01T00:00:00Z"},
            {"id": 4, "name": "Newer",  "status_updated_at": "2024-01-01T00:00:00Z"},
        ]
        client = self._make_mock_client(records)
        catalog = self._make_catalog()
        catalog.streams = [e for e in catalog.streams if e.stream == "agents"]

        sync(
            client=client,
            config=self.config,
            catalog=catalog,
            state=interrupted_state,
        )

        # Only records with status_updated_at >= bookmark should be written
        for call_args in mock_write_record.call_args_list:
            record = call_args[0][1]
            self.assertGreaterEqual(
                record["status_updated_at"],
                "2022-06-15T00:00:00Z",
                msg="Resumed sync must not replay records before the bookmark",
            )

    @patch("singer.write_state")
    @patch("singer.write_schema")
    @patch("singer.messages.write_record")
    def test_interrupted_sync_writes_records_after_bookmark(
            self, mock_write_record, mock_write_schema, mock_write_state):
        """Resumed sync must write records whose replication key is after the bookmark."""
        interrupted_state = {
            "bookmarks": {
                "agents": "2022-01-01T00:00:00Z"
            }
        }
        records = [
            {"id": 1, "name": "Pre-interrupt",  "status_updated_at": "2021-06-01T00:00:00Z"},
            {"id": 2, "name": "Post-interrupt1", "status_updated_at": "2022-06-01T00:00:00Z"},
            {"id": 3, "name": "Post-interrupt2", "status_updated_at": "2023-01-01T00:00:00Z"},
        ]
        client = self._make_mock_client(records)
        catalog = self._make_catalog()
        catalog.streams = [e for e in catalog.streams if e.stream == "agents"]

        sync(
            client=client,
            config=self.config,
            catalog=catalog,
            state=interrupted_state,
        )

        self.assertEqual(
            mock_write_record.call_count, 2,
            msg="Only records after the bookmark should be written",
        )

    @patch("singer.write_state")
    @patch("singer.write_schema")
    @patch("singer.messages.write_record")
    def test_bookmark_advanced_after_resumed_sync(
            self, mock_write_record, mock_write_schema, mock_write_state):
        """After a resumed sync the bookmark must be advanced to the newest record."""
        interrupted_state = {"bookmarks": {"agents": "2022-01-01T00:00:00Z"}}
        records = [
            {"id": 1, "name": "A", "status_updated_at": "2023-06-01T00:00:00Z"},
            {"id": 2, "name": "B", "status_updated_at": "2024-03-01T00:00:00Z"},
        ]
        client = self._make_mock_client(records)
        catalog = self._make_catalog()
        catalog.streams = [e for e in catalog.streams if e.stream == "agents"]

        sync(
            client=client,
            config=self.config,
            catalog=catalog,
            state=interrupted_state,
        )

        new_bookmark = interrupted_state.get("bookmarks", {}).get("agents")
        self.assertIsNotNone(new_bookmark)
        self.assertIn("2024-03-01", new_bookmark,
                      msg="Bookmark must advance to the most recent record after resume")

    # ── FULL_TABLE: always fully replicated ──────────────────────────────

    @patch("singer.write_state")
    @patch("singer.write_schema")
    @patch("singer.messages.write_record")
    def test_full_table_stream_always_fully_replicated(
            self, mock_write_record, mock_write_schema, mock_write_state):
        """FULL_TABLE streams must fully replicate even when state is non-empty."""
        # Even with a stale state (e.g. from a previous interrupted sync),
        # FULL_TABLE streams have no bookmark and must replicate all records.
        stale_state = {"bookmarks": {}}

        records = [{"id": 1, "name": "Team A"}, {"id": 2, "name": "Team B"}]
        client = self._make_mock_client(records)
        catalog = self._make_catalog()
        catalog.streams = [e for e in catalog.streams if e.stream == "teams"]

        sync(
            client=client,
            config=self.config,
            catalog=catalog,
            state=stale_state,
        )

        self.assertEqual(
            mock_write_record.call_count, 2,
            msg="FULL_TABLE stream must replicate all records regardless of state",
        )

    @patch("singer.write_state")
    @patch("singer.write_schema")
    @patch("singer.messages.write_record")
    def test_full_table_stream_not_bookmarked_after_sync(
            self, mock_write_record, mock_write_schema, mock_write_state):
        """FULL_TABLE stream must not leave a bookmark in state after sync."""
        state = {}
        records = [{"id": 1, "name": "Team A"}]
        client = self._make_mock_client(records)
        catalog = self._make_catalog()
        catalog.streams = [e for e in catalog.streams if e.stream == "teams"]

        sync(
            client=client,
            config=self.config,
            catalog=catalog,
            state=state,
        )

        team_bookmark = state.get("bookmarks", {}).get("teams")
        self.assertIsNone(team_bookmark,
                          msg="FULL_TABLE stream must not create a bookmark")

    # ── currently_syncing cleared after interruption recovery ────────────

    @patch("singer.write_state")
    @patch("singer.write_schema")
    @patch("singer.messages.write_record")
    def test_currently_syncing_cleared_after_resumed_sync(
            self, mock_write_record, mock_write_schema, mock_write_state):
        """currently_syncing must be absent from state after a resumed sync completes."""
        # Simulate a state where a previous sync was interrupted mid-stream
        interrupted_state = {
            "currently_syncing": "agents",
            "bookmarks": {"agents": "2022-01-01T00:00:00Z"},
        }
        records = [{"id": 1, "name": "X", "status_updated_at": "2024-01-01T00:00:00Z"}]
        client = self._make_mock_client(records)
        catalog = self._make_catalog()
        catalog.streams = [e for e in catalog.streams if e.stream == "agents"]

        sync(
            client=client,
            config=self.config,
            catalog=catalog,
            state=interrupted_state,
        )

        self.assertNotIn(
            "currently_syncing",
            interrupted_state,
            msg="currently_syncing must be cleared when sync completes successfully",
        )

    # ── chats: same bookmark logic ────────────────────────────────────────

    @patch("singer.write_state")
    @patch("singer.write_schema")
    @patch("singer.messages.write_record")
    def test_chats_interrupted_sync_resumes_correctly(
            self, mock_write_record, mock_write_schema, mock_write_state):
        """chats: resumed sync from a mid-point bookmark skips pre-bookmark records."""
        interrupted_state = {"bookmarks": {"chats": "2023-01-01T00:00:00Z"}}
        records = [
            {"id": 1, "updated_at": "2022-01-01T00:00:00Z"},   # before — skip
            {"id": 2, "updated_at": "2023-06-01T00:00:00Z"},   # after  — write
        ]
        client = self._make_mock_client(records)
        catalog = self._make_catalog()
        catalog.streams = [e for e in catalog.streams if e.stream == "chats"]

        sync(
            client=client,
            config=self.config,
            catalog=catalog,
            state=interrupted_state,
        )

        self.assertEqual(mock_write_record.call_count, 1)
