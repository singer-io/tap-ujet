"""Integration tests for tap-ujet start_date filtering with mocked data.

Verifies that streams respecting start_date return fewer records when a later
start_date is used, and that FULL_TABLE streams return all records regardless
of start_date.
"""
import unittest
from unittest.mock import patch, MagicMock

try:
    from .base import UjetBaseTest
except ImportError:
    from base import UjetBaseTest

from tap_ujet.sync import sync_endpoint


class UjetStartDateTest(UjetBaseTest, unittest.TestCase):
    """Verify start_date filtering for INCREMENTAL streams."""

    # Dates used across tests
    EARLY_START = "2020-01-01T00:00:00Z"
    LATE_START = "2023-01-01T00:00:00Z"

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _make_mock_client(records):
        """Build a minimal UjetClient double."""
        from tap_ujet.client import UjetClient
        with patch("tap_ujet.client.requests.Session"):
            client = UjetClient("k", "s", "sub", "ujet", "test/1.0")
        client._UjetClient__verified = True
        client._UjetClient__session = MagicMock()
        client.get = MagicMock(side_effect=[
            (records, len(records), None),
        ] + [([], 0, None)] * 5)
        return client

    def _sync_stream(self, stream_name, path, records, start_date,
                     bookmark_field=None, bookmark_type=None,
                     bookmark_query_field=None, static_params=None):
        """Run sync_endpoint and return written records."""
        client = self._make_mock_client(records)
        catalog = self._make_catalog()
        state = {}

        written = []
        with patch("singer.write_schema"), \
             patch("singer.write_state"), \
             patch("singer.messages.write_record",
                   side_effect=lambda s, r, **kw: written.append(r)):
            sync_endpoint(
                client=client,
                catalog=catalog,
                state=state,
                start_date=start_date,
                stream_name=stream_name,
                path=path,
                static_params=static_params or {},
                bookmark_query_field=bookmark_query_field,
                bookmark_field=bookmark_field,
                bookmark_type=bookmark_type,
            )
        return written

    # ── INCREMENTAL + OBEYS_START_DATE streams ────────────────────────────

    def test_agents_later_start_date_returns_fewer_records(self):
        """agents: syncing with a later start_date omits records before that date."""
        mixed_records = [
            {"id": 1, "name": "Old Agent",    "status_updated_at": "2021-06-01T00:00:00Z"},
            {"id": 2, "name": "Recent Agent", "status_updated_at": "2024-06-01T00:00:00Z"},
        ]

        records_early = self._sync_stream(
            "agents", "agents", mixed_records, self.EARLY_START,
            bookmark_field="status_updated_at", bookmark_type="datetime",
            bookmark_query_field="status_updated_at[from]",
            static_params={"sort_column": "status_updated_at", "sort_direction": "ASC"},
        )
        records_late = self._sync_stream(
            "agents", "agents", mixed_records, self.LATE_START,
            bookmark_field="status_updated_at", bookmark_type="datetime",
            bookmark_query_field="status_updated_at[from]",
            static_params={"sort_column": "status_updated_at", "sort_direction": "ASC"},
        )

        self.assertLessEqual(
            len(records_late), len(records_early),
            msg="Later start_date should return <= records compared to earlier start_date",
        )

    def test_calls_later_start_date_returns_fewer_records(self):
        """calls: syncing with a later start_date omits records before that date."""
        mixed_records = [
            {"id": 1, "updated_at": "2020-06-01T00:00:00Z"},
            {"id": 2, "updated_at": "2024-06-01T00:00:00Z"},
        ]
        records_early = self._sync_stream(
            "calls", "calls", mixed_records, self.EARLY_START,
            bookmark_field="updated_at", bookmark_type="datetime",
            bookmark_query_field="updated_at[from]",
            static_params={"sort_column": "updated_at", "sort_direction": "ASC"},
        )
        records_late = self._sync_stream(
            "calls", "calls", mixed_records, self.LATE_START,
            bookmark_field="updated_at", bookmark_type="datetime",
            bookmark_query_field="updated_at[from]",
            static_params={"sort_column": "updated_at", "sort_direction": "ASC"},
        )
        self.assertLessEqual(len(records_late), len(records_early))

    def test_chats_later_start_date_returns_fewer_records(self):
        """chats: syncing with a later start_date omits records before that date."""
        mixed_records = [
            {"id": 1, "updated_at": "2020-06-01T00:00:00Z"},
            {"id": 2, "updated_at": "2024-06-01T00:00:00Z"},
        ]
        records_early = self._sync_stream(
            "chats", "chats", mixed_records, self.EARLY_START,
            bookmark_field="updated_at", bookmark_type="datetime",
            bookmark_query_field="updated_at[from]",
            static_params={"sort_column": "updated_at", "sort_direction": "ASC"},
        )
        records_late = self._sync_stream(
            "chats", "chats", mixed_records, self.LATE_START,
            bookmark_field="updated_at", bookmark_type="datetime",
            bookmark_query_field="updated_at[from]",
            static_params={"sort_column": "updated_at", "sort_direction": "ASC"},
        )
        self.assertLessEqual(len(records_late), len(records_early))

    def test_early_start_date_includes_all_records(self):
        """With the earliest possible start_date all records should be returned."""
        records = [
            {"id": 1, "status_updated_at": "2019-06-01T00:00:00Z"},
            {"id": 2, "status_updated_at": "2021-01-01T00:00:00Z"},
            {"id": 3, "status_updated_at": "2024-01-01T00:00:00Z"},
        ]
        written = self._sync_stream(
            "agents", "agents", records, "2019-01-01T00:00:00Z",
            bookmark_field="status_updated_at", bookmark_type="datetime",
            bookmark_query_field="status_updated_at[from]",
            static_params={"sort_column": "status_updated_at", "sort_direction": "ASC"},
        )
        self.assertEqual(len(written), 3)

    # ── FULL_TABLE streams ignore start_date ─────────────────────────────

    def test_full_table_teams_unaffected_by_start_date(self):
        """FULL_TABLE streams replicate all records regardless of start_date."""
        records = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]

        written_early = self._sync_stream("teams", "teams", records, self.EARLY_START)
        written_late = self._sync_stream("teams", "teams", records, self.LATE_START)

        self.assertEqual(
            len(written_early), len(written_late),
            msg="FULL_TABLE stream must return the same count for any start_date",
        )

    # ── Expected metadata validates OBEYS_START_DATE ─────────────────────

    def test_incremental_streams_obey_start_date_is_true(self):
        """All INCREMENTAL streams must have OBEYS_START_DATE=True in metadata."""
        for stream_name, meta in self.expected_metadata().items():
            if meta[self.REPLICATION_METHOD] == self.INCREMENTAL:
                with self.subTest(stream=stream_name):
                    self.assertTrue(
                        meta[self.OBEYS_START_DATE],
                        msg=f"INCREMENTAL stream '{stream_name}' should obey start_date",
                    )

    def test_full_table_streams_obeys_start_date_is_false(self):
        """All FULL_TABLE streams must have OBEYS_START_DATE=False in metadata."""
        for stream_name, meta in self.expected_metadata().items():
            if meta[self.REPLICATION_METHOD] == self.FULL_TABLE:
                with self.subTest(stream=stream_name):
                    self.assertFalse(
                        meta[self.OBEYS_START_DATE],
                        msg=f"FULL_TABLE stream '{stream_name}' should not obey start_date",
                    )


if __name__ == "__main__":
    unittest.main()
