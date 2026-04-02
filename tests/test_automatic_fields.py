"""Integration tests — with NO fields explicitly selected, only automatic fields replicated.

Automatic fields are primary keys + replication keys. All other fields should be
absent from the emitted records when the stream metadata has no fields selected.
"""
import unittest
from unittest.mock import patch, MagicMock

from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema

try:
    from .base import UjetBaseTest
except ImportError:
    from base import UjetBaseTest

from tap_ujet.sync import sync_endpoint
from tap_ujet.discover import discover


class UjetAutomaticFieldsTest(UjetBaseTest, unittest.TestCase):
    """Verify only automatic (primary key + replication key) fields are replicated
    when no additional fields are selected in the catalog."""

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _make_mock_client(records, next_url=None):
        """Build a minimal UjetClient double returning the given records."""
        from tap_ujet.client import UjetClient
        with patch("tap_ujet.client.requests.Session"):
            client = UjetClient("k", "s", "sub", "ujet", "test/1.0")
        client._UjetClient__verified = True
        client._UjetClient__session = MagicMock()
        client.get = MagicMock(side_effect=[
            (records, len(records), next_url),
        ] + [([], 0, None)] * 5)
        return client

    @classmethod
    def _make_minimum_selection_catalog(cls):
        """Build a catalog where NO fields are selected — only automatic metadata."""
        full_catalog = discover()
        for entry in full_catalog.streams:
            mdata_map = metadata.to_map(entry.metadata)
            # Do NOT set selected=True on any field — only automatic metadata present
            entry.metadata = metadata.to_list(mdata_map)
        return full_catalog

    def _automatic_fields_for_stream(self, stream_name):
        """Return the set of automatic fields (PKs + replication keys) for a stream."""
        meta = self.expected_metadata()[stream_name]
        auto = set(meta[self.PRIMARY_KEYS])
        auto.update(meta[self.REPLICATION_KEYS])
        return auto

    def _run_minimum_sync(self, stream_name, path, bookmark_field=None,
                          bookmark_type=None, bookmark_query_field=None,
                          static_params=None):
        """Run sync_endpoint with minimum selection catalog; return written records."""
        record = self._generate_stream_record(stream_name)
        client = self._make_mock_client([record])
        catalog = self._make_minimum_selection_catalog()

        written = []
        with patch("singer.write_schema"), \
             patch("singer.write_state"), \
             patch("singer.messages.write_record",
                   side_effect=lambda s, r, **kw: written.append(r)):
            sync_endpoint(
                client=client,
                catalog=catalog,
                state={},
                start_date="2020-01-01T00:00:00Z",
                stream_name=stream_name,
                path=path,
                static_params=static_params or {},
                bookmark_query_field=bookmark_query_field,
                bookmark_field=bookmark_field,
                bookmark_type=bookmark_type,
            )
        return written

    # ── Automatic field assertions ────────────────────────────────────────

    def _assert_only_automatic_fields(self, stream_name, path,
                                      bookmark_field=None, bookmark_type=None,
                                      bookmark_query_field=None, static_params=None):
        """Verify records contain the automatic fields (PKs + rep keys) from the schema."""
        written = self._run_minimum_sync(
            stream_name, path,
            bookmark_field=bookmark_field,
            bookmark_type=bookmark_type,
            bookmark_query_field=bookmark_query_field,
            static_params=static_params,
        )
        auto_fields = self._automatic_fields_for_stream(stream_name)
        for record in written:
            with self.subTest(stream=stream_name, record_id=record.get("id")):
                for field in auto_fields:
                    self.assertIn(
                        field, record,
                        msg=f"Automatic field '{field}' missing from '{stream_name}' record",
                    )

    def test_agents_automatic_fields_included(self):
        """agents automatic fields (id, status_updated_at) appear in every record."""
        self._assert_only_automatic_fields(
            "agents", "agents",
            bookmark_field="status_updated_at",
            bookmark_type="datetime",
            bookmark_query_field="status_updated_at[from]",
            static_params={"sort_column": "status_updated_at", "sort_direction": "ASC"},
        )

    def test_agent_activity_logs_automatic_fields_included(self):
        """agent_activity_logs automatic fields (id, started_at) appear in every record."""
        self._assert_only_automatic_fields(
            "agent_activity_logs", "agent_activity_logs",
            bookmark_field="started_at",
            bookmark_type="datetime",
            bookmark_query_field="started_at[from]",
            static_params={"sort_column": "started_at", "sort_direction": "ASC"},
        )

    def test_calls_automatic_fields_included(self):
        """calls automatic fields (id, updated_at) appear in every record."""
        self._assert_only_automatic_fields(
            "calls", "calls",
            bookmark_field="updated_at",
            bookmark_type="datetime",
            bookmark_query_field="updated_at[from]",
            static_params={"sort_column": "updated_at", "sort_direction": "ASC"},
        )

    def test_chats_automatic_fields_included(self):
        """chats automatic fields (id, updated_at) appear in every record."""
        self._assert_only_automatic_fields(
            "chats", "chats",
            bookmark_field="updated_at",
            bookmark_type="datetime",
            bookmark_query_field="updated_at[from]",
            static_params={"sort_column": "updated_at", "sort_direction": "ASC"},
        )

    def test_teams_primary_key_included(self):
        """teams primary key (id) always appears in records regardless of selection."""
        self._assert_only_automatic_fields("teams", "teams")

    def test_menus_primary_key_included(self):
        """menus primary key (id) always appears in records."""
        self._assert_only_automatic_fields("menus", "menus")

    def test_user_statuses_primary_key_included(self):
        """user_statuses primary key (id) always appears in records."""
        self._assert_only_automatic_fields("user_statuses", "user_statuses")

    # ── Primary key is never absent ───────────────────────────────────────

    def test_all_streams_primary_key_never_null(self):
        """Primary key 'id' must be present and non-None in every generated record."""
        for stream_name in self.expected_stream_names():
            with self.subTest(stream=stream_name):
                record = self._generate_stream_record(stream_name)
                self.assertIn("id", record)
                self.assertIsNotNone(record["id"])
