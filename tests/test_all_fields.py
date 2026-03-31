"""Integration tests — all fields are replicated for tap-ujet streams (mocked data).

Uses _generate_stream_record() to produce schema-valid mock records and verifies
that every property in the stream's JSON schema appears in the emitted records.
"""
import unittest
from unittest.mock import patch, MagicMock

try:
    from .base import UjetBaseTest
except ImportError:
    from base import UjetBaseTest

from tap_ujet.sync import sync_endpoint

# ---------------------------------------------------------------------------
# Known missing fields
# Fields that exist in a schema but are not returned by the test environment.
# Add entries here only after a test run reveals them — leave a comment explaining why.
# ---------------------------------------------------------------------------
KNOWN_MISSING_FIELDS = {
    # Example:
    # "agents": {"some_premium_feature_field"},  # only available in Enterprise tier
}


class UjetAllFieldsTest(UjetBaseTest, unittest.TestCase):
    """Ensure syncing with all fields selected replicates every schema field."""

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _make_mock_client(get_side_effect):
        """Build a minimal UjetClient double."""
        from tap_ujet.client import UjetClient
        with patch("tap_ujet.client.requests.Session"):
            client = UjetClient("k", "s", "sub", "ujet", "test/1.0")
        client._UjetClient__verified = True
        client._UjetClient__session = MagicMock()
        client.get = MagicMock(side_effect=get_side_effect)
        return client

    def _run_sync_for_stream(self, stream_name, path, bookmark_field=None,
                              bookmark_type=None, bookmark_query_field=None,
                              static_params=None):
        """Run sync_endpoint for one stream; return the list of written records."""
        record = self._generate_stream_record(stream_name)
        mock_client = self._make_mock_client(
            [([ record ], 1, None)] + [([], 0, None)] * 5
        )
        catalog = self._make_catalog()

        written = []
        with patch("singer.write_schema"), \
             patch("singer.write_state"), \
             patch("singer.messages.write_record",
                   side_effect=lambda s, r, **kw: written.append(r)):
            sync_endpoint(
                client=mock_client,
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

    # ── Schema completeness per stream ────────────────────────────────────

    def _assert_all_fields_present(self, stream_name, path,
                                   bookmark_field=None, bookmark_type=None,
                                   bookmark_query_field=None, static_params=None):
        """Generic assertion: all top-level schema fields appear in the synced record."""
        written = self._run_sync_for_stream(
            stream_name, path,
            bookmark_field=bookmark_field,
            bookmark_type=bookmark_type,
            bookmark_query_field=bookmark_query_field,
            static_params=static_params,
        )
        self.assertGreater(len(written), 0,
                           f"No records written for stream '{stream_name}'")

        schema = self._load_schema(stream_name)
        expected_fields = set(schema.get("properties", {}).keys())
        missing_fields = KNOWN_MISSING_FIELDS.get(stream_name, set())
        required_fields = expected_fields - missing_fields

        actual_fields = set().union(*(set(r.keys()) for r in written))
        missing_in_actual = required_fields - actual_fields
        self.assertEqual(
            missing_in_actual,
            set(),
            msg=f"Stream '{stream_name}': schema fields missing from records: "
                f"{missing_in_actual}",
        )

    def test_agents_all_fields_present(self):
        """All top-level agents schema fields must appear in written records."""
        self._assert_all_fields_present(
            "agents", "agents",
            bookmark_field="status_updated_at",
            bookmark_type="datetime",
            bookmark_query_field="status_updated_at[from]",
            static_params={"sort_column": "status_updated_at", "sort_direction": "ASC"},
        )

    def test_agent_activity_logs_all_fields_present(self):
        """All top-level agent_activity_logs schema fields must appear in written records."""
        self._assert_all_fields_present(
            "agent_activity_logs", "agent_activity_logs",
            bookmark_field="started_at",
            bookmark_type="datetime",
            bookmark_query_field="started_at[from]",
            static_params={"sort_column": "started_at", "sort_direction": "ASC"},
        )

    def test_calls_all_fields_present(self):
        """All top-level calls schema fields must appear in written records."""
        self._assert_all_fields_present(
            "calls", "calls",
            bookmark_field="updated_at",
            bookmark_type="datetime",
            bookmark_query_field="updated_at[from]",
            static_params={"sort_column": "updated_at", "sort_direction": "ASC"},
        )

    def test_chats_all_fields_present(self):
        """All top-level chats schema fields must appear in written records."""
        self._assert_all_fields_present(
            "chats", "chats",
            bookmark_field="updated_at",
            bookmark_type="datetime",
            bookmark_query_field="updated_at[from]",
            static_params={"sort_column": "updated_at", "sort_direction": "ASC"},
        )

    def test_menus_all_fields_present(self):
        """All top-level menus schema fields must appear in written records."""
        self._assert_all_fields_present("menus", "menus")

    def test_teams_all_fields_present(self):
        """All top-level teams schema fields must appear in written records."""
        self._assert_all_fields_present("teams", "teams")

    def test_user_statuses_all_fields_present(self):
        """All top-level user_statuses schema fields must appear in written records."""
        self._assert_all_fields_present("user_statuses", "user_statuses")

    def test_menu_tree_all_fields_present(self):
        """All top-level menu_tree schema fields must appear in written records."""
        self._assert_all_fields_present("menu_tree", "menus/tree")

    def test_team_tree_all_fields_present(self):
        """All top-level team_tree schema fields must appear in written records."""
        self._assert_all_fields_present("team_tree", "teams/tree")

    # ── Cross-stream: known_missing_fields containment ────────────────────

    def test_known_missing_fields_only_reference_real_streams(self):
        """KNOWN_MISSING_FIELDS must only reference streams that exist."""
        expected = self.expected_stream_names()
        for stream_name in KNOWN_MISSING_FIELDS:
            with self.subTest(stream=stream_name):
                self.assertIn(stream_name, expected,
                              msg=f"KNOWN_MISSING_FIELDS references unknown stream '{stream_name}'")
