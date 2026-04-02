"""Integration tests for tap-ujet stream discovery with mocked data.

Calls the tap's own discover() function directly — no HTTP requests required.
"""
import unittest
from unittest.mock import patch, MagicMock
from singer import metadata

try:
    from .base import UjetBaseTest
except ImportError:
    from base import UjetBaseTest

from tap_ujet.discover import discover


class UjetDiscoveryTest(UjetBaseTest, unittest.TestCase):
    """Verify discover() returns a correct Singer Catalog for all streams."""

    def _get_catalog(self):
        """Run discover() — no HTTP calls needed (reads local schema JSON files)."""
        return discover()

    # ── Stream presence ──────────────────────────────────────────────────

    def test_discovery_returns_all_expected_streams(self):
        """discover() must return exactly the set of expected streams."""
        catalog = self._get_catalog()
        discovered = {entry.tap_stream_id for entry in catalog.streams}
        self.assertEqual(discovered, self.expected_stream_names())

    def test_discovery_stream_count_matches_expected(self):
        """Number of catalog entries must equal the number of expected streams."""
        catalog = self._get_catalog()
        self.assertEqual(len(catalog.streams), len(self.expected_metadata()))

    def test_discovery_tap_stream_id_equals_stream_name(self):
        """tap_stream_id must equal stream for every entry."""
        catalog = self._get_catalog()
        for entry in catalog.streams:
            with self.subTest(stream=entry.stream):
                self.assertEqual(entry.tap_stream_id, entry.stream)

    # ── Primary keys ─────────────────────────────────────────────────────

    def test_discovery_primary_keys_match_expected(self):
        """Primary keys must match expected_metadata() for every stream."""
        catalog = self._get_catalog()
        expected = self.expected_metadata()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                self.assertEqual(
                    set(entry.key_properties),
                    expected[entry.tap_stream_id][self.PRIMARY_KEYS],
                )

    def test_all_streams_have_id_as_primary_key(self):
        """Every stream must declare 'id' as a primary key."""
        catalog = self._get_catalog()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                self.assertIn("id", entry.key_properties)

    # ── Schema integrity ─────────────────────────────────────────────────

    def test_discovery_schema_has_properties(self):
        """Every discovered stream must have a schema with at least one property."""
        catalog = self._get_catalog()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                schema_dict = entry.schema.to_dict()
                self.assertIn("properties", schema_dict)
                self.assertGreater(len(schema_dict["properties"]), 0)

    def test_discovery_schema_is_not_none(self):
        """Schema must not be None for any stream."""
        catalog = self._get_catalog()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                self.assertIsNotNone(entry.schema)

    def test_discovery_all_schemas_have_id_property(self):
        """All stream schemas must include an 'id' property."""
        catalog = self._get_catalog()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                props = entry.schema.to_dict().get("properties", {})
                self.assertIn("id", props)

    # ── Replication method ───────────────────────────────────────────────

    def test_discovery_replication_method_matches_expected(self):
        """forced-replication-method must match expected_metadata() for every stream."""
        catalog = self._get_catalog()
        expected = self.expected_metadata()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                mdata = metadata.to_map(entry.metadata)
                actual = (
                    metadata.get(mdata, (), "forced-replication-method")
                    or metadata.get(mdata, (), "replication-method")
                )
                self.assertEqual(
                    actual,
                    expected[entry.tap_stream_id][self.REPLICATION_METHOD],
                )

    def test_incremental_streams_have_valid_replication_keys(self):
        """INCREMENTAL streams must declare valid-replication-keys in metadata."""
        catalog = self._get_catalog()
        for entry in catalog.streams:
            if entry.tap_stream_id in self.incremental_streams():
                with self.subTest(stream=entry.tap_stream_id):
                    mdata = metadata.to_map(entry.metadata)
                    keys = metadata.get(mdata, (), "valid-replication-keys")
                    self.assertIsNotNone(keys,
                        msg=f"Stream '{entry.tap_stream_id}' missing valid-replication-keys")
                    self.assertGreater(len(keys), 0)

    def test_full_table_streams_have_no_replication_keys(self):
        """FULL_TABLE streams must report no valid-replication-keys."""
        catalog = self._get_catalog()
        expected = self.expected_metadata()
        for entry in catalog.streams:
            if entry.tap_stream_id in self.full_table_streams():
                with self.subTest(stream=entry.tap_stream_id):
                    self.assertEqual(
                        expected[entry.tap_stream_id][self.REPLICATION_KEYS],
                        set(),
                    )

    # ── Metadata completeness ────────────────────────────────────────────

    def test_metadata_list_is_not_empty(self):
        """Every catalog entry must have at least one metadata dict."""
        catalog = self._get_catalog()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                self.assertGreater(len(entry.metadata), 0)

    def test_replication_key_fields_exist_in_schema(self):
        """Every INCREMENTAL stream's replication key must exist in the schema properties."""
        catalog = self._get_catalog()
        expected = self.expected_metadata()
        for entry in catalog.streams:
            stream_name = entry.tap_stream_id
            if stream_name in self.incremental_streams():
                with self.subTest(stream=stream_name):
                    rep_keys = expected[stream_name][self.REPLICATION_KEYS]
                    props = entry.schema.to_dict().get("properties", {})
                    for key in rep_keys:
                        self.assertIn(key, props,
                            msg=f"Replication key '{key}' not in '{stream_name}' schema")
