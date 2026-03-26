import unittest
from unittest.mock import patch, MagicMock

from singer.catalog import Catalog, CatalogEntry

from tap_ujet.discover import discover
from tap_ujet.streams import STREAMS


class TestDiscoverReturnsCatalog(unittest.TestCase):
    """discover() builds a Singer Catalog from schemas and stream metadata."""

    def test_returns_catalog_instance(self):
        """discover() returns a singer.catalog.Catalog object."""
        result = discover()
        self.assertIsInstance(result, Catalog)

    def test_catalog_contains_all_streams(self):
        """Catalog must contain exactly one entry per stream in STREAMS."""
        catalog = discover()
        stream_names = {entry.stream for entry in catalog.streams}
        self.assertEqual(stream_names, set(STREAMS.keys()))

    def test_catalog_entry_count_matches_streams(self):
        """Number of catalog entries equals number of defined streams."""
        catalog = discover()
        self.assertEqual(len(catalog.streams), len(STREAMS))

    def test_tap_stream_id_equals_stream_name(self):
        """tap_stream_id should equal stream for each entry."""
        catalog = discover()
        for entry in catalog.streams:
            self.assertEqual(entry.tap_stream_id, entry.stream)

    def test_key_properties_match_stream_config(self):
        """key_properties must match each stream's STREAMS configuration."""
        catalog = discover()
        for entry in catalog.streams:
            expected = STREAMS[entry.stream]['key_properties']
            self.assertEqual(entry.key_properties, expected)

    def test_each_entry_has_id_key_property(self):
        """All streams define 'id' as the primary key."""
        catalog = discover()
        for entry in catalog.streams:
            self.assertIn('id', entry.key_properties)

    def test_schema_is_not_none(self):
        """Every catalog entry must have a non-None schema."""
        catalog = discover()
        for entry in catalog.streams:
            self.assertIsNotNone(entry.schema)

    def test_agents_schema_has_id_property(self):
        """agents schema must contain an 'id' property."""
        catalog = discover()
        agents = next(e for e in catalog.streams if e.stream == 'agents')
        props = agents.schema.to_dict().get('properties', {})
        self.assertIn('id', props)

    def test_incremental_stream_has_valid_replication_keys_in_metadata(self):
        """Incremental streams must declare valid-replication-keys in root metadata."""
        catalog = discover()
        agents = next(e for e in catalog.streams if e.stream == 'agents')
        root_mdata = next(
            (m['metadata'] for m in agents.metadata if m.get('breadcrumb') in ([], ())),
            {}
        )
        self.assertIn('valid-replication-keys', root_mdata)
        self.assertEqual(root_mdata['valid-replication-keys'], ['status_updated_at'])

    def test_full_table_stream_has_correct_replication_method(self):
        """FULL_TABLE streams must declare replication-method in root metadata."""
        catalog = discover()
        teams = next(e for e in catalog.streams if e.stream == 'teams')
        root_mdata = next(
            (m['metadata'] for m in teams.metadata if m.get('breadcrumb') in ([], ())),
            {}
        )
        rep_method = root_mdata.get('forced-replication-method') or \
                     root_mdata.get('replication-method')
        self.assertEqual(rep_method, 'FULL_TABLE')

    def test_incremental_stream_replication_method_is_incremental(self):
        """Incremental streams must declare INCREMENTAL replication-method."""
        catalog = discover()
        agents = next(e for e in catalog.streams if e.stream == 'agents')
        root_mdata = next(
            (m['metadata'] for m in agents.metadata if m.get('breadcrumb') in ([], ())),
            {}
        )
        rep_method = root_mdata.get('forced-replication-method') or \
                     root_mdata.get('replication-method')
        self.assertEqual(rep_method, 'INCREMENTAL')

    def test_metadata_list_is_not_empty(self):
        """Every catalog entry must have at least one metadata dict."""
        catalog = discover()
        for entry in catalog.streams:
            self.assertGreater(len(entry.metadata), 0)

    def test_agents_replication_key_in_schema_properties(self):
        """agents' replication key 'status_updated_at' must be in its schema."""
        catalog = discover()
        agents = next(e for e in catalog.streams if e.stream == 'agents')
        props = agents.schema.to_dict().get('properties', {})
        self.assertIn('status_updated_at', props)

    def test_calls_entry_schema_has_id_property(self):
        """calls schema must contain an 'id' property."""
        catalog = discover()
        calls = next(e for e in catalog.streams if e.stream == 'calls')
        props = calls.schema.to_dict().get('properties', {})
        self.assertIn('id', props)


class TestDiscoverGetSchemas(unittest.TestCase):
    """get_schemas() correctly reads all schema JSON files."""

    def test_all_schema_files_are_readable(self):
        """get_schemas() should not raise and should return all streams."""
        from tap_ujet.schema import get_schemas
        schemas, field_metadata = get_schemas()
        self.assertEqual(set(schemas.keys()), set(STREAMS.keys()))

    def test_field_metadata_keys_match_streams(self):
        """field_metadata should contain an entry for every stream."""
        from tap_ujet.schema import get_schemas
        _, field_metadata = get_schemas()
        self.assertEqual(set(field_metadata.keys()), set(STREAMS.keys()))

    def test_schema_missing_file_raises_file_not_found(self):
        """get_schemas() raises FileNotFoundError if a schema file is missing."""
        from tap_ujet.schema import get_schemas
        original_streams = dict(STREAMS)
        try:
            STREAMS['nonexistent_stream'] = {
                'key_properties': ['id'],
                'replication_method': 'FULL_TABLE',
            }
            with self.assertRaises(FileNotFoundError):
                get_schemas()
        finally:
            del STREAMS['nonexistent_stream']


if __name__ == '__main__':
    unittest.main()
