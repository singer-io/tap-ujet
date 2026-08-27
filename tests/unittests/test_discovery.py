import unittest
from unittest.mock import MagicMock, patch

from singer.catalog import Catalog

from tap_ujet.discover import discover, _check_stream_access, _apply_access_checks
from tap_ujet.client import UjetForbiddenError
from tap_ujet.streams import STREAMS


def _make_mock_client():
    """Create a mock client that succeeds for all stream access checks."""
    client = MagicMock()
    client.request.return_value = ([], 0, None)
    return client


class TestDiscoverReturnsCatalog(unittest.TestCase):
    """discover() builds a Singer Catalog from schemas and stream metadata."""

    def test_returns_catalog_instance(self):
        """discover() returns a singer.catalog.Catalog object."""
        client = _make_mock_client()
        result = discover(client)
        self.assertIsInstance(result, Catalog)

    def test_catalog_contains_all_streams(self):
        """Catalog must contain exactly one entry per stream in STREAMS."""
        client = _make_mock_client()
        catalog = discover(client)
        stream_names = {entry.stream for entry in catalog.streams}
        self.assertEqual(stream_names, set(STREAMS.keys()))

    def test_catalog_entry_count_matches_streams(self):
        """Number of catalog entries equals number of defined streams."""
        client = _make_mock_client()
        catalog = discover(client)
        self.assertEqual(len(catalog.streams), len(STREAMS))

    def test_tap_stream_id_equals_stream_name(self):
        """tap_stream_id should equal stream for each entry."""
        client = _make_mock_client()
        catalog = discover(client)
        for entry in catalog.streams:
            self.assertEqual(entry.tap_stream_id, entry.stream)

    def test_key_properties_match_stream_config(self):
        """key_properties must match each stream's STREAMS configuration."""
        client = _make_mock_client()
        catalog = discover(client)
        for entry in catalog.streams:
            expected = STREAMS[entry.stream]['key_properties']
            self.assertEqual(entry.key_properties, expected)

    def test_each_entry_has_id_key_property(self):
        """All streams define 'id' as the primary key."""
        client = _make_mock_client()
        catalog = discover(client)
        for entry in catalog.streams:
            self.assertIn('id', entry.key_properties)

    def test_schema_is_not_none(self):
        """Every catalog entry must have a non-None schema."""
        client = _make_mock_client()
        catalog = discover(client)
        for entry in catalog.streams:
            self.assertIsNotNone(entry.schema)

    def test_agents_schema_has_id_property(self):
        """agents schema must contain an 'id' property."""
        client = _make_mock_client()
        catalog = discover(client)
        agents = next(e for e in catalog.streams if e.stream == 'agents')
        props = agents.schema.to_dict().get('properties', {})
        self.assertIn('id', props)

    def test_incremental_stream_has_valid_replication_keys_in_metadata(self):
        """Incremental streams must declare valid-replication-keys in root metadata."""
        client = _make_mock_client()
        catalog = discover(client)
        agents = next(e for e in catalog.streams if e.stream == 'agents')
        root_mdata = next(
            (m['metadata'] for m in agents.metadata if m.get('breadcrumb') in ([], ())),
            {}
        )
        self.assertIn('valid-replication-keys', root_mdata)
        self.assertEqual(root_mdata['valid-replication-keys'], ['status_updated_at'])

    def test_full_table_stream_has_correct_replication_method(self):
        """FULL_TABLE streams must declare replication-method in root metadata."""
        client = _make_mock_client()
        catalog = discover(client)
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
        client = _make_mock_client()
        catalog = discover(client)
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
        client = _make_mock_client()
        catalog = discover(client)
        for entry in catalog.streams:
            self.assertGreater(len(entry.metadata), 0)

    def test_agents_replication_key_in_schema_properties(self):
        """agents' replication key 'status_updated_at' must be in its schema."""
        client = _make_mock_client()
        catalog = discover(client)
        agents = next(e for e in catalog.streams if e.stream == 'agents')
        props = agents.schema.to_dict().get('properties', {})
        self.assertIn('status_updated_at', props)

    def test_calls_entry_schema_has_id_property(self):
        """calls schema must contain an 'id' property."""
        client = _make_mock_client()
        catalog = discover(client)
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


class TestCheckStreamAccess(unittest.TestCase):
    """_check_stream_access() probes a stream endpoint for read access."""

    def test_returns_true_when_accessible(self):
        """Should return True when client.request succeeds."""
        client = _make_mock_client()
        result = _check_stream_access(client, 'agents')
        self.assertTrue(result)

    def test_returns_false_on_forbidden(self):
        """Should return False when client.request raises UjetForbiddenError."""
        client = MagicMock()
        client.request.side_effect = UjetForbiddenError("403 Forbidden")
        result = _check_stream_access(client, 'agents')
        self.assertFalse(result)

    def test_uses_stream_path_override(self):
        """Should use the 'path' config when available (e.g., menus/tree)."""
        client = _make_mock_client()
        _check_stream_access(client, 'menu_tree')
        call_args = client.request.call_args
        self.assertEqual(call_args[1]['path'], 'menus/tree')

    def test_uses_stream_name_as_default_path(self):
        """Should use stream_name as path when no 'path' override is set."""
        client = _make_mock_client()
        _check_stream_access(client, 'agents')
        call_args = client.request.call_args
        self.assertEqual(call_args[1]['path'], 'agents')


class TestApplyAccessChecks(unittest.TestCase):
    """_apply_access_checks() removes inaccessible streams from schemas."""

    def test_all_accessible_no_changes(self):
        """When all streams are accessible, schemas remain unchanged."""
        client = _make_mock_client()
        from tap_ujet.schema import get_schemas
        schemas, field_metadata = get_schemas()
        original_keys = set(schemas.keys())
        _apply_access_checks(client, schemas, field_metadata)
        self.assertEqual(set(schemas.keys()), original_keys)

    def test_single_stream_excluded(self):
        """A stream returning 403 should be removed from schemas."""
        client = MagicMock()
        def side_effect(method, path=None, **kwargs):
            if path == 'agents':
                raise UjetForbiddenError("403 Forbidden")
            return ([], 0, None)
        client.request.side_effect = side_effect

        from tap_ujet.schema import get_schemas
        schemas, field_metadata = get_schemas()
        _apply_access_checks(client, schemas, field_metadata)
        self.assertNotIn('agents', schemas)
        self.assertNotIn('agents', field_metadata)

    def test_partial_access_retains_accessible_streams(self):
        """Accessible streams remain in schemas when others are excluded."""
        client = MagicMock()
        def side_effect(method, path=None, **kwargs):
            if path == 'agents':
                raise UjetForbiddenError("403 Forbidden")
            return ([], 0, None)
        client.request.side_effect = side_effect

        from tap_ujet.schema import get_schemas
        schemas, field_metadata = get_schemas()
        _apply_access_checks(client, schemas, field_metadata)
        self.assertIn('calls', schemas)
        self.assertIn('chats', schemas)

    def test_all_inaccessible_raises_forbidden(self):
        """When ALL streams return 403, should raise UjetForbiddenError."""
        client = MagicMock()
        client.request.side_effect = UjetForbiddenError("403 Forbidden")

        from tap_ujet.schema import get_schemas
        schemas, field_metadata = get_schemas()
        with self.assertRaises(UjetForbiddenError) as ctx:
            _apply_access_checks(client, schemas, field_metadata)
        self.assertEqual(
            str(ctx.exception),
            "No streams are accessible. Ensure the credentials have read permission for at least one stream."
        )

    @patch('tap_ujet.discover.LOGGER')
    def test_inaccessible_streams_logged_as_warning(self, mock_logger):
        """When some streams are excluded, a warning listing them is logged."""
        client = MagicMock()
        def side_effect(method, path=None, **kwargs):
            if path == 'agents':
                raise UjetForbiddenError("403 Forbidden")
            return ([], 0, None)
        client.request.side_effect = side_effect

        from tap_ujet.schema import get_schemas
        schemas, field_metadata = get_schemas()
        _apply_access_checks(client, schemas, field_metadata)

        warning_calls = [
            call for call in mock_logger.warning.call_args_list
            if 'Unauthorized streams excluded from catalog' in str(call)
        ]
        self.assertEqual(len(warning_calls), 1)
        self.assertIn('agents', str(warning_calls[0]))

    def test_discover_excludes_forbidden_streams(self):
        """discover() should return catalog without forbidden streams."""
        client = MagicMock()
        def side_effect(method, path=None, **kwargs):
            if path == 'calls':
                raise UjetForbiddenError("403 Forbidden")
            return ([], 0, None)
        client.request.side_effect = side_effect

        catalog = discover(client)
        stream_names = {entry.stream for entry in catalog.streams}
        self.assertNotIn('calls', stream_names)
        self.assertIn('agents', stream_names)
