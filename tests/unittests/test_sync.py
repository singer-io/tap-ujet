import unittest
from unittest.mock import patch, MagicMock, call

from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_ujet.sync import (
    get_bookmark,
    write_bookmark,
    write_schema,
    write_record,
    process_records,
    update_currently_syncing,
    get_selected_fields,
    transform_datetime,
    sync_endpoint,
    sync,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_test_catalog(stream_name='agents',
                       replication_method='INCREMENTAL',
                       replication_keys=None,
                       schema=None):
    """Build a minimal Catalog suitable for sync tests."""
    if schema is None:
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'status_updated_at': {'type': ['null', 'string'], 'format': 'date-time'},
                'name': {'type': ['null', 'string']},
            }
        }
    if replication_keys is None and replication_method == 'INCREMENTAL':
        replication_keys = ['status_updated_at']

    mdata = metadata.get_standard_metadata(
        schema=schema,
        key_properties=['id'],
        valid_replication_keys=replication_keys,
        replication_method=replication_method,
    )
    # Mark all non-root fields as selected
    mdata_map = metadata.to_map(mdata)
    for breadcrumb_key in mdata_map:
        if breadcrumb_key != ():
            mdata_map[breadcrumb_key]['selected'] = True
    mdata = metadata.to_list(mdata_map)

    return Catalog([
        CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=['id'],
            schema=Schema.from_dict(schema),
            metadata=mdata,
        )
    ])


def _make_full_table_catalog(stream_name='teams'):
    schema = {
        'type': 'object',
        'properties': {
            'id': {'type': ['null', 'integer']},
            'name': {'type': ['null', 'string']},
        }
    }
    mdata = metadata.get_standard_metadata(
        schema=schema,
        key_properties=['id'],
        valid_replication_keys=None,
        replication_method='FULL_TABLE',
    )
    mdata_map = metadata.to_map(mdata)
    for breadcrumb_key in mdata_map:
        if breadcrumb_key != ():
            mdata_map[breadcrumb_key]['selected'] = True
    mdata = metadata.to_list(mdata_map)

    return Catalog([
        CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=['id'],
            schema=Schema.from_dict(schema),
            metadata=mdata,
        )
    ])


# ---------------------------------------------------------------------------
# get_bookmark
# ---------------------------------------------------------------------------

class TestGetBookmark(unittest.TestCase):
    """get_bookmark reads bookmark values from Singer state."""

    def test_returns_default_when_state_is_none(self):
        result = get_bookmark(None, 'agents', '2024-01-01T00:00:00Z')
        self.assertEqual(result, '2024-01-01T00:00:00Z')

    def test_returns_default_when_bookmarks_key_missing(self):
        state = {}
        result = get_bookmark(state, 'agents', '2024-01-01T00:00:00Z')
        self.assertEqual(result, '2024-01-01T00:00:00Z')

    def test_returns_default_when_stream_not_in_bookmarks(self):
        state = {'bookmarks': {'other_stream': '2023-01-01T00:00:00Z'}}
        result = get_bookmark(state, 'agents', '2024-01-01T00:00:00Z')
        self.assertEqual(result, '2024-01-01T00:00:00Z')

    def test_returns_stored_bookmark_value(self):
        state = {'bookmarks': {'agents': '2024-06-15T12:00:00Z'}}
        result = get_bookmark(state, 'agents', '2024-01-01T00:00:00Z')
        self.assertEqual(result, '2024-06-15T12:00:00Z')

    def test_does_not_mutate_state(self):
        state = {'bookmarks': {'agents': '2024-06-15T12:00:00Z'}}
        get_bookmark(state, 'agents', '2024-01-01T00:00:00Z')
        self.assertEqual(state, {'bookmarks': {'agents': '2024-06-15T12:00:00Z'}})


# ---------------------------------------------------------------------------
# write_bookmark
# ---------------------------------------------------------------------------

class TestWriteBookmark(unittest.TestCase):
    """write_bookmark persists bookmark value and emits a Singer state message."""

    @patch('singer.write_state')
    def test_creates_bookmarks_key_if_absent(self, mock_write_state):
        state = {}
        write_bookmark(state, 'agents', '2024-06-01T00:00:00Z')
        self.assertIn('bookmarks', state)

    @patch('singer.write_state')
    def test_sets_correct_stream_value(self, mock_write_state):
        state = {}
        write_bookmark(state, 'agents', '2024-06-01T00:00:00Z')
        self.assertEqual(state['bookmarks']['agents'], '2024-06-01T00:00:00Z')

    @patch('singer.write_state')
    def test_updates_existing_bookmark(self, mock_write_state):
        state = {'bookmarks': {'agents': '2024-01-01T00:00:00Z'}}
        write_bookmark(state, 'agents', '2024-06-01T00:00:00Z')
        self.assertEqual(state['bookmarks']['agents'], '2024-06-01T00:00:00Z')

    @patch('singer.write_state')
    def test_calls_singer_write_state(self, mock_write_state):
        state = {}
        write_bookmark(state, 'agents', '2024-06-01T00:00:00Z')
        mock_write_state.assert_called_once()

    @patch('singer.write_state')
    def test_does_not_overwrite_other_streams(self, mock_write_state):
        state = {'bookmarks': {'calls': '2024-03-01T00:00:00Z'}}
        write_bookmark(state, 'agents', '2024-06-01T00:00:00Z')
        self.assertEqual(state['bookmarks']['calls'], '2024-03-01T00:00:00Z')


# ---------------------------------------------------------------------------
# transform_datetime
# ---------------------------------------------------------------------------

class TestTransformDatetime(unittest.TestCase):
    """transform_datetime converts datetime strings to RFC3339."""

    def test_iso_utc_string_round_trips(self):
        """RFC3339 UTC string is returned unchanged (or normalised)."""
        result = transform_datetime('2024-01-15T12:30:00Z')
        self.assertIsNotNone(result)
        self.assertIn('2024-01-15', result)

    def test_returns_none_for_none(self):
        result = transform_datetime(None)
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# write_schema
# ---------------------------------------------------------------------------

class TestWriteSchema(unittest.TestCase):
    """write_schema emits a Singer SCHEMA message for the stream."""

    @patch('singer.write_schema')
    def test_calls_singer_write_schema_with_correct_stream(self, mock_write_schema):
        catalog = _make_test_catalog()
        write_schema(catalog, 'agents')
        self.assertEqual(mock_write_schema.call_args[0][0], 'agents')

    @patch('singer.write_schema')
    def test_calls_singer_write_schema_with_key_properties(self, mock_write_schema):
        catalog = _make_test_catalog()
        write_schema(catalog, 'agents')
        self.assertEqual(mock_write_schema.call_args[0][2], ['id'])

    @patch('singer.write_schema', side_effect=OSError('Broken pipe'))
    def test_raises_os_error_on_write_failure(self, mock_write_schema):
        catalog = _make_test_catalog()
        with self.assertRaises(OSError):
            write_schema(catalog, 'agents')


# ---------------------------------------------------------------------------
# write_record
# ---------------------------------------------------------------------------

class TestWriteRecord(unittest.TestCase):
    """write_record emits a Singer RECORD message."""

    @patch('singer.messages.write_record')
    def test_calls_singer_write_record(self, mock_write_record):
        from singer.utils import now
        write_record('agents', {'id': 1, 'name': 'Alice'}, now())
        mock_write_record.assert_called_once()

    @patch('singer.messages.write_record')
    def test_passes_stream_name(self, mock_write_record):
        from singer.utils import now
        write_record('agents', {'id': 1}, now())
        self.assertEqual(mock_write_record.call_args[0][0], 'agents')

    @patch('singer.messages.write_record', side_effect=OSError('Broken pipe'))
    def test_raises_os_error_on_failure(self, mock_write_record):
        from singer.utils import now
        with self.assertRaises(OSError):
            write_record('agents', {'id': 1}, now())


# ---------------------------------------------------------------------------
# process_records
# ---------------------------------------------------------------------------

class TestProcessRecords(unittest.TestCase):
    """process_records transforms records and applies bookmark filtering."""

    @patch('singer.messages.write_record')
    def test_full_table_writes_all_records(self, mock_write_record):
        """FULL_TABLE streams write every record (no bookmark filtering)."""
        catalog = _make_full_table_catalog('teams')
        records = [{'id': 1, 'name': 'Team A'}, {'id': 2, 'name': 'Team B'}]
        from singer.utils import now
        max_bv, count = process_records(
            catalog=catalog,
            stream_name='teams',
            records=records,
            time_extracted=now(),
            bookmark_field=None,
        )
        self.assertEqual(count, 2)
        self.assertEqual(mock_write_record.call_count, 2)

    @patch('singer.messages.write_record')
    def test_incremental_filters_records_before_bookmark(self, mock_write_record):
        """Records with bookmark value before last_datetime are skipped."""
        catalog = _make_test_catalog()
        records = [
            {'id': 1, 'name': 'Old',    'status_updated_at': '2023-06-01T00:00:00Z'},
            {'id': 2, 'name': 'Recent', 'status_updated_at': '2024-06-01T00:00:00Z'},
        ]
        from singer.utils import now
        _, count = process_records(
            catalog=catalog,
            stream_name='agents',
            records=records,
            time_extracted=now(),
            bookmark_field='status_updated_at',
            bookmark_type='datetime',
            last_datetime='2024-01-01T00:00:00Z',
        )
        self.assertEqual(count, 1)

    @patch('singer.messages.write_record')
    def test_incremental_writes_records_at_or_after_bookmark(self, mock_write_record):
        """Records with bookmark value >= last_datetime are written."""
        catalog = _make_test_catalog()
        records = [
            {'id': 1, 'name': 'First',  'status_updated_at': '2024-01-01T00:00:00Z'},  # equal
            {'id': 2, 'name': 'Second', 'status_updated_at': '2024-06-01T00:00:00Z'},  # after
        ]
        from singer.utils import now
        _, count = process_records(
            catalog=catalog,
            stream_name='agents',
            records=records,
            time_extracted=now(),
            bookmark_field='status_updated_at',
            bookmark_type='datetime',
            last_datetime='2024-01-01T00:00:00Z',
        )
        self.assertEqual(count, 2)

    @patch('singer.messages.write_record')
    def test_max_bookmark_value_updated_to_highest(self, mock_write_record):
        """max_bookmark_value is set to the most recent bookmark in the batch."""
        catalog = _make_test_catalog()
        records = [
            {'id': 1, 'name': 'A', 'status_updated_at': '2024-03-01T00:00:00Z'},
            {'id': 2, 'name': 'B', 'status_updated_at': '2024-06-01T00:00:00Z'},
            {'id': 3, 'name': 'C', 'status_updated_at': '2024-04-01T00:00:00Z'},
        ]
        from singer.utils import now
        max_bv, _ = process_records(
            catalog=catalog,
            stream_name='agents',
            records=records,
            time_extracted=now(),
            bookmark_field='status_updated_at',
            bookmark_type='datetime',
            last_datetime='2024-01-01T00:00:00Z',
        )
        self.assertIn('2024-06-01', max_bv)

    @patch('singer.messages.write_record')
    def test_parent_id_injected_into_record(self, mock_write_record):
        """When parent_id is provided, the parent FK is added to every record."""
        catalog = _make_full_table_catalog('teams')
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'name': {'type': ['null', 'string']},
                'agent_id': {'type': ['null', 'integer']},
            }
        }
        mdata = metadata.get_standard_metadata(
            schema=schema, key_properties=['id'], replication_method='FULL_TABLE'
        )
        mdata_map = metadata.to_map(mdata)
        for k in mdata_map:
            if k != ():
                mdata_map[k]['selected'] = True
        catalog2 = Catalog([
            CatalogEntry(
                stream='teams',
                tap_stream_id='teams',
                key_properties=['id'],
                schema=Schema.from_dict(schema),
                metadata=metadata.to_list(mdata_map),
            )
        ])
        records = [{'id': 99, 'name': 'Dev'}]
        from singer.utils import now
        process_records(
            catalog=catalog2,
            stream_name='teams',
            records=records,
            time_extracted=now(),
            bookmark_field=None,
            parent='agent',
            parent_id=42,
        )
        written_record = mock_write_record.call_args[0][1]
        self.assertEqual(written_record.get('agent_id'), 42)

    @patch('singer.messages.write_record')
    def test_empty_records_returns_zero_count(self, mock_write_record):
        """Empty records list → count of 0."""
        catalog = _make_full_table_catalog('teams')
        from singer.utils import now
        _, count = process_records(
            catalog=catalog,
            stream_name='teams',
            records=[],
            time_extracted=now(),
            bookmark_field=None,
        )
        self.assertEqual(count, 0)
        mock_write_record.assert_not_called()

    @patch('singer.messages.write_record')
    def test_incremental_integer_filters_by_integer_bookmark(self, mock_write_record):
        """Integer bookmark type keeps only records >= last_integer."""
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'sequence': {'type': ['null', 'integer']},
            }
        }
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=['id'],
            valid_replication_keys=['sequence'],
            replication_method='INCREMENTAL',
        )
        mdata_map = metadata.to_map(mdata)
        for k in mdata_map:
            if k != ():
                mdata_map[k]['selected'] = True
        catalog = Catalog([
            CatalogEntry(
                stream='agents',
                tap_stream_id='agents',
                key_properties=['id'],
                schema=Schema.from_dict(schema),
                metadata=metadata.to_list(mdata_map),
            )
        ])
        records = [
            {'id': 1, 'sequence': 50},   # below bookmark — should be filtered
            {'id': 2, 'sequence': 100},  # equal to bookmark — should be written
            {'id': 3, 'sequence': 200},  # above bookmark — should be written
        ]
        from singer.utils import now
        _, count = process_records(
            catalog=catalog,
            stream_name='agents',
            records=records,
            time_extracted=now(),
            bookmark_field='sequence',
            bookmark_type='integer',
            last_integer=100,
        )
        self.assertEqual(count, 2)


# ---------------------------------------------------------------------------
# update_currently_syncing
# ---------------------------------------------------------------------------

class TestUpdateCurrentlySyncing(unittest.TestCase):
    """update_currently_syncing manages the currently_syncing Singer state key."""

    @patch('singer.write_state')
    def test_sets_currently_syncing_stream(self, mock_write_state):
        state = {}
        update_currently_syncing(state, 'agents')
        self.assertEqual(state.get('currently_syncing'), 'agents')

    @patch('singer.write_state')
    def test_removes_currently_syncing_when_stream_is_none(self, mock_write_state):
        state = {'currently_syncing': 'agents'}
        update_currently_syncing(state, None)
        self.assertNotIn('currently_syncing', state)

    @patch('singer.write_state')
    def test_calls_write_state_after_update(self, mock_write_state):
        state = {}
        update_currently_syncing(state, 'agents')
        mock_write_state.assert_called_once()

    @patch('singer.write_state')
    def test_none_without_currently_syncing_does_not_error(self, mock_write_state):
        """Clearing currently_syncing when it is absent must not raise."""
        state = {}
        update_currently_syncing(state, None)
        mock_write_state.assert_called_once()


# ---------------------------------------------------------------------------
# get_selected_fields
# ---------------------------------------------------------------------------

class TestGetSelectedFields(unittest.TestCase):
    """get_selected_fields extracts field names marked as selected in metadata."""

    def test_returns_selected_fields(self):
        """Fields with selected=True are returned."""
        catalog = _make_test_catalog()
        fields = get_selected_fields(catalog, 'agents')
        self.assertIsInstance(fields, list)
        # At least the non-key fields should be included
        self.assertIn('name', fields)

    def test_returns_empty_list_when_nothing_selected(self):
        """No selected fields → returns empty list."""
        schema = {
            'type': 'object',
            'properties': {'id': {'type': ['null', 'integer']}}
        }
        mdata = metadata.get_standard_metadata(
            schema=schema, key_properties=['id'], replication_method='FULL_TABLE'
        )
        # Do NOT set any selected=True
        catalog = Catalog([
            CatalogEntry(
                stream='teams',
                tap_stream_id='teams',
                key_properties=['id'],
                schema=Schema.from_dict(schema),
                metadata=mdata,
            )
        ])
        fields = get_selected_fields(catalog, 'teams')
        self.assertEqual(fields, [])


# ---------------------------------------------------------------------------
# sync_endpoint
# ---------------------------------------------------------------------------

class TestSyncEndpoint(unittest.TestCase):
    """sync_endpoint paginates through API data and writes records."""

    @patch('singer.write_state')
    @patch('singer.messages.write_record')
    def test_returns_total_endpoint_records(self, mock_write_record, mock_write_state):
        """sync_endpoint returns total_endpoint_records from the API response."""
        catalog = _make_test_catalog()
        state = {}
        records = [{'id': 1, 'name': 'A', 'status_updated_at': '2024-06-01T00:00:00Z'}]

        mock_client = MagicMock()
        mock_client.base_url = 'https://myco.ujet.co/manager/api/v1'
        mock_client.get.return_value = (records, 5, None)

        result = sync_endpoint(
            client=mock_client,
            catalog=catalog,
            state=state,
            start_date='2024-01-01T00:00:00Z',
            stream_name='agents',
            path='agents',
            static_params={},
            bookmark_query_field='status_updated_at[from]',
            bookmark_field='status_updated_at',
            bookmark_type='datetime',
        )
        self.assertEqual(result, 5)

    @patch('singer.write_state')
    @patch('singer.messages.write_record')
    def test_empty_data_returns_immediately(self, mock_write_record, mock_write_state):
        """sync_endpoint returns early (no writes) when API data is empty."""
        catalog = _make_test_catalog()
        state = {}

        mock_client = MagicMock()
        mock_client.base_url = 'https://myco.ujet.co/manager/api/v1'
        mock_client.get.return_value = ({}, 0, None)

        result = sync_endpoint(
            client=mock_client,
            catalog=catalog,
            state=state,
            start_date='2024-01-01T00:00:00Z',
            stream_name='agents',
            path='agents',
            static_params={},
            bookmark_query_field=None,
            bookmark_field=None,
            bookmark_type=None,
        )
        self.assertEqual(result, 0)
        mock_write_record.assert_not_called()

    @patch('singer.write_state')
    @patch('singer.messages.write_record')
    def test_writes_bookmark_after_each_page(self, mock_write_record, mock_write_state):
        """write_state is called after each page when bookmark_field is set."""
        catalog = _make_test_catalog()
        state = {}
        records = [{'id': 1, 'name': 'A', 'status_updated_at': '2024-06-01T00:00:00Z'}]

        mock_client = MagicMock()
        mock_client.base_url = 'https://myco.ujet.co/manager/api/v1'
        mock_client.get.return_value = (records, 1, None)

        sync_endpoint(
            client=mock_client,
            catalog=catalog,
            state=state,
            start_date='2024-01-01T00:00:00Z',
            stream_name='agents',
            path='agents',
            static_params={},
            bookmark_query_field='status_updated_at[from]',
            bookmark_field='status_updated_at',
            bookmark_type='datetime',
        )
        mock_write_state.assert_called()

    @patch('singer.write_state')
    @patch('singer.messages.write_record')
    def test_follows_pagination_next_url(self, mock_write_record, mock_write_state):
        """sync_endpoint follows the next_url until it is None."""
        catalog = _make_test_catalog()
        state = {}
        records_p1 = [{'id': 1, 'name': 'A', 'status_updated_at': '2024-06-01T00:00:00Z'}]
        records_p2 = [{'id': 2, 'name': 'B', 'status_updated_at': '2024-07-01T00:00:00Z'}]
        next_url = 'https://myco.ujet.co/manager/api/v1/agents?page=2'

        mock_client = MagicMock()
        mock_client.base_url = 'https://myco.ujet.co/manager/api/v1'
        mock_client.get.side_effect = [
            (records_p1, 2, next_url),
            (records_p2, 2, None),
        ]

        sync_endpoint(
            client=mock_client,
            catalog=catalog,
            state=state,
            start_date='2024-01-01T00:00:00Z',
            stream_name='agents',
            path='agents',
            static_params={},
            bookmark_query_field='status_updated_at[from]',
            bookmark_field='status_updated_at',
            bookmark_type='datetime',
        )
        self.assertEqual(mock_client.get.call_count, 2)

    @patch('singer.write_state')
    @patch('singer.messages.write_record')
    def test_full_table_no_bookmark_written(self, mock_write_record, mock_write_state):
        """FULL_TABLE sync does not write state (no bookmark_field)."""
        catalog = _make_full_table_catalog('teams')
        state = {}
        records = [{'id': 1, 'name': 'Team A'}]

        mock_client = MagicMock()
        mock_client.base_url = 'https://myco.ujet.co/manager/api/v1'
        mock_client.get.return_value = (records, 1, None)

        sync_endpoint(
            client=mock_client,
            catalog=catalog,
            state=state,
            start_date='2024-01-01T00:00:00Z',
            stream_name='teams',
            path='teams',
            static_params={},
            bookmark_query_field=None,
            bookmark_field=None,
            bookmark_type=None,
        )
        mock_write_state.assert_not_called()

    @patch('singer.write_state')
    @patch('singer.messages.write_record')
    def test_integer_bookmark_type_initialises_from_state(self, mock_write_record, mock_write_state):
        """sync_endpoint initialises last_integer from state when bookmark_type='integer'."""
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'sequence': {'type': ['null', 'integer']},
            }
        }
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=['id'],
            valid_replication_keys=['sequence'],
            replication_method='INCREMENTAL',
        )
        mdata_map = metadata.to_map(mdata)
        for k in mdata_map:
            if k != ():
                mdata_map[k]['selected'] = True
        catalog = Catalog([
            CatalogEntry(
                stream='agents',
                tap_stream_id='agents',
                key_properties=['id'],
                schema=Schema.from_dict(schema),
                metadata=metadata.to_list(mdata_map),
            )
        ])
        state = {'bookmarks': {'agents': 50}}
        records = [{'id': 1, 'sequence': 100}]

        mock_client = MagicMock()
        mock_client.base_url = 'https://myco.ujet.co/manager/api/v1'
        mock_client.get.return_value = (records, 1, None)

        result = sync_endpoint(
            client=mock_client,
            catalog=catalog,
            state=state,
            start_date='2024-01-01T00:00:00Z',
            stream_name='agents',
            path='agents',
            static_params={},
            bookmark_query_field=None,
            bookmark_field='sequence',
            bookmark_type='integer',
        )
        self.assertEqual(result, 1)
        mock_write_record.assert_called_once()

    @patch('singer.write_state')
    @patch('singer.messages.write_record')
    def test_non_list_data_does_not_write_records(self, mock_write_record, mock_write_state):
        """Non-list API response (no transformed_data) causes early return without writes."""
        catalog = _make_full_table_catalog('teams')
        state = {}

        mock_client = MagicMock()
        mock_client.base_url = 'https://myco.ujet.co/manager/api/v1'
        # Return a dict instead of a list — transform_json won't populate transformed_data
        mock_client.get.return_value = ({'message': 'ok'}, 1, None)

        result = sync_endpoint(
            client=mock_client,
            catalog=catalog,
            state=state,
            start_date='2024-01-01T00:00:00Z',
            stream_name='teams',
            path='teams',
            static_params={},
            bookmark_query_field=None,
            bookmark_field=None,
            bookmark_type=None,
        )
        self.assertEqual(result, 1)
        mock_write_record.assert_not_called()


# ---------------------------------------------------------------------------
# sync (top-level)
# ---------------------------------------------------------------------------

class TestSync(unittest.TestCase):
    """sync() orchestrates stream selection, schema output, and sync_endpoint calls."""

    @patch('singer.write_state')
    @patch('singer.write_schema')
    def test_no_selected_streams_returns_early(self, mock_write_schema, mock_write_state):
        """sync() returns without writing anything when no streams are selected."""
        catalog = MagicMock()
        catalog.get_selected_streams.return_value = []
        mock_client = MagicMock()
        config = {'start_date': '2024-01-01T00:00:00Z'}
        state = {}

        sync(client=mock_client, config=config, catalog=catalog, state=state)

        mock_write_schema.assert_not_called()
        mock_client.get.assert_not_called()

    @patch('tap_ujet.sync.sync_endpoint', return_value=10)
    @patch('singer.write_state')
    @patch('singer.write_schema')
    def test_write_schema_called_per_stream(self, mock_write_schema, mock_write_state,
                                            mock_sync_endpoint):
        """sync() calls write_schema once for each selected stream."""
        catalog = _make_test_catalog()
        # Mark the stream as selected
        for entry in catalog.streams:
            entry.metadata = [
                m for m in entry.metadata
            ]
        mock_client = MagicMock()
        mock_client.base_url = 'https://myco.ujet.co/manager/api/v1'
        config = {'start_date': '2024-01-01T00:00:00Z'}
        state = {}

        with patch.object(catalog, 'get_selected_streams', return_value=catalog.streams):
            sync(client=mock_client, config=config, catalog=catalog, state=state)

        self.assertEqual(mock_write_schema.call_count, 1)

    @patch('tap_ujet.sync.sync_endpoint', return_value=5)
    @patch('singer.write_state')
    @patch('singer.write_schema')
    def test_currently_syncing_cleared_after_stream(self, mock_write_schema,
                                                     mock_write_state, mock_sync_endpoint):
        """currently_syncing is cleared in state after each stream finishes."""
        catalog = _make_full_table_catalog('teams')
        mock_client = MagicMock()
        mock_client.base_url = 'https://myco.ujet.co/manager/api/v1'
        config = {'start_date': '2024-01-01T00:00:00Z'}
        state = {}

        with patch.object(catalog, 'get_selected_streams', return_value=catalog.streams):
            sync(client=mock_client, config=config, catalog=catalog, state=state)

        # After sync finishes, currently_syncing must not be in state
        self.assertNotIn('currently_syncing', state)


if __name__ == '__main__':
    unittest.main()
