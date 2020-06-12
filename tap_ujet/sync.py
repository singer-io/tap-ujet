import singer
from singer import (UNIX_SECONDS_INTEGER_DATETIME_PARSING, Transformer,
                    metadata, metrics, utils)
from tap_ujet.streams import STREAMS
from tap_ujet.transform import transform_json

LOGGER = singer.get_logger()


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: {}'.format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: {}'.format(stream_name))
        LOGGER.info('record: {}'.format(record))
        raise err


def get_bookmark(state, stream, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: {}, value: {}'.format(stream, value))
    singer.write_state(state)


# def transform_datetime(this_dttm):
def transform_datetime(this_dttm):
    with Transformer() as transformer:
        new_dttm = transformer._transform_datetime(this_dttm) # pylint: disable=protected-access
    return new_dttm


def process_records(catalog, #pylint: disable=too-many-branches
                    stream_name,
                    records,
                    time_extracted,
                    bookmark_field=None,
                    bookmark_type=None,
                    max_bookmark_value=None,
                    last_datetime=None,
                    last_integer=None,
                    parent=None,
                    parent_id=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + '_id'] = parent_id

            # Transform record for Singer.io
            with Transformer(integer_datetime_fmt=UNIX_SECONDS_INTEGER_DATETIME_PARSING) \
                as transformer:
                transformed_record = transformer.transform(
                    record,
                    schema,
                    stream_metadata)
                # Reset max_bookmark_value to new value if higher
                if transformed_record.get(bookmark_field):
                    if max_bookmark_value is None or \
                        transformed_record[bookmark_field] > transform_datetime(max_bookmark_value):
                        max_bookmark_value = transformed_record[bookmark_field]

                if bookmark_field and (bookmark_field in transformed_record):
                    if bookmark_type == 'integer':
                        # Keep only records whose bookmark is after the last_integer
                        if transformed_record[bookmark_field] >= last_integer:
                            write_record(stream_name, transformed_record, \
                                time_extracted=time_extracted)
                            counter.increment()
                    elif bookmark_type == 'datetime':
                        last_dttm = transform_datetime(last_datetime)
                        bookmark_dttm = transform_datetime(transformed_record[bookmark_field])
                        # Keep only records whose bookmark is after the last_datetime
                        if bookmark_dttm >= last_dttm:
                            write_record(stream_name, transformed_record, \
                                time_extracted=time_extracted)
                            counter.increment()
                else:
                    write_record(stream_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

        return max_bookmark_value, counter.value


# Sync a specific parent or child endpoint.
def sync_endpoint(client, #pylint: disable=too-many-branches
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  path,
                  static_params,
                  bookmark_query_field=None,
                  bookmark_field=None,
                  bookmark_type=None,
                  parent=None,
                  parent_id=None):

    # Get the latest bookmark for the stream and set the last_integer/datetime
    last_datetime = None
    last_integer = None
    max_bookmark_value = None
    if bookmark_type == 'integer':
        last_integer = get_bookmark(state, stream_name, 0)
        max_bookmark_value = last_integer
    else:
        last_datetime = get_bookmark(state, stream_name, start_date)
        max_bookmark_value = last_datetime

    # pagination: loop thru all pages of data using next_url (if not None)
    page = 1
    offset = 0
    to_rec = 0
    limit = 100 # Default per_page limit is 100
    total_endpoint_records = 0
    next_url = '{}/{}'.format(client.base_url, path)
    params = {
        'page': page,
        'per': limit,
        **static_params # adds in endpoint specific, sort, filter params
    }

    total_processed_records = 0

    while next_url is not None:
        # Need URL querystring for 1st page; subsequent pages provided by next_url
        # querystring: Squash query params into string
        if page == 1:
            if bookmark_query_field:
                params[bookmark_query_field] = max_bookmark_value
            if params != {}:
                querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
        else:
            querystring = None
        LOGGER.info('URL for Stream {}: {}{}'.format(
            stream_name,
            next_url,
            '?{}'.format(querystring) if querystring else ''))

        # API request data
        # total_endpoint_records: API response for all pages
        data = {}
        data, total_endpoint_records, next_url = client.get(
            url=next_url,
            path=path,
            params=querystring,
            endpoint=stream_name)

        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()
        if not data or data is None or data == {}:
            return total_endpoint_records # No data results

        # Transform data with transform_json from transform.py
        # The data_key identifies the array/list of records below the <root> element
        transformed_data = [] # initialize the record list
        if isinstance(data, list):
            transformed_data = transform_json(data, stream_name)

        if not transformed_data or transformed_data is None:
            LOGGER.info('No transformed data for data = {}'.format(data))
            return total_endpoint_records # No data results

        # Process records and get the max_bookmark_value and record_count for the set of records
        max_bookmark_value, record_count = process_records(
            catalog=catalog,
            stream_name=stream_name,
            records=transformed_data,
            time_extracted=time_extracted,
            bookmark_field=bookmark_field,
            bookmark_type=bookmark_type,
            max_bookmark_value=max_bookmark_value,
            last_datetime=last_datetime,
            last_integer=last_integer,
            parent=parent,
            parent_id=parent_id)

        total_processed_records = total_processed_records + record_count
        LOGGER.info('Stream {}, batch processed {} records, total processed records {}'.format(
            stream_name, record_count, total_processed_records))

        # Update the state with the max_bookmark_value for the stream
        if bookmark_field:
            write_bookmark(state, stream_name, max_bookmark_value)

        # to_rec: to record; ending record for the batch page
        to_rec = offset + len(data)
        LOGGER.info('Synced Stream: {}, page: {}, records: {} to {} of {}'.format(
            stream_name,
            page,
            offset,
            to_rec,
            total_endpoint_records))
        # Pagination: increment the offset by the limit (batch-size) and page
        if not next_url:
            offset = offset + len(data)
        else:
            offset = offset + limit
        page = page + 1

    # Return total_endpoint_records across all pages
    LOGGER.info('Synced Stream: {}, pages: {}, total records: {}'.format(
        stream_name,
        page - 1,
        total_endpoint_records))
    return total_endpoint_records


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


# List selected fields from stream catalog
def get_selected_fields(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    mdata = metadata.to_map(stream.metadata)
    mdata_list = singer.metadata.to_list(mdata)
    selected_fields = []
    for entry in mdata_list:
        field = None
        try:
            field = entry['breadcrumb'][1]
            if entry.get('metadata', {}).get('selected', False):
                selected_fields.append(field)
        except IndexError:
            pass
    return selected_fields

def sync(client, config, catalog, state):
    if 'start_date' in config:
        start_date = config['start_date']

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    if not selected_streams:
        return

    # Loop through selected_streams
    for stream_name in selected_streams:
        LOGGER.info('START Syncing: {}'.format(stream_name))
        selected_fields = get_selected_fields(catalog, stream_name)
        LOGGER.info('Stream: {}, selected_fields: {}'.format(stream_name, selected_fields))
        update_currently_syncing(state, stream_name)
        endpoint_config = STREAMS[stream_name]
        path = endpoint_config.get('path', stream_name)
        bookmark_field = next(iter(endpoint_config.get('replication_keys', [])), None)
        write_schema(catalog, stream_name)
        total_records = sync_endpoint(
            client=client,
            catalog=catalog,
            state=state,
            start_date=start_date,
            stream_name=stream_name,
            path=path,
            static_params=endpoint_config.get('params', {}),
            bookmark_query_field=endpoint_config.get('bookmark_query_field', None),
            bookmark_field=bookmark_field,
            bookmark_type=endpoint_config.get('bookmark_type', None))

        update_currently_syncing(state, None)
        LOGGER.info('FINISHED Syncing: {}, total_records: {}'.format(
            stream_name,
            total_records))
