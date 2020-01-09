# streams: API URL endpoints to be called
# properties:
#   <root node>: Plural stream name for the endpoint
#   path: API endpoint relative path, when added to the base URL, creates the full path,
#       default = stream_name
#   key_properties: Primary key fields for identifying an endpoint record.
#   replication_method: INCREMENTAL or FULL_TABLE
#   replication_keys: bookmark_field(s), typically a date-time, used for filtering the results
#        and setting the state
#   params: Query, sort, and other endpoint specific parameters; default = {}
#   data_key: JSON element containing the results list for the endpoint;
#        default = root (no data_key)
#   bookmark_query_field: From date-time field used for filtering the query
#   bookmark_type: Data type for bookmark, integer or datetime


STREAMS = {
    'agents': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['status_updated_at'],
        'bookmark_query_field': 'status_updated_at[from]',
        'bookmark_type': 'datetime',
        'params': {
            'sort_column': 'status_updated_at',
            'sort_direction': 'ASC'
        }
    },
    'agent_activity_logs': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['started_at'],
        'bookmark_query_field': 'started_at[from]',
        'bookmark_type': 'datetime',
        'params': {
            'sort_column': 'started_at',
            'sort_direction': 'ASC'
        }
    },
    'calls': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated_at'],
        'bookmark_query_field': 'updated_at[from]',
        'bookmark_type': 'datetime',
        'params': {
            'sort_column': 'updated_at',
            'sort_direction': 'ASC'
        }
    },
    'chats': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated_at'],
        'bookmark_query_field': 'updated_at[from]',
        'bookmark_type': 'datetime',
        'params': {
            'sort_column': 'updated_at',
            'sort_direction': 'ASC'
        }
    },
    'menu_tree': {
        'path': 'menus/tree',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'menus': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'teams': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'team_tree': {
        'path': 'teams/tree',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
    },
    'user_statuses': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    }
}
