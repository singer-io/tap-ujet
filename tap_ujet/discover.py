import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_ujet.client import UjetForbiddenError, UjetUnauthorizedError
from tap_ujet.schema import get_schemas, STREAMS

LOGGER = singer.get_logger()


def _check_stream_access(client, stream_name):
    """
    Probe a stream's endpoint for read access.
    Returns True if accessible, False if a 403 Forbidden error is raised.
    """
    stream_config = STREAMS[stream_name]
    path = stream_config.get('path', stream_name)
    try:
        client.request('GET', path=path, params={'per': 1}, endpoint=stream_name)
        return True
    except (UjetForbiddenError, UjetUnauthorizedError) as exc:
        LOGGER.warning(
            "Unauthorized Stream: %s, excluding from catalog. HTTP-Error-Message:'%s'",
            stream_name,
            str(exc)
        )
        return False


def _apply_access_checks(client, schemas, field_metadata):
    """
    Probe each stream for read access and remove inaccessible streams
    from schemas and field_metadata in place.
    Raises UjetForbiddenError if no streams are accessible.
    """
    inaccessible_streams = [
        stream_name
        for stream_name in list(schemas.keys())
        if not _check_stream_access(client, stream_name)
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    if not schemas:
        raise UjetForbiddenError(
            "No streams are accessible. "
            "Ensure the credentials have read permission "
            "for at least one stream."
        )
    
    if inaccessible_streams:
        LOGGER.warning(
            "Unauthorized streams excluded from catalog: %s",
            ", ".join(inaccessible_streams),
        )


def discover(client):
    """
    Run the discovery mode, prepare the catalog file and return the catalog.
    Access to each stream is verified using the provided client, and streams
    that the credentials cannot read are excluded from the returned catalog.
    """
    schemas, field_metadata = get_schemas()
    _apply_access_checks(client, schemas, field_metadata)

    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        LOGGER.info('discover schema for stream: {}'.format(stream_name))
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]
        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=STREAMS[stream_name]['key_properties'],
            schema=schema,
            metadata=mdata
        ))

    LOGGER.info('Returning catalog: {}'.format(catalog))
    return catalog
