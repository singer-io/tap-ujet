import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_ujet.schema import get_schemas, STREAMS

LOGGER = singer.get_logger()


def discover():
    schemas, field_metadata = get_schemas()
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
