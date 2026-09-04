import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_referral_saasquatch.exceptions import ReferralSaasquatchForbiddenError
from tap_referral_saasquatch.schema import get_schemas
from tap_referral_saasquatch.streams import STREAMS

LOGGER = singer.get_logger()


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """Exclude streams credentials cannot access."""
    inaccessible_streams = [
        stream_name
        for stream_name, stream_obj in STREAMS.items()
        if stream_name in schemas
        and not stream_obj(client=client).check_access()
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    if not schemas:
        raise ReferralSaasquatchForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams."
        )
    elif inaccessible_streams:
        LOGGER.warning(
            "Unauthorized streams excluded from catalog: %s",
            ", ".join(inaccessible_streams),
        )


def discover(client) -> Catalog:
    """Run discovery and return catalog entries for accessible streams only."""
    schemas, field_metadata = get_schemas()
    _apply_access_checks(client, schemas, field_metadata)
    catalog = Catalog([])
    for stream_name, schema_dict in schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error(f"stream_name: {stream_name}")
            LOGGER.error(f"type schema_dict: {type(schema_dict)}")
            raise err
        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")
        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )
    return catalog
