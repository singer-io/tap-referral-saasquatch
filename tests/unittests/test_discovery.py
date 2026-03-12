import unittest

from singer import metadata

from tap_referral_saasquatch.discover import discover
from tap_referral_saasquatch.schema import get_schemas
from tap_referral_saasquatch.streams import STREAMS


class TestDiscoveryAndSchema(unittest.TestCase):
    def test_get_schemas_returns_all_streams(self):
        schemas, field_metadata = get_schemas()

        self.assertEqual(set(schemas.keys()), set(STREAMS.keys()))
        self.assertEqual(set(field_metadata.keys()), set(STREAMS.keys()))

    def test_replication_key_is_automatic_in_metadata(self):
        _, field_metadata = get_schemas()
        users_metadata_map = metadata.to_map(field_metadata["users"])

        inclusion = users_metadata_map[("properties", "dateCreated")]["inclusion"]
        self.assertEqual(inclusion, "automatic")

    def test_discover_builds_catalog_entries(self):
        catalog = discover()
        stream_names = [stream.tap_stream_id for stream in catalog.streams]

        self.assertEqual(set(stream_names), set(STREAMS.keys()))
