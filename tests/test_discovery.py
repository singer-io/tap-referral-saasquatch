import unittest
from unittest.mock import MagicMock

from singer import metadata

from tap_referral_saasquatch.discover import discover

try:
    from .base import ReferralBaseTest
except ImportError:
    from base import ReferralBaseTest


class DiscoveryIntegrationTest(ReferralBaseTest, unittest.TestCase):
    def test_discovery_expected_streams_and_metadata(self):
        catalog = discover(MagicMock())
        stream_map = {stream.tap_stream_id: stream for stream in catalog.streams}
        expected_streams = self.expected_metadata()

        self.assertEqual(set(stream_map.keys()), set(expected_streams.keys()))

        for stream_name, stream_expected in expected_streams.items():
            with self.subTest(stream=stream_name):
                root_metadata = metadata.to_map(stream_map[stream_name].metadata)[()]
                self.assertEqual(
                    set(root_metadata.get("table-key-properties", [])),
                    stream_expected[self.PRIMARY_KEYS],
                )
                self.assertEqual(
                    root_metadata.get("forced-replication-method"),
                    stream_expected[self.REPLICATION_METHOD],
                )

                actual_replication_keys = root_metadata.get("valid-replication-keys", [])
                if isinstance(actual_replication_keys, str):
                    actual_replication_keys = {actual_replication_keys}
                else:
                    actual_replication_keys = set(actual_replication_keys)

                self.assertEqual(
                    actual_replication_keys,
                    stream_expected[self.REPLICATION_KEYS],
                )
