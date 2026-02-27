import unittest

from singer import metadata

from tap_referral_saasquatch.discover import discover


class DiscoveryIntegrationTest(unittest.TestCase):
    def test_discovery_expected_streams_and_metadata(self):
        catalog = discover()
        stream_map = {stream.tap_stream_id: stream for stream in catalog.streams}

        self.assertEqual(set(stream_map.keys()), {"users", "reward_balances", "referrals"})

        users_meta = metadata.to_map(stream_map["users"].metadata)
        self.assertEqual(users_meta[()].get("forced-replication-method"), "INCREMENTAL")
        self.assertEqual(set(users_meta[()].get("table-key-properties", [])), {"id", "accountId"})
        self.assertEqual(users_meta[()].get("valid-replication-keys"), "dateCreated")

        rewards_meta = metadata.to_map(stream_map["reward_balances"].metadata)
        self.assertEqual(rewards_meta[()].get("forced-replication-method"), "FULL_TABLE")
        self.assertEqual(set(rewards_meta[()].get("table-key-properties", [])), {"userId", "accountId"})
