import unittest
from unittest.mock import patch

from tap_referral_saasquatch import STATE, do_sync
from tap_referral_saasquatch.discover import discover
from tap_referral_saasquatch.schema import get_schemas
from tap_referral_saasquatch.streams import STREAMS

try:
    from .base import ReferralBaseTest
except ImportError:
    from base import ReferralBaseTest


class AllFieldsIntegrationTest(ReferralBaseTest, unittest.TestCase):

    @patch("tap_referral_saasquatch.singer.write_state")
    @patch("tap_referral_saasquatch.write_record")
    @patch("tap_referral_saasquatch.singer.write_schema")
    @patch("tap_referral_saasquatch.stream_export")
    @patch("tap_referral_saasquatch.request_export")
    def test_sync_all_streams_with_mocked_exports(
        self,
        mock_request_export,
        mock_stream_export,
        _mock_write_schema,
        mock_write_record,
        _mock_write_state,
    ):
        catalog = discover()
        catalog.metadata = []
        catalog.get_selected_streams = lambda _state: catalog.streams

        mock_request_export.side_effect = lambda stream: f"exp-{stream}"

        schemas, _ = get_schemas()
        data_by_stream = {
            stream_name: [self._generate_value(schema, date_value="2025-02-01T00:00:00Z")]
            for stream_name, schema in schemas.items()
        }
        mock_stream_export.side_effect = lambda stream, _export_id: data_by_stream[stream]

        do_sync(catalog)

        self.assertEqual(mock_request_export.call_count, 3)
        self.assertEqual(mock_stream_export.call_count, 3)
        self.assertEqual(mock_write_record.call_count, 3)

        written_streams = {call_args.args[0] for call_args in mock_write_record.call_args_list}
        self.assertEqual(written_streams, set(STREAMS.keys()))

        for stream_name, stream_cls in STREAMS.items():
            replication_key = stream_cls.replication_keys
            if replication_key:
                with self.subTest(stream=stream_name, replication_key=replication_key):
                    record = data_by_stream[stream_name][0]
                    self.assertIn(replication_key, record)

        self.assertIn("users", STATE)
        self.assertIn("reward_balances", STATE)
        self.assertIn("referrals", STATE)
