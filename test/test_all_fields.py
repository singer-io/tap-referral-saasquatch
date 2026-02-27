import unittest
from unittest.mock import patch

from tap_referral_saasquatch import CONFIG, STATE, do_sync
from tap_referral_saasquatch.discover import discover


class AllFieldsIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.original_config = dict(CONFIG)
        self.original_state = dict(STATE)

        CONFIG.update(
            {
                "api_key": "dummy-key",
                "tenant_alias": "dummy-tenant",
                "start_date": "2025-01-01T00:00:00Z",
            }
        )
        STATE.clear()

    def tearDown(self):
        CONFIG.clear()
        CONFIG.update(self.original_config)
        STATE.clear()
        STATE.update(self.original_state)

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

        data_by_stream = {
            "users": [{"id": "u1", "accountId": "a1", "dateCreated": "2025-01-01T00:00:00Z"}],
            "reward_balances": [{"userId": "u1", "accountId": "a1", "amount": "5"}],
            "referrals": [{"id": "r1", "dateReferralStarted": "2025-01-01T00:00:00Z"}],
        }
        mock_stream_export.side_effect = lambda stream, _export_id: data_by_stream[stream]

        do_sync(catalog)

        self.assertEqual(mock_request_export.call_count, 3)
        self.assertEqual(mock_stream_export.call_count, 3)
        self.assertEqual(mock_write_record.call_count, 3)

        self.assertIn("users", STATE)
        self.assertIn("reward_balances", STATE)
        self.assertIn("referrals", STATE)
