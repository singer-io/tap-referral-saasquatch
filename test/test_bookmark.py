import json
import unittest
from unittest.mock import MagicMock, patch

from tap_referral_saasquatch import CONFIG, STATE, do_sync, request_export
from tap_referral_saasquatch.discover import discover


class BookmarkIntegrationTest(unittest.TestCase):
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

    @patch("tap_referral_saasquatch.export_ready", return_value=True)
    @patch("tap_referral_saasquatch.session.send")
    def test_request_export_uses_existing_bookmark(self, mock_send, _mock_export_ready):
        STATE["users"] = "2025-06-01T00:00:00Z"

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"id": "exp-users"}
        mock_send.return_value = response

        export_id = request_export("users")
        self.assertEqual(export_id, "exp-users")

        prepared_request = mock_send.call_args[0][0]
        body = prepared_request.body.decode("utf-8") if isinstance(prepared_request.body, bytes) else prepared_request.body
        payload = json.loads(body)

        self.assertEqual(payload["params"]["createdOrUpdatedSince"], "2025-06-01T00:00:00Z")

    @patch("tap_referral_saasquatch.singer.write_state")
    @patch("tap_referral_saasquatch.write_record")
    @patch("tap_referral_saasquatch.singer.write_schema")
    @patch("tap_referral_saasquatch.stream_export")
    @patch("tap_referral_saasquatch.request_export")
    def test_sync_advances_bookmark_state(
        self,
        mock_request_export,
        mock_stream_export,
        _mock_write_schema,
        _mock_write_record,
        _mock_write_state,
    ):
        old_bookmark = "2025-01-01T00:00:00Z"
        STATE["users"] = old_bookmark

        catalog = discover()
        catalog.metadata = []
        catalog.get_selected_streams = lambda _state: [
            stream for stream in catalog.streams if stream.tap_stream_id == "users"
        ]

        mock_request_export.return_value = "exp-users"
        mock_stream_export.return_value = [
            {"id": "u1", "accountId": "a1", "dateCreated": "2025-01-01T00:00:00Z"}
        ]

        do_sync(catalog)

        mock_request_export.assert_called_once_with("users")
        self.assertIn("users", STATE)
        self.assertGreaterEqual(STATE["users"], old_bookmark)
