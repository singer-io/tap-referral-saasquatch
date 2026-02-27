import json
import unittest
from unittest.mock import MagicMock, patch

from tap_referral_saasquatch import CONFIG, STATE, request_export


class StartDateIntegrationTest(unittest.TestCase):
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
    def test_request_export_uses_start_date_when_no_bookmark(self, mock_send, _mock_export_ready):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"id": "exp-referrals"}
        mock_send.return_value = response

        export_id = request_export("referrals")
        self.assertEqual(export_id, "exp-referrals")

        prepared_request = mock_send.call_args[0][0]
        body = prepared_request.body.decode("utf-8") if isinstance(prepared_request.body, bytes) else prepared_request.body
        payload = json.loads(body)

        self.assertEqual(payload["params"]["createdOrUpdatedSince"], "2025-01-01T00:00:00Z")
        self.assertEqual(STATE["referrals"], "2025-01-01T00:00:00Z")
