import json
import unittest
from unittest.mock import MagicMock, patch

from tap_referral_saasquatch import CONFIG, STATE, request_export

try:
    from .base import ReferralBaseTest
except ImportError:
    from base import ReferralBaseTest


class StartDateIntegrationTest(ReferralBaseTest, unittest.TestCase):

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

    @patch("tap_referral_saasquatch.export_ready", return_value=True)
    @patch("tap_referral_saasquatch.session.send")
    def test_request_export_uses_updated_start_date_value(self, mock_send, _mock_export_ready):
        CONFIG["start_date"] = "2025-04-20T10:30:00Z"

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"id": "exp-users"}
        mock_send.return_value = response

        export_id = request_export("users")
        self.assertEqual(export_id, "exp-users")

        prepared_request = mock_send.call_args[0][0]
        body = prepared_request.body.decode("utf-8") if isinstance(prepared_request.body, bytes) else prepared_request.body
        payload = json.loads(body)

        self.assertEqual(payload["params"]["createdOrUpdatedSince"], "2025-04-20T10:30:00Z")
        self.assertEqual(STATE["users"], "2025-04-20T10:30:00Z")
