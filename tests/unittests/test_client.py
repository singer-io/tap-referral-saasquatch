import unittest
from unittest.mock import MagicMock, patch

from tap_referral_saasquatch import CONFIG, export_ready, request_export


class TestClient(unittest.TestCase):
    def setUp(self):
        self.original_config = dict(CONFIG)
        CONFIG.update(
            {
                "api_key": "dummy-key",
                "tenant_alias": "tenant-a",
                "start_date": "2025-01-01T00:00:00Z",
            }
        )

    def tearDown(self):
        CONFIG.clear()
        CONFIG.update(self.original_config)

    @patch("tap_referral_saasquatch.requests.get")
    def test_export_ready_completed(self, mock_get):
        response = MagicMock()
        response.json.return_value = {"status": "COMPLETED"}
        mock_get.return_value = response

        self.assertTrue(export_ready("exp-1"))

    @patch("tap_referral_saasquatch.requests.get")
    def test_export_ready_not_completed(self, mock_get):
        response = MagicMock()
        response.json.return_value = {"status": "PROCESSING"}
        mock_get.return_value = response

        self.assertFalse(export_ready("exp-2"))

    @patch("tap_referral_saasquatch.sys.exit", side_effect=SystemExit(1))
    @patch("tap_referral_saasquatch.session.send")
    def test_request_export_http_error_exits(self, mock_send, mock_exit):
        response = MagicMock()
        response.status_code = 500
        response.content = b"failure"
        mock_send.return_value = response

        with self.assertRaises(SystemExit):
            request_export("users")

        mock_exit.assert_called_once_with(1)

    @patch("tap_referral_saasquatch.time.sleep")
    @patch("tap_referral_saasquatch.export_ready", side_effect=[False, True])
    @patch("tap_referral_saasquatch.session.send")
    def test_request_export_waits_until_ready(self, mock_send, mock_export_ready, mock_sleep):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"id": "exp-42"}
        mock_send.return_value = response

        export_id = request_export("users")

        self.assertEqual(export_id, "exp-42")
        self.assertEqual(mock_export_ready.call_count, 2)
        mock_sleep.assert_called_once_with(5)
