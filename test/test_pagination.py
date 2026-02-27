import unittest
from unittest.mock import MagicMock, patch

from tap_referral_saasquatch import CONFIG, stream_export


class PaginationIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.original_config = dict(CONFIG)
        CONFIG.update(
            {
                "api_key": "dummy-key",
                "tenant_alias": "dummy-tenant",
                "start_date": "2025-01-01T00:00:00Z",
            }
        )

    def tearDown(self):
        CONFIG.clear()
        CONFIG.update(self.original_config)

    @patch("tap_referral_saasquatch.requests.get")
    def test_stream_export_reads_all_rows_across_chunks(self, mock_get):
        response = MagicMock()
        response.iter_content.return_value = [
            b"id,name\n1,Ali",
            b"ce\n2,Bob\n3,Cha",
            b"rlie\n",
        ]
        mock_get.return_value = response

        rows = stream_export("users", "exp-1")

        self.assertEqual(
            rows,
            [
                {"id": "1", "name": "Alice"},
                {"id": "2", "name": "Bob"},
                {"id": "3", "name": "Charlie"},
            ],
        )
