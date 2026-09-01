import unittest
import csv
import io
from unittest.mock import MagicMock, patch

from tap_referral_saasquatch import stream_export

try:
    from .base import ReferralBaseTest
except ImportError:
    from base import ReferralBaseTest


class PaginationIntegrationTest(ReferralBaseTest, unittest.TestCase):

    @staticmethod
    def _chunk_payload(payload, chunk_sizes):
        chunks = []
        index = 0
        for size in chunk_sizes:
            if index >= len(payload):
                break
            chunks.append(payload[index:index + size])
            index += size
        if index < len(payload):
            chunks.append(payload[index:])
        return chunks

    @staticmethod
    def _to_csv_payload(records, field_names):
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=field_names)
        writer.writeheader()
        for record in records:
            writer.writerow(record)
        return buffer.getvalue().encode("utf-8")

    def _build_dynamic_user_rows(self):
        date_values = [
            "2025-01-01T00:00:00Z",
            "2025-02-01T00:00:00Z",
            "2025-03-01T00:00:00Z",
        ]
        rows = []
        for idx, date_value in enumerate(date_values, start=1):
            record = self._generate_stream_record("users", date_value=date_value)
            rows.append(
                {
                    "id": f"user-{idx}",
                    "accountId": record.get("accountId", "mock"),
                    "firstName": f"name-{idx}",
                    "dateCreated": date_value,
                }
            )
        return rows

    def _build_dynamic_referral_rows(self):
        date_values = [
            "2025-01-10T00:00:00Z",
            "2025-02-10T00:00:00Z",
            "2025-03-10T00:00:00Z",
        ]
        rows = []
        for idx, date_value in enumerate(date_values, start=1):
            record = self._generate_stream_record("referrals", date_value=date_value)
            rows.append(
                {
                    "id": f"ref-{idx}",
                    "programId": record.get("programId", "mock"),
                    "dateReferralStarted": date_value,
                    "dateConverted": date_value,
                    "dateModerated": date_value,
                }
            )
        return rows

    @patch("tap_referral_saasquatch.requests.get")
    def test_stream_export_reads_all_rows_across_chunk_patterns(self, mock_get):
        expected_rows = self._build_dynamic_user_rows()
        field_names = ["id", "accountId", "firstName", "dateCreated"]
        payload = self._to_csv_payload(expected_rows, field_names)

        chunk_patterns = {
            "uneven_chunks": [9, 4, 15, 2, 11],
            "single_byte_chunks": [1] * 25,
            "line_like_chunks": [20, 30, 40],
            "contains_empty_chunk": [13, 0, 17, 5],
        }

        for pattern_name, sizes in chunk_patterns.items():
            with self.subTest(pattern=pattern_name):
                chunks = self._chunk_payload(payload, sizes)
                if pattern_name == "contains_empty_chunk" and len(chunks) > 1:
                    chunks.insert(1, b"")

                response = MagicMock()
                response.iter_content.return_value = chunks
                mock_get.return_value = response

                rows = stream_export("users", "exp-1")

                self.assertEqual(rows, expected_rows)

    @patch("tap_referral_saasquatch.requests.get")
    def test_stream_export_referrals_across_chunk_patterns(self, mock_get):
        expected_rows = self._build_dynamic_referral_rows()
        field_names = [
            "id",
            "programId",
            "dateReferralStarted",
            "dateConverted",
            "dateModerated",
        ]
        payload = self._to_csv_payload(expected_rows, field_names)

        chunk_patterns = {
            "tiny_chunks": [2, 3, 4, 5, 6],
            "mixed_chunks": [17, 1, 23, 8],
            "single_large_then_tail": [120],
        }

        for pattern_name, sizes in chunk_patterns.items():
            with self.subTest(pattern=pattern_name):
                response = MagicMock()
                response.iter_content.return_value = self._chunk_payload(payload, sizes)
                mock_get.return_value = response

                rows = stream_export("referrals", "exp-2")

                self.assertEqual(rows, expected_rows)
