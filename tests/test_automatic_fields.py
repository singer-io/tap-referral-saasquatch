import unittest
from unittest.mock import MagicMock

from tap_referral_saasquatch.discover import discover


class AutomaticFieldsIntegrationTest(unittest.TestCase):
    def test_primary_and_replication_keys_are_automatic(self):
        catalog = discover(MagicMock())

        for stream in catalog.streams:
            root = [m for m in stream.metadata if m.get("breadcrumb") in ((), [])][0]
            key_props = set(root.get("metadata", {}).get("table-key-properties", []))
            rep_keys = root.get("metadata", {}).get("valid-replication-keys", [])
            if isinstance(rep_keys, str):
                rep_keys = {rep_keys}
            else:
                rep_keys = set(rep_keys)

            expected_auto = key_props | rep_keys

            actual_auto = set()
            for entry in stream.metadata:
                breadcrumb = entry.get("breadcrumb", ())
                if len(breadcrumb) == 2 and breadcrumb[0] == "properties":
                    if entry.get("metadata", {}).get("inclusion") == "automatic":
                        actual_auto.add(breadcrumb[1])

            self.assertTrue(expected_auto.issubset(actual_auto))
