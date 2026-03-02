import unittest

from tap_referral_saasquatch import CONFIG, STATE, get_start, transform_timestamp


class TestIncrementalHelpers(unittest.TestCase):
    def setUp(self):
        self.original_state = dict(STATE)
        self.original_config = dict(CONFIG)
        STATE.clear()
        CONFIG["start_date"] = "2025-01-01T00:00:00Z"

    def tearDown(self):
        STATE.clear()
        STATE.update(self.original_state)
        CONFIG.clear()
        CONFIG.update(self.original_config)

    def test_get_start_initializes_missing_entity(self):
        result = get_start("users")
        self.assertEqual(result, "2025-01-01T00:00:00Z")
        self.assertEqual(STATE["users"], "2025-01-01T00:00:00Z")

    def test_get_start_returns_existing_state_value(self):
        STATE["users"] = "2025-06-01T00:00:00Z"
        result = get_start("users")
        self.assertEqual(result, "2025-06-01T00:00:00Z")

    def test_transform_timestamp_none_value(self):
        self.assertIsNone(transform_timestamp(None))

    def test_transform_timestamp_epoch_millis(self):
        result = transform_timestamp("1735689600000")
        self.assertEqual(result, "2025-01-01T00:00:00.000000Z")
