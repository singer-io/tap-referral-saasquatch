import unittest
from unittest.mock import MagicMock, patch

from singer import metadata

from tap_referral_saasquatch.discover import _apply_access_checks, discover
from tap_referral_saasquatch.exceptions import (
    ReferralSaasquatchAuthenticationError,
    ReferralSaasquatchError,
    ReferralSaasquatchForbiddenError,
)
from tap_referral_saasquatch.schema import get_schemas
from tap_referral_saasquatch.streams import STREAMS, Users


class TestDiscoveryAndSchema(unittest.TestCase):
    def test_get_schemas_returns_all_streams(self):
        schemas, field_metadata = get_schemas()

        self.assertEqual(set(schemas.keys()), set(STREAMS.keys()))
        self.assertEqual(set(field_metadata.keys()), set(STREAMS.keys()))

    def test_replication_key_is_automatic_in_metadata(self):
        _, field_metadata = get_schemas()
        users_metadata_map = metadata.to_map(field_metadata["users"])

        inclusion = users_metadata_map[("properties", "dateCreated")]["inclusion"]
        self.assertEqual(inclusion, "automatic")

    def test_discover_builds_catalog_entries(self):
        catalog = discover(MagicMock())
        stream_names = [stream.tap_stream_id for stream in catalog.streams]

        self.assertEqual(set(stream_names), set(STREAMS.keys()))

    def test_apply_access_checks_excludes_inaccessible_streams(self):
        schemas, field_metadata = get_schemas()
        client = MagicMock()
        client.probe_stream_access.side_effect = lambda stream_name: stream_name != "users"

        with self.assertLogs(level="WARNING") as logs:
            _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn("users", schemas)
        self.assertIn("referrals", schemas)
        self.assertIn("reward_balances", schemas)
        warning_text = "\n".join(logs.output)
        self.assertIn("users", warning_text)
        self.assertIn("403", warning_text)

    def test_apply_access_checks_raises_when_none_accessible(self):
        schemas, field_metadata = get_schemas()
        client = MagicMock()
        client.probe_stream_access.return_value = False

        with self.assertRaises(ReferralSaasquatchForbiddenError):
            _apply_access_checks(client, schemas, field_metadata)

    def test_apply_access_checks_fails_fast_on_invalid_credentials(self):
        schemas, field_metadata = get_schemas()
        client = MagicMock()
        client.probe_stream_access.side_effect = ReferralSaasquatchAuthenticationError(
            "HTTP-error-code: 401, Error: invalid API key"
        )

        with self.assertRaisesRegex(ReferralSaasquatchAuthenticationError, "401"):
            _apply_access_checks(client, schemas, field_metadata)

        client.probe_stream_access.assert_called_once_with("referrals")

    @patch("tap_referral_saasquatch.streams.LOGGER")
    def test_check_access_logs_and_returns_false_on_forbidden(self, mock_logger):
        client = MagicMock()
        client.probe_stream_access.side_effect = ReferralSaasquatchForbiddenError(
            "HTTP-error-code: 403, Error: forbidden"
        )
        stream = Users(client=client)

        self.assertFalse(stream.check_access())
        mock_logger.warning.assert_called_once()

    def test_check_access_preserves_non_authorization_error(self):
        client = MagicMock()
        client.probe_stream_access.side_effect = ReferralSaasquatchError(
            "HTTP-error-code: 429, Error: rate limited"
        )
        stream = Users(client=client)

        with self.assertRaisesRegex(ReferralSaasquatchError, "429"):
            stream.check_access()
