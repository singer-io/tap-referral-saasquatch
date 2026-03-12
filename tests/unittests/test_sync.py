import unittest
from unittest.mock import MagicMock, patch

from tap_referral_saasquatch import do_sync, sync_entity


class TestSync(unittest.TestCase):
    @patch("tap_referral_saasquatch.singer.write_state")
    @patch("tap_referral_saasquatch.utils.update_state")
    @patch("tap_referral_saasquatch.write_record")
    @patch("tap_referral_saasquatch.metadata.to_map", return_value={})
    @patch("tap_referral_saasquatch.stream_export")
    @patch("tap_referral_saasquatch.request_export")
    @patch("tap_referral_saasquatch.load_schema")
    @patch("tap_referral_saasquatch.singer.write_schema")
    @patch("tap_referral_saasquatch.get_start", return_value="2025-01-01T00:00:00Z")
    def test_sync_entity_writes_schema_records_and_state(
        self,
        mock_get_start,
        mock_write_schema,
        mock_load_schema,
        mock_request_export,
        mock_stream_export,
        mock_metadata_map,
        mock_write_record,
        mock_update_state,
        mock_write_state,
    ):
        mock_load_schema.return_value = {"type": "object", "properties": {"id": {"type": "string"}}}
        mock_request_export.return_value = "export-1"
        mock_stream_export.return_value = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]

        mock_catalog_stream = MagicMock()
        mock_catalog_stream.schema.to_dict.return_value = {
            "type": "object",
            "properties": {"id": {"type": "string"}, "name": {"type": "string"}},
        }

        catalog = MagicMock()
        catalog.metadata = []
        catalog.get_stream.return_value = mock_catalog_stream

        transformer = MagicMock()
        transformer.transform.side_effect = lambda row, schema, mdata: row

        sync_entity("users", ["id", "accountId"], catalog, transformer)

        mock_get_start.assert_called_once_with("users")
        mock_write_schema.assert_called_once_with(
            "users", {"type": "object", "properties": {"id": {"type": "string"}}}, ["id", "accountId"]
        )
        mock_request_export.assert_called_once_with("users")
        mock_stream_export.assert_called_once_with("users", "export-1")
        self.assertEqual(mock_write_record.call_count, 2)
        mock_update_state.assert_called_once()
        mock_write_state.assert_called_once()

    @patch("tap_referral_saasquatch.sync_entity")
    @patch("tap_referral_saasquatch.singer.Transformer")
    def test_do_sync_calls_selected_streams(self, mock_transformer_cls, mock_sync_entity):
        transformer = MagicMock()
        mock_transformer_cls.return_value.__enter__.return_value = transformer

        stream_users = MagicMock()
        stream_users.stream = "users"
        stream_referrals = MagicMock()
        stream_referrals.stream = "referrals"
        stream_reward_balances = MagicMock()
        stream_reward_balances.stream = "reward_balances"

        catalog = MagicMock()
        catalog.get_selected_streams.return_value = [
            stream_users,
            stream_referrals,
            stream_reward_balances,
        ]

        do_sync(catalog)

        self.assertEqual(mock_sync_entity.call_count, 3)
        mock_sync_entity.assert_any_call("users", ["id", "accountId"], catalog, transformer)
        mock_sync_entity.assert_any_call("referrals", ["id"], catalog, transformer)
        mock_sync_entity.assert_any_call("reward_balances", ["userId", "accountId"], catalog, transformer)
