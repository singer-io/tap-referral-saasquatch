import pytest
from unittest.mock import patch, MagicMock
from tap_referral_saasquatch import do_sync, CONFIG, STATE

CONFIG['start_date'] = '2025-01-01T00:00:00Z'
STATE.clear()

@pytest.fixture
def mock_catalog():
    mock_stream_obj = MagicMock()
    mock_stream_obj.schema.to_dict.return_value = {
        "type": "object", "properties": {"id": {"type": "string"}}
    }

    mock = MagicMock()
    mock.get_selected_streams.return_value = [MagicMock(stream='users')]
    mock.get_stream.return_value = mock_stream_obj
    mock.metadata = [{"breadcrumb": [], "metadata": {}}]

    return mock

@patch("tap_referral_saasquatch.singer.write_state")
@patch("tap_referral_saasquatch.utils.update_state")
@patch("tap_referral_saasquatch.request_export")
@patch("tap_referral_saasquatch.stream_export")
@patch("tap_referral_saasquatch.write_record")
@patch("tap_referral_saasquatch.metadata.to_map")
@patch("tap_referral_saasquatch.singer.Transformer")
def test_do_sync(mock_transformer_cls, mock_metadata_map, mock_write_record,
                 mock_stream_export, mock_request_export, mock_update_state,
                 mock_write_state, mock_catalog):

    # Mock export id and rows with a replication key value
    mock_request_export.return_value = "fake_export_id"
    mock_stream_export.return_value = [
        {"id": "1", "name": "Alice", "dateCreated": "2025-07-01T00:00:00Z"},
        {"id": "2", "name": "Bob", "dateCreated": "2025-07-02T00:00:00Z"},       # newest
        {"id": "3", "name": "Charlie", "dateCreated": "2025-06-30T00:00:00Z"},  # older
    ]
    mock_metadata_map.return_value = {}

    mock_transformer = MagicMock()
    mock_transformer.transform.side_effect = lambda r, s, m: r
    mock_transformer_cls.return_value.__enter__.return_value = mock_transformer

    do_sync(mock_catalog)

    # Verify export and stream were called correctly
    mock_request_export.assert_called_once_with("users")
    mock_stream_export.assert_called_once_with("users", "fake_export_id")
    assert mock_write_record.call_count == 3

    mock_write_record.assert_any_call("users", {"id": "1", "name": "Alice", "dateCreated": "2025-07-01T00:00:00Z"})
    mock_write_record.assert_any_call("users", {"id": "2", "name": "Bob", "dateCreated": "2025-07-02T00:00:00Z"})
    mock_write_record.assert_any_call("users", {"id": "3", "name": "Charlie", "dateCreated": "2025-06-30T00:00:00Z"})

    # ✅ Assert state was updated with latest replication key
    mock_update_state.assert_called_once()
    args, _ = mock_update_state.call_args
    assert args[0] is STATE
    assert args[1] == "users"
    assert STATE['users'] == "2025-01-01T00:00:00Z"  # updated entity state date

    # ✅ Assert write_state was also called to persist final state
    mock_write_state.assert_called_once()
