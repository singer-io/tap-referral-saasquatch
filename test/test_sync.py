import pytest
from unittest.mock import patch, MagicMock
from tap_referral_saasquatch import do_sync

from tap_referral_saasquatch import CONFIG, STATE

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

@patch("tap_referral_saasquatch.request_export")
@patch("tap_referral_saasquatch.stream_export")
@patch("tap_referral_saasquatch.write_record")
@patch("tap_referral_saasquatch.metadata.to_map")
@patch("tap_referral_saasquatch.singer.Transformer")
def test_do_sync(mock_transformer_cls, mock_metadata_map, mock_write_record,
                 mock_stream_export, mock_request_export, mock_catalog):

    mock_request_export.return_value = "fake_export_id"
    mock_stream_export.return_value = [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"}
    ]
    mock_metadata_map.return_value = {}

    mock_transformer = MagicMock()
    mock_transformer.transform.side_effect = lambda r, s, m: r
    mock_transformer_cls.return_value.__enter__.return_value = mock_transformer

    do_sync(mock_catalog)

    mock_request_export.assert_called_once_with("users")
    mock_stream_export.assert_called_once_with("users", "fake_export_id")
    assert mock_write_record.call_count == 2
    mock_write_record.assert_any_call("users", {"id": "1", "name": "Alice"})
    mock_write_record.assert_any_call("users", {"id": "2", "name": "Bob"})
