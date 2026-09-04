import os
import runpy
import types
import unittest
from unittest.mock import MagicMock, patch

from tap_referral_saasquatch import (
    CONFIG,
    STATE,
    Client,
    do_discover,
    main,
    main_impl,
    get_abs_path,
    export_ready,
    iter_lines,
    load_schema,
    request_export,
    stream_export,
    transform_field,
    transform_row,
)
from tap_referral_saasquatch.discover import discover
from tap_referral_saasquatch.exceptions import (
    ReferralSaasquatchAuthenticationError,
    ReferralSaasquatchError,
    ReferralSaasquatchForbiddenError,
)
from tap_referral_saasquatch.streams import Users


class _FakeResponse:
    def __init__(self, chunks):
        self._chunks = chunks

    def iter_content(self, chunk_size=None, decode_unicode=None):
        for chunk in self._chunks:
            yield chunk


class TestInitModuleCoverage(unittest.TestCase):
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

    def test_client_context_manager_closes_session(self):
        client = Client(CONFIG)
        client.session = MagicMock()
        with client as entered:
            self.assertIs(entered, client)
        client.session.close.assert_called_once_with()

    def test_probe_stream_access_forbidden_and_error(self):
        client = Client(CONFIG)
        forbidden_response = MagicMock()
        forbidden_response.status_code = 403
        forbidden_response.text = "insufficient permissions"
        client.session.send = MagicMock(return_value=forbidden_response)
        with self.assertRaisesRegex(ReferralSaasquatchForbiddenError, "403.*insufficient permissions"):
            client.probe_stream_access("users")

        error_response = MagicMock()
        error_response.status_code = 500
        error_response.text = "boom"
        client.session.send = MagicMock(return_value=error_response)
        with self.assertRaises(ReferralSaasquatchError):
            client.probe_stream_access("users")

    def test_probe_stream_access_raises_authentication_error_on_401(self):
        client = Client(CONFIG)
        response = MagicMock()
        response.status_code = 401
        response.text = "invalid API key"
        client.session.send = MagicMock(return_value=response)

        with self.assertRaisesRegex(ReferralSaasquatchAuthenticationError, "401.*invalid API key"):
            client.probe_stream_access("users")

    def test_probe_stream_access_success_with_user_agent(self):
        config = dict(CONFIG)
        config["user_agent"] = "ua-test"
        client = Client(config)
        success_response = MagicMock()
        success_response.status_code = 200
        success_response.json.return_value = {"id": "probe-users"}
        client.session.send = MagicMock(return_value=success_response)
        delete_response = MagicMock()
        delete_response.status_code = 204
        client.session.delete = MagicMock(return_value=delete_response)

        self.assertTrue(client.probe_stream_access("users"))
        client.session.delete.assert_called_once_with(
            "https://app.referralsaasquatch.com/api/v1/tenant-a/export/probe-users",
            auth=("", "dummy-key"),
            headers={"Content-Type": "application/json", "User-Agent": "ua-test"},
        )

    def test_probe_stream_access_raises_when_cleanup_fails(self):
        client = Client(CONFIG)
        success_response = MagicMock()
        success_response.status_code = 200
        success_response.json.return_value = {"id": "probe-users"}
        delete_response = MagicMock()
        delete_response.status_code = 500
        delete_response.text = "cleanup failed"
        client.session.send = MagicMock(return_value=success_response)
        client.session.delete = MagicMock(return_value=delete_response)

        with self.assertRaisesRegex(ReferralSaasquatchError, "delete access probe 'probe-users'.*500"):
            client.probe_stream_access("users")

    @patch("tap_referral_saasquatch.utils.load_json", return_value={"type": "object"})
    def test_get_abs_path_and_load_schema(self, mock_load_json):
        result = get_abs_path("schemas/users.json")
        self.assertIn("schemas", result)
        self.assertTrue(result.endswith("users.json"))

        loaded = load_schema("users")
        self.assertEqual(loaded, {"type": "object"})
        mock_load_json.assert_called_once()

    @patch("tap_referral_saasquatch.requests.get")
    def test_stream_export_parses_csv_rows(self, mock_get):
        CONFIG["user_agent"] = "ua-test"
        mock_get.return_value = MagicMock()

        with patch(
            "tap_referral_saasquatch.iter_lines",
            return_value=iter([b"id,name", b"1,Alice", b"2,Bob"]),
        ):
            rows = stream_export("users", "exp-123")

        self.assertEqual(rows, [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}])

    @patch("tap_referral_saasquatch.requests.get")
    def test_export_ready_includes_user_agent(self, mock_get):
        CONFIG["user_agent"] = "ua-test"
        response = MagicMock()
        response.json.return_value = {"status": "COMPLETED"}
        mock_get.return_value = response

        self.assertTrue(export_ready("exp-ua"))
        self.assertEqual(mock_get.call_args.kwargs["headers"]["User-Agent"], "ua-test")

    def test_iter_lines_default_and_delimiter_paths(self):
        response = _FakeResponse([b"alpha\n", b"", b"beta"])
        self.assertEqual(list(iter_lines(response)), [b"alpha", b"beta"])

        response_delim = _FakeResponse([b"a|b|", b"c"])
        self.assertEqual(list(iter_lines(response_delim, delimiter=b"|")), [b"a", b"b", b"c"])

    def test_iter_lines_handles_crlf_across_chunks(self):
        response = _FakeResponse([b"x\r", b"\n", b"y\r\n"])
        self.assertEqual(list(iter_lines(response)), [b"x", b"y"])

    @patch("tap_referral_saasquatch.export_ready", return_value=False)
    @patch("tap_referral_saasquatch.time.sleep")
    @patch("tap_referral_saasquatch.session.send")
    def test_request_export_timeout_and_missing_id(self, mock_send, mock_sleep, _mock_ready):
        response_with_id = MagicMock()
        response_with_id.status_code = 200
        response_with_id.json.return_value = {"id": "exp-timeout"}
        mock_send.return_value = response_with_id

        with self.assertRaises(Exception) as timeout_err:
            request_export("users")
        self.assertIn("took over an hour", str(timeout_err.exception))

        response_without_id = MagicMock()
        response_without_id.status_code = 200
        response_without_id.content = b"bad"
        response_without_id.json.return_value = {"status": "created"}
        mock_send.return_value = response_without_id

        with self.assertRaises(Exception) as missing_id_err:
            request_export("users")
        self.assertIn("Request to create users export failed", str(missing_id_err.exception))
        self.assertTrue(mock_sleep.called)

    @patch("tap_referral_saasquatch.export_ready", return_value=True)
    @patch("tap_referral_saasquatch.session.send")
    def test_request_export_includes_user_agent(self, mock_send, _mock_ready):
        CONFIG["user_agent"] = "ua-test"
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"id": "exp-ua"}
        mock_send.return_value = response

        self.assertEqual(request_export("users"), "exp-ua")

    def test_transform_helpers(self):
        converted = transform_field("users", "dateCreated", "1735689600000")
        self.assertEqual(converted, "2025-01-01T00:00:00.000000Z")

        untouched = transform_field("users", "name", "Alice")
        self.assertEqual(untouched, "Alice")

        row = transform_row("users", {"dateCreated": "1735689600000", "name": "Alice"})
        self.assertEqual(row["name"], "Alice")
        self.assertEqual(row["dateCreated"], "2025-01-01T00:00:00.000000Z")

    @patch("tap_referral_saasquatch.discover")
    @patch("tap_referral_saasquatch.json.dump")
    def test_do_discover_dumps_catalog(self, mock_dump, mock_discover):
        fake_catalog = MagicMock()
        fake_catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = fake_catalog

        do_discover(client=MagicMock())

        mock_discover.assert_called_once()
        mock_dump.assert_called_once()

    @patch("tap_referral_saasquatch.discover.Schema.from_dict", side_effect=ValueError("bad schema"))
    @patch("tap_referral_saasquatch.discover.get_schemas")
    def test_discover_logs_and_raises_bad_schema(self, mock_get_schemas, _mock_schema_from_dict):
        schemas = {"users": {"type": "object", "properties": {"id": {"type": "string"}}}}
        field_metadata = {"users": []}
        mock_get_schemas.return_value = (schemas, field_metadata)

        with self.assertRaises(ValueError):
            discover(MagicMock())

    def test_check_access_returns_client_probe_result(self):
        client = MagicMock()
        client.probe_stream_access.return_value = True

        self.assertTrue(Users(client=client).check_access())
        client.probe_stream_access.assert_called_once_with("users")

    @patch("singer.utils.parse_args")
    def test_main_guard_executes_without_crash(self, mock_parse_args):
        mock_parse_args.return_value = types.SimpleNamespace(
            config={"api_key": "x", "tenant_alias": "y", "start_date": "2025-01-01T00:00:00Z"},
            state=None,
            discover=False,
            catalog=None,
        )

        runpy.run_path(get_abs_path("__init__.py"), run_name="__main__")

    @patch("tap_referral_saasquatch.do_discover")
    @patch("tap_referral_saasquatch.utils.parse_args")
    def test_main_impl_discover_branch_and_state_update(self, mock_parse_args, mock_do_discover):
        STATE.clear()
        mock_parse_args.return_value = types.SimpleNamespace(
            config={"api_key": "x", "tenant_alias": "y", "start_date": "2025-01-01T00:00:00Z"},
            state={"users": "2025-01-01T00:00:00Z"},
            discover=True,
            catalog=None,
        )

        main_impl()

        self.assertIn("users", STATE)
        mock_do_discover.assert_called_once()

    @patch("tap_referral_saasquatch.do_sync")
    @patch("tap_referral_saasquatch.utils.parse_args")
    def test_main_impl_catalog_branch(self, mock_parse_args, mock_do_sync):
        catalog = MagicMock()
        mock_parse_args.return_value = types.SimpleNamespace(
            config={"api_key": "x", "tenant_alias": "y", "start_date": "2025-01-01T00:00:00Z"},
            state=None,
            discover=False,
            catalog=catalog,
        )

        main_impl()

        mock_do_sync.assert_called_once_with(catalog=catalog)

    @patch("tap_referral_saasquatch.logger")
    @patch("tap_referral_saasquatch.main_impl", side_effect=RuntimeError("boom"))
    def test_main_logs_critical_and_reraises(self, _mock_main_impl, mock_logger):
        with self.assertRaises(RuntimeError):
            main()

        mock_logger.critical.assert_called_once()
