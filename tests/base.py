from tap_referral_saasquatch import CONFIG, STATE
from tap_referral_saasquatch.schema import get_schemas


class ReferralBaseTest:
    default_start_date = "2025-01-01T00:00:00Z"
    PRIMARY_KEYS = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS = "replication_keys"
    OBEYS_START_DATE = "obeys_start_date"
    API_LIMIT = "api_limit"

    @classmethod
    def expected_metadata(cls):
        return {
            "users": {
                cls.PRIMARY_KEYS: {"id", "accountId"},
                cls.REPLICATION_METHOD: "INCREMENTAL",
                cls.REPLICATION_KEYS: {"dateCreated"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "reward_balances": {
                cls.PRIMARY_KEYS: {"userId", "accountId"},
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "referrals": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: "INCREMENTAL",
                cls.REPLICATION_KEYS: {"dateReferralStarted"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
        }

    def setUp(self):
        self.original_config = dict(CONFIG)
        self.original_state = dict(STATE)

        CONFIG.update(
            {
                "api_key": "dummy-key",
                "tenant_alias": "dummy-tenant",
                "start_date": self.default_start_date,
            }
        )
        STATE.clear()

    def tearDown(self):
        CONFIG.clear()
        CONFIG.update(self.original_config)
        STATE.clear()
        STATE.update(self.original_state)

    @staticmethod
    def _schema_type(schema):
        """Return concrete type when schema allows null union types."""
        schema_type = schema.get("type", "object")
        if isinstance(schema_type, list):
            non_null = [item for item in schema_type if item != "null"]
            return non_null[0] if non_null else "null"
        return schema_type

    @staticmethod
    def _generate_value(schema, date_value="2024-01-01T00:00:00Z"):
        """Generate one valid mock value for a JSON-schema fragment."""
        if "enum" in schema and schema["enum"]:
            return schema["enum"][0]

        schema_type = ReferralBaseTest._schema_type(schema)
        if schema_type == "object":
            properties = schema.get("properties", {})
            required = set(schema.get("required", []))
            return {
                key: ReferralBaseTest._generate_value(value, date_value=date_value)
                for key, value in properties.items()
                if key in required or ReferralBaseTest._schema_type(value) != "null"
            }
        if schema_type == "array":
            return [
                ReferralBaseTest._generate_value(
                    schema.get("items", {"type": "string"}),
                    date_value=date_value,
                )
            ]
        if schema_type == "string":
            fmt = schema.get("format")
            return (
                date_value
                if fmt == "date-time"
                else "mock@example.com"
                if fmt == "email"
                else "mock"
            )
        return {"integer": 1, "number": 1.0, "boolean": True}.get(schema_type)

    @staticmethod
    def _generate_stream_record(stream_name, date_value="2024-01-01T00:00:00Z"):
        """Generate one schema-valid record for a stream."""
        schemas, _ = get_schemas()
        return ReferralBaseTest._generate_value(schemas[stream_name], date_value=date_value)
