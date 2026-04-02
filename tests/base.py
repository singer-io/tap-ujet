"""Base test mixin for tap-ujet integration tests (mock mode).

Not a TestCase itself — mix with unittest.TestCase in each test class.
All HTTP calls are patched at the tap level; no live API credentials required.
"""
import json
import os


# ---------------------------------------------------------------------------
# Minimal requests.Response stand-in
# ---------------------------------------------------------------------------

class MockResponse:
    """Minimal requests.Response stand-in used across all mock test files."""

    def __init__(self, payload, status_code=200, headers=None):
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {"total": str(len(payload) if isinstance(payload, list) else 0)}
        self.content = json.dumps(payload).encode()
        self.text = json.dumps(payload)
        self.reason = "OK"

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


# ---------------------------------------------------------------------------
# Base mixin
# ---------------------------------------------------------------------------

class UjetBaseTest:
    """Base test case for tap-ujet integration tests with mocked data.

    Not a TestCase itself — mix with unittest.TestCase in each test class.
    """

    # ── Metadata constants ───────────────────────────────────────────────
    PRIMARY_KEYS = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS = "replication_keys"
    OBEYS_START_DATE = "obeys_start_date"
    API_LIMIT = "api_limit"

    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"

    default_start_date = "2020-01-01T00:00:00Z"

    # ── Stream metadata ──────────────────────────────────────────────────

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "agents": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"status_updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
            "agent_activity_logs": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"started_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
            "calls": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
            "chats": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
            "menu_tree": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "menus": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "teams": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "team_tree": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
            "user_statuses": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
            },
        }

    @classmethod
    def expected_stream_names(cls):
        """Return the set of all expected stream names."""
        return set(cls.expected_metadata().keys())

    @classmethod
    def incremental_streams(cls):
        """Return all streams that use INCREMENTAL replication."""
        return {
            s for s, m in cls.expected_metadata().items()
            if m[cls.REPLICATION_METHOD] == cls.INCREMENTAL
        }

    @classmethod
    def full_table_streams(cls):
        """Return all streams that use FULL_TABLE replication."""
        return {
            s for s, m in cls.expected_metadata().items()
            if m[cls.REPLICATION_METHOD] == cls.FULL_TABLE
        }

    # ── Test setup / teardown ────────────────────────────────────────────

    def setUp(self):
        """Set up test fixtures with dummy config and empty state."""
        self.config = self.get_mock_config()
        self.state = {}

    def tearDown(self):
        """Clean up after tests."""
        pass

    # ── Config helpers ───────────────────────────────────────────────────

    @staticmethod
    def get_mock_config():
        """Return mock configuration with dummy values — no real credentials."""
        return {
            "company_key": "mock_company_key",
            "company_secret": "mock_company_secret",
            "subdomain": "mock_subdomain",
            "domain": "ujet",
            "start_date": "2020-01-01T00:00:00Z",
            "user_agent": "tap-ujet-test/1.0",
        }

    @staticmethod
    def get_mock_state():
        """Return initial mock state."""
        return {}

    # ── Schema-driven mock data generation ──────────────────────────────

    @staticmethod
    def _schema_path(stream_name):
        """Resolve the path of a stream's JSON schema file."""
        base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        return os.path.join(base_dir, "tap_ujet", "schemas",
                            f"{stream_name}.json")

    @classmethod
    def _load_schema(cls, stream_name):
        """Load and return the JSON schema dict for a stream."""
        with open(cls._schema_path(stream_name), "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _schema_type(schema):
        """Return the concrete type, resolving null unions."""
        t = schema.get("type", "object")
        if isinstance(t, list):
            non_null = [x for x in t if x != "null"]
            return non_null[0] if non_null else "null"
        return t

    @staticmethod
    def _generate_value(schema, date_value="2024-01-01T00:00:00Z"):
        """Recursively generate one valid mock value for a JSON Schema fragment."""
        if "enum" in schema and schema["enum"]:
            return schema["enum"][0]

        # Handle anyOf/oneOf — pick the first non-null branch
        for key in ("anyOf", "oneOf"):
            if key in schema:
                for sub in schema[key]:
                    if isinstance(sub, dict):
                        t = sub.get("type", "object")
                        if t != "null" and t != ["null"]:
                            return UjetBaseTest._generate_value(sub, date_value)
                # All branches null — return None
                return None

        schema_type = UjetBaseTest._schema_type(schema)

        if schema_type == "object":
            properties = schema.get("properties", {})
            return {
                key: UjetBaseTest._generate_value(val, date_value)
                for key, val in properties.items()
            }
        if schema_type == "array":
            item_schema = schema.get("items", {"type": "string"})
            # items may be a dict or a list (tuple validation) — handle both
            if isinstance(item_schema, list):
                item_schema = item_schema[0] if item_schema else {"type": "string"}
            # Empty items schema — return empty list to satisfy anyOf array branch
            if not item_schema:
                return []
            return [UjetBaseTest._generate_value(item_schema, date_value)]
        if schema_type == "string":
            fmt = schema.get("format")
            if fmt == "date-time":
                return date_value
            if fmt == "email":
                return "mock@example.com"
            return "mock_value"
        if schema_type == "integer":
            return 1
        if schema_type == "number":
            return 1.0
        if schema_type == "boolean":
            return True
        # null or unknown
        return None

    @classmethod
    def _generate_stream_record(cls, stream_name, date_value="2024-01-01T00:00:00Z"):
        """Generate one schema-valid mock record for the given stream."""
        schema = cls._load_schema(stream_name)
        # Handle anyOf/oneOf at root level by using the first concrete object schema
        if "anyOf" in schema:
            for sub in schema["anyOf"]:
                if isinstance(sub, dict) and sub.get("type") == "object":
                    schema = sub
                    break
        record = cls._generate_value(schema, date_value=date_value)
        if not isinstance(record, dict):
            record = {}
        # Ensure id is always set as an integer (required by most schemas)
        record.setdefault("id", 1)
        return record

    # ── Client request mock factory ──────────────────────────────────────

    @classmethod
    def _make_client_get_response(cls, stream_name, records, total=None, next_url=None):
        """Return the 3-tuple (records, total, next_url) that UjetClient.get() returns."""
        return (records, total if total is not None else len(records), next_url)

    @classmethod
    def _make_catalog(cls):
        """Return a singer Catalog for all streams (used in sync tests)."""
        from singer import metadata
        from singer.catalog import Catalog, CatalogEntry, Schema
        from tap_ujet.discover import discover
        catalog = discover()
        # Mark all streams and fields as selected
        for entry in catalog.streams:
            mdata_map = metadata.to_map(entry.metadata)
            mdata_map[()]["selected"] = True
            for breadcrumb in mdata_map:
                if breadcrumb != ():
                    mdata_map[breadcrumb]["selected"] = True
            entry.metadata = metadata.to_list(mdata_map)
        return catalog
