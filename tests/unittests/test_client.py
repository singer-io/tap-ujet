import unittest
from unittest.mock import patch, MagicMock, call
from requests.exceptions import HTTPError

from tap_ujet.client import (
    UjetClient,
    raise_for_error,
    get_exception_for_error_code,
    Server5xxError,
    Server429Error,
    UjetError,
    UjetBadRequestError,
    UjetUnauthorizedError,
    UjetRequestFailedError,
    UjetNotFoundError,
    UjetForbiddenError,
    UjetInternalServiceError,
    UjetUnprocessableEntityError,
    UjetMethodNotAllowedError,
    UjetConflictError,
)


def _build_response(status_code, content=b'', json_data=None):
    """Return a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = content.decode('utf-8') if isinstance(content, bytes) else str(content)
    resp.reason = 'Test Reason'
    resp.content = content
    resp.raise_for_status.side_effect = HTTPError(response=resp)
    if json_data is not None:
        resp.json.return_value = json_data
    else:
        resp.json.side_effect = ValueError('No JSON')
    return resp


def _make_client():
    """Construct UjetClient with a patched Session (no network calls)."""
    with patch('tap_ujet.client.requests.Session') as mock_session_cls:
        mock_session = mock_session_cls.return_value
        client = UjetClient('key', 'secret', 'mycompany', 'ujet', 'test-agent/1.0')
    # Bypass check_access and inject the mock session
    client._UjetClient__verified = True
    client._UjetClient__session = mock_session
    return client, mock_session


# ---------------------------------------------------------------------------
# get_exception_for_error_code
# ---------------------------------------------------------------------------

class TestGetExceptionForErrorCode(unittest.TestCase):
    """Maps HTTP status codes to the correct exception class."""

    def test_400_maps_to_bad_request(self):
        self.assertIs(get_exception_for_error_code(400), UjetBadRequestError)

    def test_401_maps_to_unauthorized(self):
        self.assertIs(get_exception_for_error_code(401), UjetUnauthorizedError)

    def test_402_maps_to_request_failed(self):
        self.assertIs(get_exception_for_error_code(402), UjetRequestFailedError)

    def test_403_maps_to_forbidden(self):
        self.assertIs(get_exception_for_error_code(403), UjetForbiddenError)

    def test_404_maps_to_not_found(self):
        self.assertIs(get_exception_for_error_code(404), UjetNotFoundError)

    def test_409_maps_to_conflict(self):
        self.assertIs(get_exception_for_error_code(409), UjetConflictError)

    def test_422_maps_to_unprocessable_entity(self):
        self.assertIs(get_exception_for_error_code(422), UjetUnprocessableEntityError)

    def test_500_maps_to_internal_service_error(self):
        self.assertIs(get_exception_for_error_code(500), UjetInternalServiceError)

    def test_unknown_code_maps_to_ujet_error(self):
        self.assertIs(get_exception_for_error_code(999), UjetError)


# ---------------------------------------------------------------------------
# raise_for_error
# ---------------------------------------------------------------------------

class TestRaiseForError(unittest.TestCase):
    """raise_for_error parses error responses and raises typed exceptions."""

    def test_empty_content_returns_none(self):
        """Empty response body should silently return None."""
        resp = _build_response(400, b'')
        result = raise_for_error(resp)
        self.assertIsNone(result)

    def test_400_with_error_key_raises_bad_request(self):
        """JSON body with 'error' key and status 400 → UjetBadRequestError."""
        resp = _build_response(400, b'...', {
            'error': 'Bad Request', 'message': 'Invalid param', 'status': 400
        })
        with self.assertRaises(UjetBadRequestError):
            raise_for_error(resp)

    def test_404_response_raises_not_found(self):
        """JSON body with status 404 → UjetNotFoundError."""
        resp = _build_response(404, b'...', {
            'error': 'Not Found', 'message': 'Resource missing', 'status': 404
        })
        with self.assertRaises(UjetNotFoundError):
            raise_for_error(resp)

    def test_unknown_status_in_body_raises_ujet_error(self):
        """JSON body with unmapped status code → UjetError."""
        resp = _build_response(418, b'...', {
            'error': 'Teapot', 'message': 'Short and stout', 'status': 418
        })
        with self.assertRaises(UjetError):
            raise_for_error(resp)

    def test_non_json_response_raises_ujet_error(self):
        """Non-JSON response body → UjetError."""
        resp = _build_response(500, b'Internal Server Error')
        resp.json.side_effect = ValueError('No JSON')
        with self.assertRaises(UjetError):
            raise_for_error(resp)

    def test_json_body_without_error_key_raises_ujet_error(self):
        """JSON body with no 'error' or 'errorCode' key → UjetError."""
        # Pass json_data directly so no side_effect is set on resp.json
        resp = _build_response(400, b'...', {'unexpected': 'structure'})
        with self.assertRaises(UjetError):
            raise_for_error(resp)

    def test_error_message_included_in_exception(self):
        """Exception message should contain the 'message' field from response."""
        resp = _build_response(400, b'...', {
            'error': 'Bad Request', 'message': 'field X is invalid', 'status': 400
        })
        with self.assertRaises(UjetBadRequestError) as ctx:
            raise_for_error(resp)
        self.assertIn('field X is invalid', str(ctx.exception))


# ---------------------------------------------------------------------------
# UjetClient.__init__
# ---------------------------------------------------------------------------

class TestUjetClientInit(unittest.TestCase):
    """Client constructor sets base_url and handles optional domain."""

    def test_base_url_constructed_correctly(self):
        """base_url uses subdomain, domain, and API version."""
        with patch('tap_ujet.client.requests.Session'):
            client = UjetClient('k', 's', 'myco', 'ujet', 'agent/1.0')
        self.assertEqual(client.base_url, 'https://myco.ujet.co/manager/api/v1')

    def test_default_domain_is_ujet(self):
        """Omitting domain defaults to 'ujet'."""
        with patch('tap_ujet.client.requests.Session'):
            client = UjetClient('k', 's', 'myco')
        self.assertIn('.ujet.co', client.base_url)

    def test_custom_domain_used_in_url(self):
        """Custom domain is reflected in base_url."""
        with patch('tap_ujet.client.requests.Session'):
            client = UjetClient('k', 's', 'myco', 'customdomain', 'agent')
        self.assertIn('customdomain', client.base_url)


# ---------------------------------------------------------------------------
# UjetClient.check_access
# ---------------------------------------------------------------------------

class TestUjetClientCheckAccess(unittest.TestCase):
    """check_access verifies credentials against the settings endpoint."""

    @patch('time.sleep')
    def test_returns_true_on_200(self, _mock_sleep):
        """200 response from settings endpoint → True."""
        with patch('tap_ujet.client.requests.Session') as mock_session_cls:
            mock_session = mock_session_cls.return_value
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_session.get.return_value = mock_resp
            client = UjetClient('k', 's', 'myco', 'ujet', 'agent')
            result = client.check_access()
        self.assertTrue(result)

    @patch('time.sleep')
    def test_returns_false_on_non_200(self, _mock_sleep):
        """Non-200 response from settings endpoint → False."""
        with patch('tap_ujet.client.requests.Session') as mock_session_cls:
            mock_session = mock_session_cls.return_value
            mock_resp = MagicMock()
            mock_resp.status_code = 401
            mock_session.get.return_value = mock_resp
            client = UjetClient('k', 's', 'myco', 'ujet', 'agent')
            result = client.check_access()
        self.assertFalse(result)

    @patch('time.sleep')
    def test_raises_when_company_key_is_none(self, _mock_sleep):
        """None company_key raises an Exception before making any request."""
        with patch('tap_ujet.client.requests.Session'):
            client = UjetClient(None, 's', 'myco', 'ujet', 'agent')
        with self.assertRaises(Exception):
            client.check_access()

    @patch('time.sleep')
    def test_raises_when_company_secret_is_none(self, _mock_sleep):
        """None company_secret raises an Exception before making any request."""
        with patch('tap_ujet.client.requests.Session'):
            client = UjetClient('k', None, 'myco', 'ujet', 'agent')
        with self.assertRaises(Exception):
            client.check_access()

    @patch('time.sleep')
    def test_raises_when_subdomain_is_none(self, _mock_sleep):
        """None subdomain raises an Exception before making any request."""
        with patch('tap_ujet.client.requests.Session'):
            client = UjetClient('k', 's', None, 'ujet', 'agent')
        with self.assertRaises(Exception):
            client.check_access()

    @patch('time.sleep')
    def test_sends_user_agent_header(self, _mock_sleep):
        """check_access sends User-Agent header when user_agent is configured."""
        with patch('tap_ujet.client.requests.Session') as mock_session_cls:
            mock_session = mock_session_cls.return_value
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_session.get.return_value = mock_resp
            client = UjetClient('k', 's', 'myco', 'ujet', 'my-agent/2.0')
            client.check_access()
        call_kwargs = mock_session.get.call_args[1]
        self.assertEqual(call_kwargs['headers']['User-Agent'], 'my-agent/2.0')


# ---------------------------------------------------------------------------
# UjetClient.request
# ---------------------------------------------------------------------------

class TestUjetClientRequest(unittest.TestCase):
    """request() handles auth, headers, pagination, and error responses."""

    @patch('time.sleep')
    def test_raises_server5xx_on_500(self, _mock_sleep):
        """Response status >= 500 raises Server5xxError (after all retries)."""
        client, mock_session = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.headers = {}
        mock_session.request.return_value = mock_resp
        with self.assertRaises(Server5xxError):
            client.request('GET', path='agents')

    @patch('time.sleep')
    def test_returns_json_total_and_next_url_on_200(self, _mock_sleep):
        """Successful 200 returns (json_body, total_records, next_url)."""
        client, mock_session = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {'total': '42'}
        mock_resp.json.return_value = [{'id': 1}]
        mock_session.request.return_value = mock_resp
        result, total, next_url = client.request('GET', path='agents')
        self.assertEqual(result, [{'id': 1}])
        self.assertEqual(total, 42)
        self.assertIsNone(next_url)

    @patch('time.sleep')
    def test_total_defaults_to_zero_when_header_absent(self, _mock_sleep):
        """Missing 'total' header defaults to 0."""
        client, mock_session = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {}
        mock_resp.json.return_value = []
        mock_session.request.return_value = mock_resp
        _, total, _ = client.request('GET', path='agents')
        self.assertEqual(total, 0)

    @patch('time.sleep')
    def test_parses_next_url_from_link_header(self, _mock_sleep):
        """'next' rel link in Link header is parsed as next_url."""
        client, mock_session = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {
            'total': '200',
            'link': '<https://myco.ujet.co/manager/api/v1/agents?page=2>; rel="next"',
        }
        mock_resp.json.return_value = []
        mock_session.request.return_value = mock_resp
        _, _, next_url = client.request('GET', path='agents')
        self.assertEqual(next_url, 'https://myco.ujet.co/manager/api/v1/agents?page=2')

    @patch('time.sleep')
    def test_next_url_none_when_no_next_rel(self, _mock_sleep):
        """Link header without 'next' rel → next_url is None."""
        client, mock_session = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {
            'total': '50',
            'link': '<https://myco.ujet.co/manager/api/v1/agents?page=1>; rel="prev"',
        }
        mock_resp.json.return_value = []
        mock_session.request.return_value = mock_resp
        _, _, next_url = client.request('GET', path='agents')
        self.assertIsNone(next_url)

    @patch('time.sleep')
    def test_user_agent_header_sent(self, _mock_sleep):
        """User-Agent header is included in every request."""
        client, mock_session = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {'total': '0'}
        mock_resp.json.return_value = []
        mock_session.request.return_value = mock_resp
        client.request('GET', path='agents')
        call_kwargs = mock_session.request.call_args[1]
        self.assertEqual(call_kwargs['headers']['User-Agent'], 'test-agent/1.0')

    @patch('time.sleep')
    def test_post_sets_content_type_header(self, _mock_sleep):
        """POST requests include Content-Type: application/json."""
        client, mock_session = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {'total': '0'}
        mock_resp.json.return_value = []
        mock_session.request.return_value = mock_resp
        client.request('POST', path='agents')
        call_kwargs = mock_session.request.call_args[1]
        self.assertEqual(call_kwargs['headers']['Content-Type'], 'application/json')

    @patch('time.sleep')
    def test_accept_header_is_vnd_json(self, _mock_sleep):
        """Accept header is set to application/vnd.json."""
        client, mock_session = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {'total': '0'}
        mock_resp.json.return_value = []
        mock_session.request.return_value = mock_resp
        client.request('GET', path='agents')
        call_kwargs = mock_session.request.call_args[1]
        self.assertEqual(call_kwargs['headers']['Accept'], 'application/vnd.json')

    @patch('time.sleep')
    def test_4xx_calls_raise_for_error(self, _mock_sleep):
        """4xx response triggers raise_for_error which raises a typed exception."""
        client, mock_session = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.headers = {}
        mock_resp.content = b'...'
        mock_resp.json.return_value = {
            'error': 'Not Found', 'message': 'not there', 'status': 404
        }
        mock_resp.raise_for_status.side_effect = HTTPError(response=mock_resp)
        mock_session.request.return_value = mock_resp
        with self.assertRaises(UjetNotFoundError):
            client.request('GET', path='nonexistent')

    @patch('time.sleep')
    def test_get_delegates_to_request(self, _mock_sleep):
        """client.get() calls request() with method=GET."""
        client, mock_session = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {'total': '0'}
        mock_resp.json.return_value = []
        mock_session.request.return_value = mock_resp
        result, total, _ = client.get(path='agents')
        self.assertEqual(mock_session.request.call_args[1]['method'], 'GET')

    @patch('time.sleep')
    def test_post_delegates_to_request(self, _mock_sleep):
        """client.post() calls request() with method=POST."""
        client, mock_session = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {'total': '0'}
        mock_resp.json.return_value = []
        mock_session.request.return_value = mock_resp
        client.post(path='agents')
        self.assertEqual(mock_session.request.call_args[1]['method'], 'POST')

    @patch('time.sleep')
    def test_endpoint_kwarg_extracted_from_kwargs(self, _mock_sleep):
        """'endpoint' kwarg is extracted before being forwarded to the session."""
        client, mock_session = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {'total': '0'}
        mock_resp.json.return_value = []
        mock_session.request.return_value = mock_resp
        # Pass 'endpoint' explicitly — must be stripped before session.request call
        client.request('GET', path='agents', endpoint='agents_endpoint')
        call_kwargs = mock_session.request.call_args[1]
        self.assertNotIn('endpoint', call_kwargs)

    @patch('time.sleep')
    def test_malformed_link_header_sets_next_url_none(self, _mock_sleep):
        """Malformed link entries (no regex match) are silently skipped."""
        client, mock_session = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {
            'total': '10',
            # Malformed — no quotes around rel value, regex won't match
            'link': '<https://example.com/page2>; rel=next',
        }
        mock_resp.json.return_value = []
        mock_session.request.return_value = mock_resp
        _, _, next_url = client.request('GET', path='agents')
        self.assertIsNone(next_url)

    @patch('time.sleep')
    def test_request_calls_check_access_when_not_verified(self, _mock_sleep):
        """request() calls check_access() to re-authenticate when not verified."""
        with patch('tap_ujet.client.requests.Session') as mock_session_cls:
            mock_session = mock_session_cls.return_value
            client = UjetClient('k', 's', 'myco', 'ujet', 'agent')
        # Leave __verified as False (default)
        client._UjetClient__verified = False
        client._UjetClient__session = mock_session

        # Mock check_access to return True without hitting the network
        mock_auth_resp = MagicMock()
        mock_auth_resp.status_code = 200
        mock_session.get.return_value = mock_auth_resp

        mock_req_resp = MagicMock()
        mock_req_resp.status_code = 200
        mock_req_resp.headers = {'total': '0'}
        mock_req_resp.json.return_value = []
        mock_session.request.return_value = mock_req_resp

        _, _, _ = client.request('GET', path='agents')
        # check_access should have been called (session.get called for auth)
        mock_session.get.assert_called_once()


# ---------------------------------------------------------------------------
# Context manager (__enter__ / __exit__)
# ---------------------------------------------------------------------------

class TestUjetClientContextManager(unittest.TestCase):
    """UjetClient supports the 'with' statement."""

    @patch('time.sleep')
    def test_enter_calls_check_access(self, _mock_sleep):
        """__enter__ calls check_access and returns self."""
        with patch('tap_ujet.client.requests.Session') as mock_session_cls:
            mock_session = mock_session_cls.return_value
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_session.get.return_value = mock_resp
            client = UjetClient('k', 's', 'myco', 'ujet', 'agent')
            result = client.__enter__()
        self.assertIs(result, client)

    @patch('time.sleep')
    def test_exit_closes_session(self, _mock_sleep):
        """__exit__ closes the underlying requests Session."""
        with patch('tap_ujet.client.requests.Session') as mock_session_cls:
            mock_session = mock_session_cls.return_value
            client = UjetClient('k', 's', 'myco', 'ujet', 'agent')
            client.__exit__(None, None, None)
        mock_session.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
