import re
import backoff
import requests
# from requests.exceptions import ConnectionError
from singer import metrics, utils
import singer

LOGGER = singer.get_logger()
API_VERSION = 'v1'


class Server5xxError(Exception):
    pass


class Server429Error(Exception):
    pass


class UjetError(Exception):
    pass


class UjetBadRequestError(UjetError):
    pass


class UjetUnauthorizedError(UjetError):
    pass


class UjetRequestFailedError(UjetError):
    pass


class UjetNotFoundError(UjetError):
    pass


class UjetMethodNotAllowedError(UjetError):
    pass


class UjetConflictError(UjetError):
    pass


class UjetForbiddenError(UjetError):
    pass


class UjetUnprocessableEntityError(UjetError):
    pass


class UjetInternalServiceError(UjetError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: UjetBadRequestError,
    401: UjetUnauthorizedError,
    402: UjetRequestFailedError,
    403: UjetForbiddenError,
    404: UjetNotFoundError,
    405: UjetMethodNotAllowedError,
    409: UjetConflictError,
    422: UjetUnprocessableEntityError,
    500: UjetInternalServiceError}


def get_exception_for_error_code(error_code):
    return ERROR_CODE_EXCEPTION_MAPPING.get(error_code, UjetError)

def raise_for_error(response):
    LOGGER.error('ERROR {}: {}, REASON: {}'.format(response.status_code,\
        response.text, response.reason))
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            content_length = len(response.content)
            if content_length == 0:
                # There is nothing we can do here since Ujet has neither sent
                # us a 2xx response nor a response content.
                return
            response = response.json()
            if ('error' in response) or ('errorCode' in response):
                message = '%s: %s' % (response.get('error', str(error)),
                                      response.get('message', 'Unknown Error'))
                error_code = response.get('status')
                ex = get_exception_for_error_code(error_code)
                raise ex(message)
            else:
                raise UjetError(error)
        except (ValueError, TypeError):
            raise UjetError(error)


class UjetClient(object):
    def __init__(self,
                 company_key,
                 company_secret,
                 subdomain,
                 domain=None,
                 user_agent=None):
        self.__company_key = company_key
        self.__company_secret = company_secret
        self.__subdomain = subdomain
        if not domain:
            self.__domain = 'ujet'
        else:
            self.__domain = domain
        self.base_url = "https://{}.{}.co/manager/api/{}".format(
            self.__subdomain,
            self.__domain,
            API_VERSION)
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.__verified = False

    def __enter__(self):
        self.__verified = self.check_access()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=5,
                          factor=2)
    @utils.ratelimit(1, 1.5)
    def check_access(self):
        if self.__company_key is None or self.__company_secret is None:
            raise Exception('Error: Missing company_key or company_secret in config.json.')
        if self.__subdomain is None:
            raise Exception('Error: Missing subdomain in cofig.json.')
        headers = {}
        endpoint = 'settings/organization'
        url = '{}/{}'.format(self.base_url, endpoint)
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['Accept'] = 'application/json'
        response = self.__session.get(
            url=url,
            headers=headers,
            # Basic Authentication: https://api.Ujet.com/?http#authentication
            auth=(self.__company_key, self.__company_secret))
        if response.status_code != 200:
            LOGGER.error('Error status_code = {}'.format(response.status_code))
            raise_for_error(response)
        else:
            return True


    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error),
                          max_tries=7,
                          factor=3)
    @utils.ratelimit(1, 1.5)
    def request(self, method, path=None, url=None, json=None, version=None, **kwargs):
        if not self.__verified:
            self.__verified = self.check_access()

        if not version:
            version = 'v2'

        if not url and path:
            url = '{}/{}'.format(self.base_url, path)

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        # Version represents API version (e.g. v2): https://api.Ujet.com/?http#versioning
        kwargs['headers']['Accept'] = 'application/vnd.json'.format(version)

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(
                method=method,
                url=url,
                auth=(self.__company_key, self.__company_secret),
                json=json,
                **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code != 200:
            raise_for_error(response)

        # pagination details are returned in the header: total, per-page, next url
        total_records = int(response.headers.get('total', 0))

        # Not returning currently due to client API bug
        per_page = total_records = int(response.headers.get('per-page', 0))
        next_url = None
        if ((response.headers.get('link') is not None) and ('link' in response.headers)):
            links = response.headers.get('link').split(',')
            next_url = None
            for link in links:
                try:
                    url, rel = re.search(r'^\<(https.*)\>; rel\=\"(.*)\"$', link.strip()).groups()
                    if rel == 'next':
                        next_url = url
                except AttributeError:
                    next_url = None

        return response.json(), total_records, next_url

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)
