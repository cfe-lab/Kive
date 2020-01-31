"""
Contains the main class for the accessing Kive's
RESTful API.

"""
import logging
from itertools import chain

# noinspection PyPackageRequirements
import requests
# noinspection PyPackageRequirements
from requests import Session

from .endpoint_manager import EndpointManager
from .dataset import Dataset
from .errors import KiveMalformedDataException, KiveAuthException,\
    KiveClientException, KiveServerException, is_client_error, is_server_error

logger = logging.getLogger('kiveapi')


class KiveAPI(Session):
    """
    The main KiveAPI class
    """
    SERVER_URL = ""

    def __init__(self, server=None, verify=True):
        self.server_url = server

        if server is None:
            self.server_url = KiveAPI.SERVER_URL

        if self.server_url[-1] == '/':
            self.server_url = self.server_url[:-1]

        self.endpoint_map = {
            'api_auth': '/login/',
            'api_get_cdts': '/api/compounddatatypes/',
            'api_get_cdt': '/api/compounddatatypes/{cdt-id}/',

            'api_get_dataset': '/api/datasets/{dataset-id}/',
            'api_find_datasets': '/api/datasets/?{filters}',
            'api_dataset_add': '/api/datasets/',
            'api_dataset_dl': '/api/datasets/{dataset-id}/download/',

            'api_pipeline_families': '/api/pipelinefamilies/',
            'api_pipeline_family': '/api/pipelinefamilies/{family-id}/',

            'api_pipelines': '/api/pipelines/',
            'api_pipeline': '/api/pipelines/{pipeline-id}/',

            'api_runs': '/api/runs/',
            'api_run': '/api/runs/{run-id}/',
            'api_find_runs': '/api/runs/status/?{filters}',

            'api_runbatches': '/api/runbatches/',
            'api_runbatch': '/api/runbatches/{runbatch-id}'
        }
        super(KiveAPI, self).__init__()
        self.verify = verify
        self.csrf_token = None

    @property
    def endpoints(self):
        """ An open-ended interface to the API.

        You can access endpoints through named properties, and you don't need
        a new library version when Kive adds new endpoints or filters.
        For example, you can get a specific Dataset like this:

        >>> import kiveapi
        >>> session = kiveapi.KiveAPI('https://localhost:8000')
        >>> session.login('joe', 'secret')
        >>> ds = session.endpoints.datasets.get(109)
        >>> ds['name'], ds['url']
        (u'greetings_csv', u'https://localhost:8000/api/datasets/109/')

        You can create a new batch with a call to post().

        >>> batch = session.endpoints.batches.post(json=dict(name='Chocolate Cookies'))
        >>> batch['name'], batch['id']
        (u'Chocolate Cookies', 52)

        You can search for batches with a call to filter(). This example looks
        for any batches with 'cookies' in the name. You can add more filters
        by adding more pairs of arguments.

        >>> batches = session.endpoints.batches.filter('name', 'cookies')
        >>> [(batch['name'], batch['id']) for batch in batches]
        [(u'Chocolate Cookies', 52), (u'Sugar Cookies', 51)]

        There are also methods for delete, patch, and head.
        """
        return EndpointManager(self)

    def login(self, username, password):
        self.fetch_csrf_token()  # for the login request
        response = self.post('@api_auth',
                             {'username': username, 'password': password},
                             allow_redirects=False,
                             is_json=False)

        # When the login fails, it just displays the login form again.
        # On success, it redirects to the home page (status code 'found').
        if response.status_code != requests.codes['found']:
            raise KiveAuthException('Incorrect user name or password.')
        self.fetch_csrf_token()  # for the next request
        self.headers.update({'referer': self.server_url})

    def fetch_csrf_token(self):
        login_response = self.head('@api_auth')
        if login_response.status_code == requests.codes['found']:
            message = 'Fetching a CSRF token failed with a redirect to {!r}.'.format(
                login_response.headers.get('location'))
            raise RuntimeError(message)
        self.csrf_token = login_response.cookies['csrftoken']

    def _prep_url(self, url):
        if url[0] == '@':
            url = self.endpoint_map[url[1:]]
        if url[0] == '/':
            url = self.server_url + url
        return url

    def _format_field_errors(self, fields, context=None):
        if context is None:
            context = []
        messages = []
        for field, errors in sorted(fields.items()):
            context.append(field)
            if isinstance(errors, list):
                for error in errors:
                    if isinstance(error, dict):
                        messages.extend(self._format_field_errors(error,
                                                                  context))
                    else:
                        messages.append('{}: {}'.format('.'.join(context),
                                                        error))
            else:
                messages.append('{}: {}'.format('.'.join(context),
                                                errors))
            context.pop()
        return messages

    def _validate_response(self, response, is_json=True):
        try:
            if not response.ok:
                logger.warning('Error response %d for %s: %s',
                               response.status_code,
                               response.url,
                               response.text)
            if is_server_error(response.status_code):
                raise KiveServerException("Server error {} on {}.".format(
                    response.status_code,
                    self.server_url))
            if is_json:
                json_data = response.json()
            else:
                json_data = []

            if response.status_code == requests.codes['not_found']:
                raise KiveServerException('Resource not found!')

            if is_client_error(response.status_code):
                message = 'Client error {}'.format(response.status_code)
                if response.status_code in (requests.codes['bad_request'],
                                            requests.codes['conflict']):
                    if is_json:
                        field_error_messages = self._format_field_errors(json_data)
                        message += ': '
                        message += '; '.join(field_error_messages)
                    raise KiveMalformedDataException(message)
                if is_json and 'detail' in json_data:
                    # noinspection PyTypeChecker
                    message += ': ' + json_data['detail']
                raise KiveClientException(message)

        except ValueError:
            raise KiveMalformedDataException("Malformed response from server! (check server config '%s!')\n%s" %
                                             (self.server_url, response))
        return response

    def get(self, *args, **kwargs):
        nargs = list(args)
        nargs[0] = self._prep_url(nargs[0])

        is_json = kwargs.pop('is_json', True)
        context = kwargs.pop('context', None)
        if context:
            nargs[0] = nargs[0].format(**context)

        return self._validate_response(super(KiveAPI, self).get(*nargs, **kwargs),
                                       is_json=is_json)

    def download(self, *args, **kwargs):
        """ Send a download request for a URL.

        :return: a response object
        """
        kwargs.update(is_json=False, stream=True)
        return self.get(*args, **kwargs)

    def download_file(self, handle, *args, **kwargs):
        """ Downloads from a URL and streams it into handle

        :param handle: A file handle
        """

        for block in self.download(*args, **kwargs).iter_content(1024):
            handle.write(block)

    def download_lines(self, *args, **kwargs):
        """ Returns an iterator to lines in the response, including newlines.
        """

        for line in self.download(*args, **kwargs).iter_lines(decode_unicode=True):
            yield line + '\n'

    def filter(self, url, *args, **kwargs):
        """ Filter a get request with name/value pairs.

        For example, session.filter('/api/datasets',
                                    'name', 'titles.csv',
                                    'user', 'joe')
        is equivalent to session.get(
            '/api/datasets?'
            'filters[0][key]=name&filters[0][val]=titles.csv&'
            'filters[1][key]=user&filters[1][val]=joe')
        """
        filters = []
        pairs = iter(args)
        for i, (name, value) in enumerate(zip(pairs, pairs)):
            filters.append('filters[{}][key]={}'.format(i, name))
            filters.append('filters[{}][val]={}'.format(i, value))
        if '?' in url:
            url += '&'
        else:
            url += '?'
        url += '&'.join(filters)
        return self.get(url, **kwargs)

    def post(self, *args, **kwargs):
        nargs = list(args)
        url = self._prep_url(nargs[0])
        nargs[0] = url
        is_json = kwargs.pop('is_json', True)
        self._prep_headers(kwargs, url)
        return self._validate_response(super(KiveAPI, self).post(*nargs, **kwargs),
                                       is_json=is_json)

    def _prep_headers(self, kwargs, url):
        headers = kwargs.get('headers', {})
        kwargs['headers'] = headers
        headers.setdefault('referer', url)
        if 'json' in kwargs:
            headers.setdefault('Content-Type', 'application/json')
        if hasattr(self, 'csrf_token'):
            headers.setdefault('X-CSRFToken', self.csrf_token)

    def patch(self, *args, **kwargs):
        nargs = list(args)
        nargs[0] = self._prep_url(nargs[0])
        self._prep_headers(kwargs, nargs[0])
        return self._validate_response(super(KiveAPI, self).patch(*nargs, **kwargs))

    def delete(self, *args, **kwargs):
        nargs = list(args)
        nargs[0] = self._prep_url(nargs[0])
        self._prep_headers(kwargs, nargs[0])
        return self._validate_response(super(KiveAPI, self).delete(*nargs, **kwargs),
                                       is_json=False)

    def head(self, *args, **kwargs):
        newargs = list(args)
        newargs[0] = self._prep_url(newargs[0])
        return self._validate_response(super(KiveAPI, self).head(*newargs, **kwargs),
                                       is_json=False)

    def get_dataset(self, dataset_id):
        """
        Gets a dataset in kive by its ID.

        :param dataset_id: Integer id
        :return: Dataset object
        """

        dataset = self.get('@api_get_dataset', context={'dataset-id': dataset_id}).json()
        return Dataset(dataset, self)

    def find_datasets(self,
                      dataset_id=None,
                      cdt=None,
                      **kwargs):
        """ Find a list of datasets that match search criteria.

        :param dataset_id: a specific dataset id to find, result will be a list
            with one entry, and other search parameters will be ignored
        :param cdt: a CompoundDataType object that must match the datasets'
            compound data types
        :param kwargs: other search parameters, with the same names as the
            keys used by the API
        :return: a list of Dataset objects
        """
        if dataset_id is not None:
            return [self.get_dataset(dataset_id)]

        filters = dict(kwargs)
        if cdt is not None:
            filters['cdt'] = cdt.cdt_id
        filter_args = chain.from_iterable(filters.items())
        datasets = self.filter('/api/datasets/', *filter_args).json()
        return [Dataset(d, self) for d in datasets]

    def add_dataset(self,
                    name,
                    description,
                    handle=None,
                    cdt=None,
                    users=None,
                    groups=None,
                    externalfiledirectory=None,
                    external_path=None):
        """ Adds a dataset to kive.

        :param str name: a name for the dataset
        :param str description: a description of the dataset
        :param handle: an open file object with the dataset contents, or None
            for external datasets
        :param cdt: a CompoundDatatype object, or None for a raw dataset
        :param list users: a list of user names that will have access
        :param list groups: a list of group names that will have access
        :param str externalfiledirectory: name of an external file directory,
            or None for internal datasets
        :param str external_path: path relative to external file directory,
            or None for internal datasets
        :return: Dataset object
        """
        users_allowed = users or []
        groups_allowed = groups or []

        metadata_dict = {
            'name': name,
            'description': description,
            'users_allowed': users_allowed,
            'groups_allowed': groups_allowed,
            'compounddatatype': cdt and cdt.cdt_id
        }

        if not external_path:
            dataset = self.post(
                '@api_dataset_add',
                metadata_dict,
                files={'dataset_file': handle}
            ).json()
        else:
            metadata_dict.update(
                {
                    'externalfiledirectory': externalfiledirectory,
                    'external_path': external_path
                }
            )
            dataset = self.post(
                '@api_dataset_add',
                metadata_dict
            ).json()

        return Dataset(dataset, self)
