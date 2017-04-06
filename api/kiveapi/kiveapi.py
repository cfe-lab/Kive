"""
Contains the main class for the accessing Kive's
RESTful API.

"""
import logging

import requests
from requests import Session

from .dataset import Dataset
from .datatype import CompoundDatatype
from .errors import KiveMalformedDataException, KiveAuthException,\
    KiveClientException, KiveServerException, is_client_error, is_server_error
from .pipeline import PipelineFamily, Pipeline
from .runstatus import RunStatus
from .runbatch import RunBatch

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
        for field, errors in fields.items():
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
                logger.warn('Error response %d for %s: %s',
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

    def post(self, *args, **kwargs):
        nargs = list(args)
        url = self._prep_url(nargs[0])
        nargs[0] = url
        is_json = kwargs.pop('is_json', True)
        headers = kwargs.get('headers', {})
        kwargs['headers'] = headers
        headers.setdefault('referer', url)
        if 'json' in kwargs:
            headers.setdefault('Content-Type', 'application/json')
        if hasattr(self, 'csrf_token'):
            headers.setdefault('X-CSRFToken', self.csrf_token)
        return self._validate_response(super(KiveAPI, self).post(*nargs, **kwargs),
                                       is_json=is_json)

    def put(self, *args, **kwargs):
        nargs = list(args)
        nargs[0] = self._prep_url(nargs[0])
        return self._validate_response(super(KiveAPI, self).put(*nargs, **kwargs))

    def delete(self, *args, **kwargs):
        nargs = list(args)
        nargs[0] = self._prep_url(nargs[0])
        return self._validate_response(super(KiveAPI, self).delete(*nargs, **kwargs))

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
        filter_text = '&'.join(
            'filters[{}][key]={}&filters[{}][val]={}'.format(i, key, i, val)
            for i, (key, val) in enumerate(filters.items()))
        datasets = self.get('@api_find_datasets',
                            context={'filters': filter_text}).json()
        return [Dataset(d, self) for d in datasets]

    def get_pipeline_families(self):
        """
        Returns a list of all pipeline families and
        the pipeline revisions underneath each.

        :return: List of PipelineFamily objects
        """

        families = self.get('@api_pipeline_families').json()
        return [PipelineFamily(c) for c in families]

    def get_pipeline_family(self, pipeline_fam_id):
        """
        Returns a PipelineFamily object for a specific id

        :param pipeline_fam_id:
        :return: PipelineFamily Object
        """
        family = self.get('@api_pipeline_family', context={'family-id': pipeline_fam_id}).json()
        return PipelineFamily(family)

    def get_pipelines(self):
        pipelines = self.get('@api_pipelines').json()
        return [Pipeline(c) for c in pipelines]

    def get_pipeline(self, pipeline_id):
        """

        :param pipeline_id:
        :return: Pipeline object
        """
        pipeline = self.get('@api_pipeline', context={'pipeline-id': pipeline_id}).json()
        return Pipeline(pipeline)

    def get_cdts(self):
        """
        Returns a list of all current compound datatypes

        :return: A list of CompoundDatatypes objects
        """

        data = self.get('@api_get_cdts').json()
        return [CompoundDatatype(c) for c in data]

    def get_cdt(self, cdt_id):
        """
        Returns a CDT object for a specific id.

        :param cdt_id:
        :return: CompoundDatatype object.
        """
        data = self.get('@api_get_cdt', context={'cdt-id': cdt_id}).json()
        return CompoundDatatype(data)

    def create_cdt(self, name, users=None, groups=None, members=None):
        """
        Create a CompoundDatatype.

        :param name: The name of this CompoundDatatype
        :param users: None or a list of user names that should be
            allowed to access the CDT
        :param groups: None or a list of group names that should be
            allowed to access the CDT
        :param members: A list of dictionaries defining the columns of the CDT,
            each containing fields:
             - "column_idx": an integer, 1 or greater
             - "column_name": column header
             - "datatype": primary key of the Datatype for this column
             - "blankable": (optional) if True, this column accepts blank entries
        :return: CompoundDatatype object
        """
        users_allowed = users or []
        groups_allowed = groups or []

        cdt_dict = {
            "name": name,
            "users_allowed": users_allowed,
            "groups_allowed": groups_allowed,
            "members": members
        }
        cdt = self.post("@api_get_cdts", json=cdt_dict, is_json=True).json()
        return CompoundDatatype(cdt)

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

    def run_pipeline(self,
                     pipeline,
                     inputs,
                     name=None,
                     force=False,
                     runbatch=None,
                     users=None,
                     groups=None):
        """
        Checks that a pipeline has the correct inputs, then
        submits the job to kive.

        :param pipeline: A Pipeline Object
        :param inputs: A list of Datasets
        :param name: An optional name for the run
        :param force: True if the datasets should not be checked for matching
            compound datatypes
        :param runbatch: An optional RunBatch object or the integer primary key of an object
        :param users: None or a list of user names that should be
            allowed to see the run
        :param groups: None or a list of group names that should be
            allowed to see the run
        :return: A RunStatus object
        """
        users_allowed = users or []
        groups_allowed = groups or []

        # Check to see if we can even call this pipeline
        if len(inputs) != len(pipeline.inputs):
            raise KiveMalformedDataException(
                'Number of inputs to pipeline is not equal to the number of given inputs (%d != %d)' % (
                    len(pipeline.inputs),
                    len(inputs)
                )
            )

        if not force:
            # Check to see if the CDT for each input matches the
            # Expected CDT
            zlist = zip(inputs, pipeline.inputs)

            for dset, pipeline_instance in zlist:
                if dset.cdt != pipeline_instance.compounddatatype:
                    raise KiveMalformedDataException(
                        'Content check failed (%s != %s)! ' % (str(dset.cdt), str(pipeline_instance.compounddatatype))
                    )

        # Construct the inputs
        params = dict(
            pipeline=pipeline.pipeline_id,
            inputs=[dict(dataset=d.dataset_id,
                         index=i)
                    for i, d in enumerate(inputs, 1)],
            name=name if name is not None else "",
            users_allowed=users_allowed,
            groups_allowed=groups_allowed,
            runbatch=runbatch.id if isinstance(runbatch, RunBatch) else runbatch)
        run = self.post('@api_runs',
                        json=params,
                        is_json=True).json()
        return RunStatus(run, self)

    def create_run_batch(self,
                         name=None,
                         description=None,
                         users=None,
                         groups=None):
        """
        Creates a RunBatch in Kive.

        :param name: An optional name for the RunBatch
        :param description: An optional description for the RunBatch
        :param users: None or a list of user names that should be
            allowed to see the run
        :param groups: None or a list of group names that should be
            allowed to see the run
        :return: A RunStatus object
        """
        # Construct the inputs
        params = dict(name=name,
                      description=description,
                      users_allowed=users,
                      groups_allowed=groups)
        run = self.post('@api_runbatches',
                        json=params,
                        is_json=True).json()
        return RunBatch(run)

    def get_run(self, id):
        """ Get a RunStatus object for the given id.

        :param id: a Run id
        :return: RunStatus object.
        """
        data = self.get('@api_run', context={'run-id': id}).json()
        return RunStatus(data, self)

    def find_runs(self,
                  run_id=None,
                  **kwargs):
        """ Find a list of runs that match search criteria

        :param run_id: a specific run id to find, result will be a list
            with one entry, and other search parameters will be ignored
        :param kwargs: other search parameters, with the same names as the
            keys used by the API
        :return: a list of RunStatus objects
        """
        if run_id is not None:
            return [self.get_run(run_id)]

        filters = kwargs
        filter_text = '&'.join(
            'filters[{}][key]={}&filters[{}][val]={}'.format(i, key, i, val)
            for i, (key, val) in enumerate(filters.items()))
        runs = self.get('@api_find_runs',
                        context={'filters': filter_text}).json()
        return [RunStatus(run, self) for run in runs]
