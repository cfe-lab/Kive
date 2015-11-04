"""
Contains the main class for the accessing Kive's
RESTful API.

"""
import json
import logging

import requests
from requests import Session

from .dataset import Dataset
from .datatype import CompoundDatatype
from .errors import KiveMalformedDataException, KiveAuthException,\
    KiveClientException, KiveServerException, is_client_error, is_server_error
from .pipeline import PipelineFamily, Pipeline
from .runstatus import RunStatus

logger = logging.getLogger('kiveapi')


class KiveAPI(Session):
    """
    The main KiveAPI class
    """
    SERVER_URL = ""

    def __init__(self, username, password, server=None, verify=True):
        self.server_url = server

        if server is None:
            self.server_url = KiveAPI.SERVER_URL

        if self.server_url[-1] == '/':
            self.server_url = self.server_url[:-1]

        self.endpoint_map = {
            'api_auth': '/login/',
            'api_get_cdts': '/api/compounddatatypes/',
            'api_get_cdt': '/api/compounddatatypes/{cdt-id}/',

            'api_get_datasets': '/api/datasets/',
            'api_get_dataset': '/api/datasets/{dataset-id}/',
            'api_dataset_add': '/api/datasets/',
            'api_dataset_dl': '/api/datasets/{dataset-id}/download/',

            'api_pipeline_families': '/api/pipelinefamilies/',
            'api_pipeline_family': '/api/pipelinefamilies/{family-id}/',

            'api_pipelines': '/api/pipelines/',
            'api_pipeline': '/api/pipelines/{pipeline-id}/',

            'api_runs': '/api/runs/',
            'api_run': '/api/runs/{run-id}/',

        }
        super(KiveAPI, self).__init__()
        self.verify = verify
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
        self.csrf_token = login_response.cookies['csrftoken']

    def _prep_url(self, url):
        if url[0] == '@':
            url = self.endpoint_map[url[1:]]
        if url[0] == '/':
            url = self.server_url + url
        return url

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
                        json_fields = json_data.iteritems()
                        message += ': '
                        message += '; '.join((field + ': ' + ', '.join(errors)
                                              for field, errors in json_fields))
                    raise KiveMalformedDataException(message)
                if is_json and 'detail' in json_data:
                    message += ': ' + json_data['detail']
                raise KiveClientException("Couldn't authorize request!")

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
        nargs[0] = self._prep_url(nargs[0])
        is_json = kwargs.pop('is_json', True)
        if hasattr(self, 'csrf_token'):
            nargs[1]['csrfmiddlewaretoken'] = self.csrf_token
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

    def get_datasets(self):
        """
        Returns a list of all datasets.

        :return: A list of Dataset objects.
        """

        datasets = self.get('@api_get_datasets').json()
        return [Dataset(d, self) for d in datasets]

    def get_dataset(self, dataset_id):
        """
        Gets a dataset in kive by its ID.

        :param dataset_id: Integer id
        :return: Dataset object
        """

        dataset = self.get('@api_get_dataset', context={'dataset-id': dataset_id}).json()
        return Dataset(dataset, self)

    def find_datasets(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        datasets = self.get_datasets()
        ret = []
        if 'dataset_id' in kwargs:
            ret += filter(lambda d: d.dataset_id == kwargs['dataset_id'], datasets)

        if 'dataset_name' in kwargs:
            ret += filter(lambda d: d.name == kwargs['dataset_name'], datasets)

        return ret

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

    def add_dataset(self, name, description, handle, cdt=None, users=None, groups=None):
        """
        Adds a dataset to kive under the user associated
        with the token.

        :return: Dataset object
        """
        users_allowed = users or []
        groups_allowed = groups or []

        dataset = self.post('@api_dataset_add', {
            'name': name,
            'description': description,
            'users_allowed': users_allowed,
            'groups_allowed': groups_allowed,
            'compounddatatype': cdt and cdt.cdt_id
        }, files={
            'dataset_file': handle,
        }).json()
        return Dataset(dataset, self)

    def run_pipeline(self,
                     pipeline,
                     inputs,
                     name=None,
                     force=False,
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
                    len(inputs),
                    len(pipeline.inputs)
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
        params = dict(pipeline=pipeline.pipeline_id,
                      inputs=[dict(symbolicdataset=d.symbolicdataset_id,
                                   index=i)
                              for i, d in enumerate(inputs, 1)],
                      name=name,
                      users_allowed=users_allowed,
                      groups_allowed=groups_allowed)
        form = dict(_content_type='application/json',
                    _content=json.dumps(params))

        run = self.post('@api_runs', form).json()
        return RunStatus(run, self)

    def get_run(self, id):
        """ Get a RunStatus object for the given id.

        :param id: a RunToProcess id
        :return: RunStatus object.
        """
        data = self.get('@api_run', context={'run-id': id}).json()
        return RunStatus(data, self)
