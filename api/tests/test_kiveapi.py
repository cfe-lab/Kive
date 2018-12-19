from io import StringIO

import pytest
from kiveapi import KiveAPI, KiveAuthException, KiveServerException, KiveMalformedDataException, KiveClientException
# noinspection PyPackageRequirements
from mock import patch, DEFAULT
# noinspection PyPackageRequirements
from requests import Session

from kiveapi.dataset import Dataset


@pytest.fixture
def mocked_api():
    with patch.multiple('requests.Session',
                        get=DEFAULT,
                        send=DEFAULT,
                        post=DEFAULT,
                        patch=DEFAULT,
                        delete=DEFAULT,
                        head=DEFAULT):
        yield KiveAPI('http://localhost')


def test_trim():
    session1 = KiveAPI('http://localhost')
    session2 = KiveAPI('http://localhost/')
    expected_url = 'http://localhost'

    assert expected_url == session1.server_url
    assert expected_url == session2.server_url


def test_login_success(mocked_api):
    # noinspection PyUnresolvedReferences
    Session.post.return_value.status_code = 302
    mocked_api.login('joe', 'secret')


def test_login_fail(mocked_api):
    # noinspection PyUnresolvedReferences
    Session.post.return_value.status_code = 200
    with pytest.raises(KiveAuthException,
                       match=r'Incorrect user name or password\.'):
        mocked_api.login('joe', 'secret')


def test_get(mocked_api):
    # noinspection PyUnresolvedReferences
    expected_response = Session.get.return_value

    response = mocked_api.get('/api/foos/42')

    assert expected_response is response
    # noinspection PyUnresolvedReferences
    Session.get.assert_called_once_with('http://localhost/api/foos/42')


def test_item_missing(mocked_api):
    # noinspection PyUnresolvedReferences
    Session.get.return_value.status_code = 404
    with pytest.raises(KiveServerException,
                       match=r'Resource not found!'):
        mocked_api.get('/api/foos/42')


def test_post(mocked_api):
    # noinspection PyUnresolvedReferences
    expected_response = Session.post.return_value
    expected_data = dict(name='my foo')

    response = mocked_api.post('/api/foos/', json=expected_data)

    assert expected_response is response
    # noinspection PyUnresolvedReferences
    Session.post.assert_called_once_with(
        'http://localhost/api/foos/',
        headers={'Content-Type': 'application/json',
                 'referer': 'http://localhost/api/foos/',
                 'X-CSRFToken': None},
        json=expected_data)


def test_post_field_error(mocked_api):
    # noinspection PyUnresolvedReferences
    mock_response = Session.post.return_value
    mock_response.status_code = 400
    mock_response.json.return_value = dict(name='bad name')
    mock_response.ok = False
    with pytest.raises(KiveMalformedDataException,
                       match=r'name: bad name'):
        mocked_api.post('/api/foos', data=dict(name='my foo'))


def test_post_field_error_plus_detail(mocked_api):
    # noinspection PyUnresolvedReferences
    mock_response = Session.post.return_value
    mock_response.status_code = 403
    mock_response.json.return_value = dict(detail='session expired')
    mock_response.ok = False
    with pytest.raises(KiveClientException,
                       match=r'Client error 403: session expired'):
        mocked_api.post('/api/foos', data=dict(name='my foo'))


def test_post_nested_field_error(mocked_api):
    # noinspection PyUnresolvedReferences
    mock_response = Session.post.return_value
    mock_response.status_code = 400
    mock_response.json.return_value = dict(name=['bad name', dict(length='too long')],
                                           age='wrong age')
    mock_response.ok = False
    with pytest.raises(KiveMalformedDataException,
                       match=r'age: wrong age; name: bad name; name.length: too long'):
        mocked_api.post('/api/foos', data=dict(name='my foo'))


def test_post_server_error(mocked_api):
    # noinspection PyUnresolvedReferences
    mock_response = Session.post.return_value
    mock_response.status_code = 500
    mock_response.ok = False
    with pytest.raises(KiveServerException,
                       match=r'Server error 500 on http://localhost\.'):
        mocked_api.post('/api/foos', data=dict(name='my foo'))


def test_patch(mocked_api):
    # noinspection PyUnresolvedReferences
    expected_response = Session.patch.return_value
    expected_data = dict(name='my foo')

    response = mocked_api.patch('/api/foos/', json=expected_data)

    assert expected_response is response
    # noinspection PyUnresolvedReferences
    Session.patch.assert_called_once_with(
        'http://localhost/api/foos/',
        headers={'Content-Type': 'application/json',
                 'referer': 'http://localhost/api/foos/',
                 'X-CSRFToken': None},
        json=expected_data)


def test_delete(mocked_api):
    # noinspection PyUnresolvedReferences
    expected_response = Session.delete.return_value

    response = mocked_api.delete('/api/foos/99/')

    assert expected_response is response
    # noinspection PyUnresolvedReferences
    Session.delete.assert_called_once_with(
        'http://localhost/api/foos/99/',
        headers={'referer': 'http://localhost/api/foos/99/',
                 'X-CSRFToken': None})


def test_filter(mocked_api):
    # noinspection PyUnresolvedReferences
    expected_response = Session.get.return_value
    response = mocked_api.filter('/api/foos',
                                 'name', 'item',
                                 'name', 'big',
                                 'user', 'joe')

    # noinspection PyUnresolvedReferences
    Session.get.assert_called_once_with(
        'http://localhost/api/foos?'
        'filters[0][key]=name&filters[0][val]=item&'
        'filters[1][key]=name&filters[1][val]=big&'
        'filters[2][key]=user&filters[2][val]=joe')
    assert expected_response == response


def test_filter_extra(mocked_api):
    mocked_api.filter('/api/foos?is_granted=true',
                      'name', 'item',
                      'name', 'big',
                      'user', 'joe')

    # noinspection PyUnresolvedReferences
    Session.get.assert_called_once_with(
        'http://localhost/api/foos?'
        'is_granted=true&'
        'filters[0][key]=name&filters[0][val]=item&'
        'filters[1][key]=name&filters[1][val]=big&'
        'filters[2][key]=user&filters[2][val]=joe')


def test_find_datasets_id(mocked_api):
    # noinspection PyUnresolvedReferences
    Session.get.return_value.json.return_value = dict(id=42,
                                                      name='items',
                                                      filename='items.csv')
    datasets = mocked_api.find_datasets(dataset_id=42)

    assert len(datasets) == 1
    assert datasets[0].name == 'items'


def test_find_datasets_filter(mocked_api):
    # noinspection PyUnresolvedReferences
    Session.get.return_value.json.return_value = [dict(id=42,
                                                       name='items',
                                                       filename='items.csv')]
    datasets = mocked_api.find_datasets(name='item', user='joe')

    assert len(datasets) == 1
    assert datasets[0].name == 'items'
    # noinspection PyUnresolvedReferences
    Session.get.assert_called_once_with(
        'http://localhost/api/datasets/?'
        'filters[0][key]=name&filters[0][val]=item&'
        'filters[1][key]=user&filters[1][val]=joe')


def test_dataset_download(mocked_api):
    dataset = Dataset(dict(id=42, filename='data.csv', name='my dataset'),
                      mocked_api)
    # noinspection PyUnresolvedReferences
    Session.get.return_value.iter_content.return_value = [u'line 1\n', u'line 2\n']
    destination = StringIO()
    expected_download = 'line 1\nline 2\n'

    dataset.download(destination)

    # noinspection PyUnresolvedReferences
    Session.get.assert_called_once_with(
        'http://localhost/api/datasets/42/download/',
        stream=True)
    assert expected_download == destination.getvalue()


def test_download(mocked_api):
    # noinspection PyUnresolvedReferences
    expected_response = Session.get.return_value

    response = mocked_api.download("@api_dataset_dl",
                                   context={'dataset-id': 42})

    # noinspection PyUnresolvedReferences
    Session.get.assert_called_once_with(
        'http://localhost/api/datasets/42/download/',
        stream=True)
    assert expected_response == response


def test_download_file(mocked_api):
    # noinspection PyUnresolvedReferences
    Session.get.return_value.iter_content.return_value = [u'line 1\n', u'line 2\n']
    destination = StringIO()
    expected_download = 'line 1\nline 2\n'

    mocked_api.download_file(destination,
                             "@api_dataset_dl",
                             context={'dataset-id': 42})

    # noinspection PyUnresolvedReferences
    Session.get.assert_called_once_with(
        'http://localhost/api/datasets/42/download/',
        stream=True)
    assert expected_download == destination.getvalue()


def test_download_lines(mocked_api):
    # noinspection PyUnresolvedReferences
    Session.get.return_value.iter_lines.return_value = iter([u'line 1',
                                                             u'line 2'])
    expected_lines = [u'line 1\n', u'line 2\n']

    lines = list(mocked_api.download_lines("@api_dataset_dl",
                                           context={'dataset-id': 42}))

    # noinspection PyUnresolvedReferences
    Session.get.assert_called_once_with(
        'http://localhost/api/datasets/42/download/',
        stream=True)
    assert expected_lines == lines


def test_endpoint_get(mocked_api):
    expected_foos = ['foo1', 'foo2']
    # noinspection PyUnresolvedReferences
    Session.get.return_value.json.return_value = expected_foos

    foos = mocked_api.endpoints.foos.get()

    # noinspection PyUnresolvedReferences
    Session.get.assert_called_once_with('http://localhost/api/foos/')
    assert expected_foos == foos


def test_endpoint_get_id(mocked_api):
    mocked_api.endpoints.bars.get(42)

    # noinspection PyUnresolvedReferences
    Session.get.assert_called_once_with('http://localhost/api/bars/42/')


def test_endpoint_head(mocked_api):
    expected_headers = {'Date': 'Fri, 07 Dec 2018 23:46:08 GMT',
                        'Location': '/api/bars/42/'}
    # noinspection PyUnresolvedReferences
    Session.head.return_value.headers = expected_headers

    headers = mocked_api.endpoints.bars.head(42)

    # noinspection PyUnresolvedReferences
    Session.head.assert_called_once_with('http://localhost/api/bars/42/')
    assert expected_headers == headers


def test_endpoint_post(mocked_api):
    params = dict(name='bob', age=43)
    expected_data = dict(name='bob', age=43, id=99)
    # noinspection PyUnresolvedReferences
    Session.post.return_value.json.return_value = expected_data

    data = mocked_api.endpoints.foos.post(json=params)

    # noinspection PyUnresolvedReferences
    Session.post.assert_called_once_with(
        'http://localhost/api/foos/',
        headers={'Content-Type': 'application/json',
                 'referer': 'http://localhost/api/foos/',
                 'X-CSRFToken': None},
        json=params)
    assert expected_data == data


def test_endpoint_delete(mocked_api):
    mocked_api.endpoints.foos.delete(42)

    # noinspection PyUnresolvedReferences
    Session.delete.assert_called_once_with(
        'http://localhost/api/foos/42/',
        headers={'referer': 'http://localhost/api/foos/42/',
                 'X-CSRFToken': None})


def test_endpoint_patch(mocked_api):
    params = dict(name='jane')
    expected_data = dict(name='jane', id=99)
    # noinspection PyUnresolvedReferences
    Session.patch.return_value.json.return_value = expected_data

    data = mocked_api.endpoints.foos.patch('99', json=params)

    # noinspection PyUnresolvedReferences
    Session.patch.assert_called_once_with(
        'http://localhost/api/foos/99/',
        headers={'Content-Type': 'application/json',
                 'referer': 'http://localhost/api/foos/99/',
                 'X-CSRFToken': None},
        json=params)
    assert expected_data == data


def test_endpoint_filter(mocked_api):
    expected_bars = ['bar1', 'bar2']
    # noinspection PyUnresolvedReferences
    Session.get.return_value.json.return_value = expected_bars

    bars = mocked_api.endpoints.bars.filter('name', 'bob', 'age', 23)

    # noinspection PyUnresolvedReferences
    Session.get.assert_called_once_with(
        'http://localhost/api/bars/?'
        'filters[0][key]=name&filters[0][val]=bob&'
        'filters[1][key]=age&filters[1][val]=23')

    assert expected_bars == bars
