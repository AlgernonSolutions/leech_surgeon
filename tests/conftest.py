import json
from datetime import datetime
from os import path
from unittest.mock import MagicMock, patch

import pytest
import requests


@pytest.fixture(autouse=True)
def silence_x_ray():
    x_ray_patch_all = 'algernon.aws.lambda_logging.patch_all'
    patch(x_ray_patch_all).start()
    yield
    patch.stopall()


@pytest.fixture
def stored_event_generator():
    return _read_test_event


@pytest.fixture
def mock_response_generator():
    return _read_mock_response


@pytest.fixture
def mock_static_json():
    with patch('toll_booth.tasks.credible_fe_tasks.StaticJson') as mock_json:
        mock_json.for_team_data.return_value = _read_mock_response('team_data')
        yield mock_json


@pytest.fixture
def mock_driver():
    with patch('toll_booth.tasks.credible_fe_tasks.CredibleFrontEndDriver') as mock_driver:
        yield mock_driver


@pytest.fixture
def id_source():
    return 'Algernon'


@pytest.fixture
def mock_credible_login():
    with patch('requests.sessions.Session.post') as mock_request:
        first_response = MagicMock()
        first_response.json.return_value = {'SessionCookie': 'some_value'}
        mock_request.side_effect = [first_response, MagicMock()]
        yield mock_request


@pytest.fixture
def mock_login_credentials(mocked_method, stubbed_responses):
    with patch(getattr(requests.sessions.Session, mocked_method)) as mock_request:
        mock_request.sid_effect = stubbed_responses
        yield mock_request


@pytest.fixture
def mock_opossum():
    with patch('algernon.aws.Opossum.get_untrustworthy_credentials') as opossum:
        opossum.return_value = {
            'username': 'algernon',
            'password': 'myHovercraftFul1of33ls',
            'domain_name': 'ALG'
        }
        yield opossum


@pytest.fixture
def mock_context():
    context = MagicMock(name='context')
    context.__reduce__ = cheap_mock
    context.function_name = 'test_function'
    context.invoked_function_arn = 'test_function_arn'
    context.aws_request_id = '12344_request_id'
    context.get_remaining_time_in_millis.side_effect = [1000001, 500001, 250000, 0]
    return context


def cheap_mock(*args):
    from unittest.mock import Mock
    return Mock, ()


def _read_test_event(event_name):
    with open(path.join('test_events', f'{event_name}.json')) as json_file:
        event = json.load(json_file)
        return event


def _read_mock_response(event_name):
    with open(path.join('mock_responses', f'{event_name}.json')) as json_file:
        event = json.load(json_file)
        for entry, value in event.items():
            if 'Date' in entry:
                event[entry] = datetime.fromisoformat(value)
        return event

