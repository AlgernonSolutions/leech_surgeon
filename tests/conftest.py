import json
from os import path
from unittest.mock import MagicMock, patch

import pytest
import requests


@pytest.fixture
def id_source():
    return 'Algernon'


@pytest.fixture
def mock_credible(stubbed_response, mocked_method):
    with patch(getattr(requests, mocked_method)) as mock_request:
        mock_request.return_value = stubbed_response
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
