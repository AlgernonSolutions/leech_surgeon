import os

import pytest

from toll_booth import engine_handler, builder_handler


@pytest.mark.tasks_i
class TestTasks:
    def test_builder_handler(self, mock_context):
        id_source = 'PSI'
        os.environ['LEECH_BUCKET'] = 'algernonsolutions-leech-dev'
        os.environ['CLIENT_DATA_TABLE_NAME'] = 'ClientData'
        event = {'id_source': id_source}
        results = builder_handler(event, mock_context)
        assert results

    def test_engine_handler(self, mock_context):
        id_source = 'PSI'
        os.environ['LEECH_BUCKET'] = 'algernonsolutions-leech-dev'
        event = {'id_source': id_source}
        results = engine_handler(event, mock_context)
        assert results
