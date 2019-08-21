import os

import pytest

from toll_booth import engine_handler, builder_handler, send_handler


@pytest.mark.tasks_i
class TestTasks:
    @pytest.mark.engine_i
    def test_engine_handler(self, mock_context):
        id_source = 'ICFS'
        os.environ['LEECH_BUCKET'] = 'surgeon-asset-bucket-surgeon'
        event = {'id_source': id_source}
        results = engine_handler(event, mock_context)
        assert results

    @pytest.mark.build_i
    def test_builder_handler(self, mock_context):
        id_source = 'ICFS'
        os.environ['LEECH_BUCKET'] = 'surgeon-asset-bucket-surgeon'
        os.environ['CLIENT_DATA_TABLE_NAME'] = 'ClientData'
        event = {'id_source': id_source}
        results = builder_handler(event, mock_context)
        assert results is None

    @pytest.mark.send_i
    def test_send_handler(self, mock_context):
        id_source = 'PSI'
        os.environ['LEECH_BUCKET'] = 'algernonsolutions-leech-dev'
        os.environ['CLIENT_DATA_TABLE_NAME'] = 'ClientData'
        event = {'id_source': id_source}
        results = send_handler(event, mock_context)
        assert results

    @pytest.mark.build_send_i
    def test_build_and_send(self, mock_context):
        self.test_builder_handler(mock_context)
        self.test_send_handler(mock_context)
