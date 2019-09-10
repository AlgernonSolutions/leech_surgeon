import json
import os

import pytest

from toll_booth import engine_handler, builder_handler, send_handler, get_months_intake, audit_encounter_handler
from toll_booth.provider_audit_handler import provider_audit_handler

ids = [11838, 12328, 12020, 10154]


@pytest.mark.tasks_i
class TestTasks:
    @pytest.mark.audit_encounter_i
    def test_audit_encounter(self, stored_event_generator, mock_context):
        os.environ['LEECH_BUCKET'] = 'surgeon-asset-bucket-surgeon'
        os.environ['CLIENT_DATA_TABLE_NAME'] = 'ClientData'
        os.environ['ELASTIC_HOST'] = 'vpc-algernon-test-ankmhqkcdnx2izwfkwys67wmiq.us-east-1.es.amazonaws.com'

        event = stored_event_generator('audit_encounter')
        fn_payload = event['fn_payload']
        # fn_payload = json.loads(event['fn_payload'])
        results = audit_encounter_handler(fn_payload, mock_context)
        assert results

    @pytest.mark.audit_provider_i
    def test_audit_provider(self, mock_context):
        id_source = 'PSI'
        provider_id = '11838'
        os.environ['LEECH_BUCKET'] = 'surgeon-asset-bucket-surgeon'
        os.environ['CLIENT_DATA_TABLE_NAME'] = 'ClientData'
        os.environ['ELASTIC_HOST'] = 'vpc-algernon-test-ankmhqkcdnx2izwfkwys67wmiq.us-east-1.es.amazonaws.com'

        event = {
            'id_source': id_source,
            'provider_id': provider_id,
            'start_date': '3/1/2019',
            'end_date': '9/1/2019'
        }
        results = provider_audit_handler(event, mock_context)
        assert results

    @pytest.mark.intake_i
    def test_intake_report(self, mock_context):
        id_source = 'PSI'
        os.environ['LEECH_BUCKET'] = 'surgeon-asset-bucket-surgeon'
        event = {'id_source': id_source, 'intake_month_number': 7}
        results = get_months_intake(id_source, 7)
        assert results

    @pytest.mark.engine_i
    def test_engine_handler(self, mock_context):
        id_source = 'PSI'
        os.environ['LEECH_BUCKET'] = 'algernonsolutions-leech-dev'
        event = {'id_source': id_source}
        results = engine_handler(event, mock_context)
        assert results

    @pytest.mark.build_i
    def test_builder_handler(self, mock_context):
        id_source = 'PSI'
        os.environ['LEECH_BUCKET'] = 'surgeon-asset-bucket-surgeon'
        os.environ['CLIENT_DATA_TABLE_NAME'] = 'ClientData'
        event = {'id_source': id_source}
        results = builder_handler(event, mock_context)
        assert results is None

    @pytest.mark.send_i
    def test_send_handler(self, mock_context):
        id_source = 'PSI'
        os.environ['LEECH_BUCKET'] = 'surgeon-asset-bucket-surgeon'
        os.environ['CLIENT_DATA_TABLE_NAME'] = 'ClientData'
        event = {'id_source': id_source}
        results = send_handler(event, mock_context)
        assert results

    @pytest.mark.build_send_i
    def test_build_and_send(self, mock_context):
        self.test_builder_handler(mock_context)
        self.test_send_handler(mock_context)
