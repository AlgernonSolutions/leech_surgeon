import pytest

from toll_booth.tasks import credible_fe_tasks


def _generate_client_data(source_data):
    print()


def _generate_emp_data(source_data):
    results = []
    found_supervisors = [x['Supervisors'] for x in source_data if x['Supervisors']]

    for entry in source_data:
        first_name = entry['First Name']
        last_name = entry['Last Name']
        supervisors = entry['Supervisors']
        supervisees = entry['Supervisees']
        if not supervisees:
            entry['First Name'] = 'XXXXXXXXX'
            entry['Last Name'] = 'XXXXXXXXX'
            results.append(entry)
            continue
        print()
    return results


def _generate_encounter_data(source_data):
    print()


@pytest.mark.tasks_i
class TestTasks:
    def test_get_productivity_report_data(self):
        event = {'id_source': 'PSI'}
        results = credible_fe_tasks.get_productivity_report_data(**event)
        test_data = {
            'emp_data': _generate_emp_data(results['emp_data']),
            'client_data': _generate_client_data(results['client_data']),
            'encounter_data': _generate_encounter_data(results['encounter_data']),
            'unapproved_data': _generate_encounter_data(results['unapproved_data']),
            'tx_data': _generate_encounter_data(results['tx_data']),
            'da_data': _generate_encounter_data(results['da_data']),
        }
        assert results

    def test_build_clinical_teams(self, id_source, mock_static_json, mock_response_generator):
        event = {
            'id_source': id_source,
            'emp_data': mock_response_generator('emp_data')
        }
        results = credible_fe_tasks.build_clinical_teams(**event)
        assert results

    def test_build_daily_report(self, mock_context):
        pass

    def test_build_clinical_caseloads(self, mock_context):
        pass

    def test_write_report_data(self, mock_context):
        pass

    def test_send_report(self, mock_context):
        pass
