import pytest

from toll_booth.tasks import credible_fe_tasks


@pytest.mark.tasks
class TestTasks:
    def test_get_productivity_report_data(self, id_source, mock_driver, mock_x_ray, mock_context):
        event = {'id_source': id_source}
        results = credible_fe_tasks.get_productivity_report_data(event, mock_context)
        assert results

    def test_build_clinical_teams(self, mock_context):
        pass

    def test_build_daily_report(self, mock_context):
        pass

    def test_build_clinical_caseloads(self, mock_context):
        pass

    def test_write_report_data(self, mock_context):
        pass

    def test_send_report(self, mock_context):
        pass
