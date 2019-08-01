from datetime import datetime, timedelta

import pytest
from toll_booth.tasks import credible_fe_tasks


@pytest.mark.credible_tasks_i
class TestCredibleTasks:
    def test_get_tx_plans(self, psi_credible_driver):
        today = datetime.now()
        seven_months_ago = datetime.now() - timedelta(days=210)
        results = credible_fe_tasks.get_tx_plans(psi_credible_driver, seven_months_ago, today)
        assert results
