from algernon.aws import lambda_logged
from aws_xray_sdk.core import xray_recorder

from toll_booth.tasks import credible_fe_tasks


@xray_recorder.capture('surgeon_handler')
@lambda_logged
def handler(event, context):
    tasks = [
        credible_fe_tasks.get_productivity_report_data,
        credible_fe_tasks.build_clinical_teams,
        credible_fe_tasks.build_clinical_caseloads,
        credible_fe_tasks.build_daily_report,
        credible_fe_tasks.write_report_data,
        credible_fe_tasks.send_report
    ]
    for task in tasks:
        task_data = task(**event)
        event.update(task_data)
    return True
