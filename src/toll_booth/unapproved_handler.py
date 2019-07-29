import os


def send_unapproved_report(id_source):
    bucket_name = os.environ['LEECH_BUCKET']
    event = {'id_source': id_source, 'report_name': 'unapproved'}
    daily_data = s3_tasks.retrieve_stored_engine_data(bucket_name, id_source, 'daily_data')
    event.update(daily_data)
    tasks = [
        reporting_tasks.build_clinical_teams,
        reporting_tasks.build_clinical_caseloads,
        audit_tasks.audit_encounters,
        audit_tasks.format_audit_results,
        reporting_tasks.write_report_data,
        reporting_tasks.send_report
    ]
    for task in tasks:
        task_results = task(**event)
        event.update(task_results)
    return True
