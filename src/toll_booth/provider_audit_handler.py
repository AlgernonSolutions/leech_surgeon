from algernon import rebuild_event
from algernon.aws import lambda_logged
from aws_xray_sdk.core import xray_recorder
from botocore.exceptions import ClientError

from toll_booth.obj import CredibleFrontEndDriver
from toll_booth.obj.reports import ReportData
from toll_booth.tasks import credible_fe_tasks, audit_tasks, building_tasks, s3_tasks
from toll_booth.tasks.building_tasks.invert_caseloads import invert_caseloads
from toll_booth.tasks.write_report_data import write_report_data


def _run_task(driver, bucket_name, task_name, task_fn, task_args):
    id_source = driver.id_source
    try:
        report_data = s3_tasks.retrieve_stored_engine_data(bucket_name, id_source, task_name)
        return rebuild_event(report_data)
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchKey':
            raise e
        report_data = task_fn(driver, *task_args)
        if report_data:
            _store_engine_data(bucket_name, id_source, task_name, report_data)
        return report_data


def _store_engine_data(bucket_name, id_source, task_name, report_data):
    file_key = s3_tasks.build_engine_file_key(task_name, id_source)
    s3_tasks.store_engine_data(bucket_name, file_key, report_data)


def build_audit_results(audit_results):
    header = ['Encounter ID', 'Provider ID', 'Provider Name', 'Patient ID', 'Patient Name', 'Audit Results']
    formatted = []
    for entry in audit_results:
        encounter_id = str(entry.encounter_id)
        provider_id = str(entry.provider_id)
        patient_id = str(entry.patient_id)
        findings = 'nothing found'
        if entry.findings:
            findings = '\n'.join(str(x) for x in entry.findings)
        formatted_row = {
            'encounter_id': encounter_id,
            'provider_id': provider_id,
            'encounter_date': entry.encounter_datetime_in,
            'patient_id': patient_id,
            'patient_name': entry.patient_name,
            'findings': findings
        }
        formatted.append(formatted_row)
    return formatted

# @xray_recorder.capture('surgeon_handler')
@lambda_logged
def provider_audit_handler(event, context):
    bucket_name = 'algernonsolutions-leech-prod'
    audit_data = {}
    id_source = event['id_source']
    provider_id = event['provider_id']
    start_date = event['start_date']
    end_date = event['end_date']
    driver = CredibleFrontEndDriver(id_source)
    tasks = [
        (f'provider_audit_encounters_{provider_id}', credible_fe_tasks.get_encounters_by_provider, (provider_id, start_date, end_date)),
        (f'provider_old_audit_encounters_{provider_id}', credible_fe_tasks.get_old_audit_encounters, (provider_id, bucket_name)),
        (f'provider_old_transfer_encounters_{provider_id}', credible_fe_tasks.get_old_audit_encounters, (provider_id, bucket_name)),
    ]
    for task in tasks:
        task_name = task[0]
        task_results = _run_task(driver, bucket_name, *task)
        audit_data[task_name] = task_results
    old_encounters = audit_data[f'provider_old_audit_encounters_{provider_id}']
    provider_encounters = audit_data[f'provider_audit_encounters_{provider_id}']
    old_transfer_encounters = audit_data[f'provider_old_transfer_encounters_{provider_id}']
    filtered = {x['Service ID']: x for x in provider_encounters}
    old_encounters.extend(old_transfer_encounters)
    for encounter in old_encounters:
        encounter_id = encounter['Service ID']
        if encounter_id not in filtered:
            filtered[encounter_id] = encounter
    rebuilt_encounters = [x for x in filtered.values()]
    daily_data = {'unapproved_data': [x for x in rebuilt_encounters if x['Staff ID'] == provider_id], 'encounter_data': rebuilt_encounters}
    audit_results = audit_tasks.audit_encounters(id_source, provider_id, daily_data, [])
    formatted = build_audit_results(audit_results)
    report_data = ReportData('audit', formatted)
    write_report_data(**{
        'report_name': f'provider_audit_{provider_id}',
        'id_source': 'PSI',
        'report_data': {'provider_audit': report_data}
    })
    return True
