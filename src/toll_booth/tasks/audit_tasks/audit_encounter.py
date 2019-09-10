import logging
import os

from algernon import rebuild_event
from botocore.exceptions import ClientError

from toll_booth.tasks import audit_tasks, s3_tasks


def _store_engine_data(bucket_name, id_source, task_name, report_data):
    file_key = s3_tasks.build_engine_file_key(task_name, id_source)
    s3_tasks.store_engine_data(bucket_name, file_key, report_data)


def audit_encounter(id_source, encounter, other_encounters, unapproved_encounters, hotwords, checkpoint=True):
    logging.info(f'preparing to audit encounter: {encounter}')
    if checkpoint is False:
        return _audit_encounter(id_source, encounter, other_encounters, unapproved_encounters, hotwords)
    bucket_name = os.environ['ALGERNON_BUCKET_NAME']
    task_name = f'audit_provider-{encounter["Staff ID"]}-{encounter["Service ID"]}'
    try:
        audit_results = s3_tasks.retrieve_stored_engine_data(bucket_name, id_source, task_name)
        return rebuild_event(audit_results)
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchKey':
            raise e
        audit_results = _audit_encounter(id_source, encounter, other_encounters, unapproved_encounters, hotwords)
        if audit_results:
            _store_engine_data(bucket_name, id_source, task_name, audit_results)
        return audit_results


def _audit_encounter(id_source, encounter, other_encounters, unapproved_encounters, hotwords):
    checks = [
        audit_tasks.check_duration,
        audit_tasks.check_auth,
        audit_tasks.check_encounter_spacing,
        audit_tasks.check_for_hotwords,
        audit_tasks.check_days_billing,
        audit_tasks.check_for_collisions,
        audit_tasks.check_for_clones
    ]
    results = []
    check_kwargs = {
        'id_source': id_source,
        'test_encounter': encounter,
        'other_encounters': other_encounters,
        'unapproved_encounters': unapproved_encounters,
        'hotwords': hotwords
    }
    for check in checks:
        logging.debug(f'running check: {check} for encounter: {encounter}')
        check_result = check(**check_kwargs)
        logging.debug(f'completed check: {check} for encounter: {encounter}')
        if check_result:
            if isinstance(check_result, list):
                results.extend(check_result)
                continue
            results.append(check_result)
    return encounter, results
