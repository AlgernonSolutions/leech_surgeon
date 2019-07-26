import logging
import os
from datetime import datetime, timedelta

from algernon.aws import lambda_logged
from botocore.exceptions import ClientError

from toll_booth.obj import CredibleFrontEndDriver
from toll_booth.tasks import credible_fe_tasks
from toll_booth.tasks import s3_tasks


def _run_task(driver, bucket_name, task_name, task_fn, task_args):
    id_source = driver.id_source
    try:
        report_data = s3_tasks.retrieve_stored_engine_data(bucket_name, id_source, task_name)
        return report_data
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


def _store_final_product(bucket_name, id_source, engine_data):
    file_key = s3_tasks.build_engine_file_key('daily_data', id_source)
    if not s3_tasks.check_for_engine_data(bucket_name, file_key):
        s3_tasks.store_engine_data(bucket_name, file_key, engine_data)


@lambda_logged
def engine_handler(event, context):
    logging.info(f'received a call to run an engine task: {event}/{context}')
    id_source = event['id_source']
    engine_data = {}
    logging.info(f'started a cycle of the surgical engine for id_source: {id_source}')
    bucket_name = os.environ['LEECH_BUCKET']
    driver = CredibleFrontEndDriver(id_source)
    today = datetime.utcnow()
    ninety_days_ago = today - timedelta(days=90)
    one_year_ago = today - timedelta(days=365)
    tasks = [
        ('emp_data', credible_fe_tasks.get_providers, ()),
        ('client_data', credible_fe_tasks.get_patients, ()),
        ('encounter_data', credible_fe_tasks.get_encounters, (ninety_days_ago,)),
        ('unapproved_data', credible_fe_tasks.get_unapproved_encounters, (one_year_ago, today)),
        ('tx_data', credible_fe_tasks.get_tx_plans, (one_year_ago, today)),
        ('da_data', credible_fe_tasks.get_diagnostics, (one_year_ago, today)),
        ('old_encounters', credible_fe_tasks.get_old_encounters, (bucket_name, today))
    ]
    for task in tasks:
        task_name = task[0]
        task_results = _run_task(driver, bucket_name, *task)
        engine_data[task_name] = task_results
    _store_final_product(bucket_name, id_source, engine_data)
    logging.info(f'completed a call to run an engine task: {event}/{engine_data}')
    return True
