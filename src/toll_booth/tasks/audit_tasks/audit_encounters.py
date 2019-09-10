import os
import uuid
from collections import deque
from datetime import datetime
from queue import Queue
from threading import Thread

import boto3
import dateutil
import rapidjson

from algernon import ajson, rebuild_event
from algernon.aws import StoredData
from botocore.exceptions import ClientError

from toll_booth.obj.hotwords import ClinicalHotwords
from toll_booth.obj.inspector import InspectionEncounterData, InspectionFindings
from toll_booth.tasks.audit_tasks.audit_encounter import audit_encounter
from multiprocessing.dummy import Pool as ThreadPool


def _generate_stored_results(results):
    stored_results = rapidjson.loads(ajson.dumps(StoredData.from_object(uuid.uuid4(), results, True)))
    return stored_results


def _check_migration_status(id_source, encounter_id):
    session = boto3.session.Session(profile_name='prod')
    table = session.resource('dynamodb').Table('Migration')
    response = table.get_item(Key={'identifier': f'#{id_source}#Encounter#', 'id_value': encounter_id})
    return encounter_id, response.get('Item')


def _start_missing_values(id_source, id_values, results):
    session = boto3.session.Session(profile_name='prod')
    client = session.client('stepfunctions')
    machine_input = rapidjson.dumps({
        'id_source': id_source,
        'identifier': f'#{id_source}#Encounter#',
        'object_type': 'Encounter',
        'id_values': id_values,
        'results': results
    })
    response = client.start_execution(
        stateMachineArn='arn:aws:states:us-east-1:322652498512:stateMachine:CredibleWorker',
        input=machine_input
    )
    return response


def _build_result_package(id_source, comm_supt_encounter_id, comm_supt_encounters):

    return [{
        'result': {
            'encounter_id': x['Service ID'],
            'id_source': id_source,
            'provider_id': x['Staff ID'],
            'patient_id': x['Consumer ID'],
            'encounter_type': x['Visit Type'],
            'encounter_datetime_in': x['Time In'],
            'encounter_datetime_out': x['Time Out'],
            'patient_last_name': x['Last Name'],
            'patient_first_name': x['First Name'],
            'patient_dob': x['DOB']
        },
        'id_value': x['Service ID']
    } for x in comm_supt_encounters if x['Service ID'] == comm_supt_encounter_id]


def _remedy_missing_values(id_source, missing_values, comm_supt_encounters):
    batch = []
    for missing_value in missing_values:
        batch.extend(_build_result_package(id_source, missing_value, comm_supt_encounters))
        if len(batch) > 99:
            _start_missing_values(id_source, [str(x['id_value']) for x in batch], _generate_stored_results(batch))
            batch = []
    if batch:
        _start_missing_values(id_source, [str(x['id_value']) for x in batch], _generate_stored_results(batch))


def start_leech_machine(client,  machine_input):
    response = client.start_execution(
        stateMachineArn='arn:aws:states:us-east-1:322652498512:stateMachine:Leech',
        input=machine_input
    )
    return response['executionArn']


def _remedy_unleeched_values(unleeched_encounters):
    session = boto3.session.Session(profile_name='prod')
    client = session.client('stepfunctions')
    for encounter in unleeched_encounters:
        extraction_results = encounter['extraction']['stage_results']
        leech_input = _build_leech_input(extraction_results)
        machine_input = StoredData.from_object(uuid.uuid4(), leech_input, full_unpack=True)
        start_leech_machine(client, ajson.dumps(machine_input))


def _remedy_unprocessed_value(unprocessed_encounters):
    session = boto3.session.Session(profile_name='prod')
    client = session.client('stepfunctions')
    for encounter in unprocessed_encounters:
        source_vertex = encounter['generate_source_vertex']['stage_results']
        machine_input = _build_post_process_input(source_vertex)
        _start_post_process_machine(client, ajson.dumps(machine_input))


def _build_leech_input(extraction_results):
    extraction_results = rebuild_event(rapidjson.loads(extraction_results))
    extracted_data = extraction_results['extracted_data']
    leech_input = {
        'object_type': extraction_results['object_type'],
        'identifier': extraction_results['identifier'],
        'id_value': extraction_results['id_value'],
        'extracted_data': {
            'source': {
                'id_source': extracted_data['source']['id_source'],
                'encounter_id': int(extracted_data['source']['encounter_id']),
                'provider_id': int(extracted_data['source']['provider_id']),
                'patient_id': int(extracted_data['source']['patient_id']),
                'encounter_type': extracted_data['source']['encounter_type'],
                'encounter_datetime_in': extracted_data['source']['encounter_datetime_in'].isoformat(),
                'encounter_datetime_out': extracted_data['source']['encounter_datetime_out'].isoformat(),
                'documentation': extracted_data['source']['documentation']
            },
            'patient_data': [
                {
                    'last_name': x['last_name'],
                    'first_name': x['first_name'],
                    'dob': x['dob'].isoformat() if isinstance(x['dob'], datetime) else dateutil.parser.parse(x['dob']).isoformat()
                } for x in extracted_data['patient_data']
            ]
        }
    }
    return leech_input


def _build_post_process_input(source_vertex):
    return {
        "version": "0",
        "id": "a62f7d20-776b-0caa-3bdd-9c00da558b15",
        "detail-type": "vertex_added",
        "source": "algernon",
        "account": "322652498512",
        "time": "2019-09-04T14:33:44Z",
        "region": "us-east-1",
        "resources": [],
        "detail": source_vertex
    }


def _start_post_process_machine(client, machine_input):
    machine_arn = 'arn:aws:states:us-east-1:322652498512:stateMachine:PostProcessEncounter'
    response = client.start_execution(
        stateMachineArn=machine_arn,
        input=machine_input
    )
    return response['executionArn']


def _verify_migration(id_source, comm_supt_encounters):
    comm_supt_ids = [int(x['Service ID']) for x in comm_supt_encounters]
    pool = ThreadPool(25)
    results = pool.starmap(_check_migration_status, [(id_source, x) for x in comm_supt_ids])
    pool.close()
    pool.join()
    missing_values = [x[0] for x in results if x[1] is None]
    started_values = [x for x in results if x[1]]
    unextracted_id_values = [x[0] for x in started_values if 'extraction' not in x[1]]
    unleeched_encounters = [x[1] for x in started_values if 'generate_source_vertex' not in x[1] and 'extraction' in x[1]]
    unprocessed_encounters = [x[1] for x in started_values if 'post_process' not in x[1] and 'generate_source_vertex' in x[1]]
    if missing_values:
        _remedy_missing_values(id_source, missing_values, comm_supt_encounters)
    if unextracted_id_values:
        _remedy_missing_values(id_source, unextracted_id_values, comm_supt_encounters)
    if unleeched_encounters:
        _remedy_unleeched_values(unleeched_encounters)
    if unprocessed_encounters:
        _remedy_unprocessed_value(unprocessed_encounters)
    return results


def _start_workers(target_fn, work, results, num_workers=25):
    workers = []
    for _ in range(num_workers):
        worker = Thread(target=target_fn, kwargs={'work': work, 'results': results})
        worker.start()
        workers.append(worker)
    return workers


def _shutdown_workers(workers, work):
    for _ in workers:
        work.put(None)
    for worker in workers:
        worker.join()


def _generate_stored_parameter(parameter):
    stored = StoredData.from_object(uuid.uuid4(), parameter, True)
    stored.store()
    return stored


def _send_messages(work, **kwargs):
    audit_queue_url = os.environ['AUDIT_QUEUE_URL']
    session = boto3.session.Session()
    sqs = session.resource('sqs')
    queue = sqs.Queue(audit_queue_url)
    batch = []
    while True:
        entry = work.get()
        if package is None:
            if batch:
                queue.send_messages(
                    Entries=[
                        {
                            'Id': str(x),
                            'MessageBody': ajson.dumps(y)
                        } for x, y in enumerate(batch)
                    ]
                )
            return
        batch.append(package)
        if len(batch) == 10:
            queue.send_messages(
                Entries=[
                    {
                        'Id': str(x),
                        'MessageBody': ajson.dumps(y)
                    } for x, y in enumerate(batch)
                ]
            )
            batch = []
        work.task_done()
        print(work.qsize())


def _retrieve_results(flow_id, encounter_id):
    bucket_name = os.environ['ALGERNON_BUCKET_NAME']
    session = boto3.session.Session()
    s3_resource = session.resource('s3')
    stored_object = s3_resource.Object(bucket_name, f'{flow_id}/{str(encounter_id)}')
    response = stored_object.get()
    serialized_results = response['Body'].read()
    results = ajson.loads(serialized_results)
    return results


def _package_and_send_encounter(id_source, flow_id, fn_id, encounter, other_encounters, unapproved_encounters, hotwords, queue):
    fn_payload = {
        'id_source': id_source,
        'encounter': _generate_stored_parameter(encounter),
        'other_encounters': other_encounters,
        'unapproved_encounters': unapproved_encounters,
        'clinical_hotwords': hotwords,
        'checkpoint': False
    }
    package = {
        'flow_id': flow_id,
        'fn_id': str(fn_id),
        'fn_name': 'provider-audit-prod-AuditEncounterH-1POVCQMTFP0EG',
        'fn_payload': ajson.dumps(fn_payload),
        'result_bucket': 'algernonsolutions-leech-prod'
    }
    queue.put(package)


def _run_encounter(flow_id, id_source, encounter, stored_other_encounters, stored_unapproved_encounters, stored_hotwords):
    encounter_id = encounter['Service ID']
    try:
        return _retrieve_results(flow_id, encounter_id)
    except ClientError as e:
        print(e)


def audit_encounters(id_source, provider_id, daily_data, old_encounters):
    auditor = EncounterAuditor.from_daily_data(id_source, provider_id, daily_data, old_encounters, False)
    auditor.run_audit()
    for entry in results_deque:
        encounter = entry[0]
        encounter_results = entry[1]
        encounter_id = encounter['Service ID']
        service_date = encounter['Service Date'].isoformat()
        provider_id = encounter['Staff ID']
        patient_id = encounter['Consumer ID']
        patient_name = f'{encounter["Last Name"]}, {encounter["First Name"]}'
        findings = InspectionFindings(
            id_source, encounter_id, service_date, provider_id, patient_id, patient_name, encounter_results)
        results.append(findings)
    return results


class EncounterAuditor:
    def __init__(self,
                 flow_id,
                 bucket_name,
                 id_source,
                 provider_id,
                 other_encounters,
                 unapproved_encounters,
                 hotwords,
                 use_checkpoints=True):
        self._flow_id = flow_id
        self._bucket_name = bucket_name
        self._id_source = id_source
        self._provider_id = provider_id
        self._other_encounters = other_encounters
        self._unapproved_encounters = unapproved_encounters
        self._hotwords = hotwords
        self._use_checkpoints = use_checkpoints
        self._stored = {}
        self._send_workers = []
        self._send_queue = Queue()
        self._results = deque()

    @classmethod
    def from_daily_data(cls, id_source, provider_id, daily_data, old_encounters, use_checkpoints=True):
        flow_id = f'audit_provider_{provider_id}_{datetime.utcnow().date().isoformat()}'
        unapproved_encounters = InspectionEncounterData.from_raw_encounters(daily_data['unapproved_data'])
        other_encounters = daily_data['encounter_data']
        other_encounters.extend(old_encounters)
        other_encounters = InspectionEncounterData.from_raw_encounters(other_encounters)
        bucket_name = os.environ['ALGERNON_BUCKET_NAME']
        hotwords = ClinicalHotwords.retrieve(id_source)
        return cls(
            flow_id, bucket_name, id_source, provider_id, other_encounters, unapproved_encounters, hotwords, use_checkpoints)

    @property
    def flow_id(self):
        return self._flow_id

    def _build_package(self, encounter):
        fn_id = encounter['Service ID']
        fn_payload = {
            'id_source': self._id_source,
            'encounter': _generate_stored_parameter(encounter),
            'checkpoint': self._use_checkpoints
        }
        fn_payload.update(self._stored)
        package = {
            'flow_id': self._flow_id,
            'fn_id': str(fn_id),
            'fn_name': 'provider-audit-prod-AuditEncounterH-1POVCQMTFP0EG',
            'fn_payload': ajson.dumps(fn_payload),
            'result_bucket': 'algernonsolutions-leech-prod'
        }
        return package

    def _start_workers(self, num_workers=25):
        for _ in range(num_workers):
            worker = Thread(target=_send_messages)
            worker.start()
            self._send_workers.append(worker)

    def _shutdown_workers(self):
        for _ in self._send_workers:
            self._send_queue.put(None)
        for worker in self._send_workers:
            worker.join()

    def _store_statics(self):
        self._stored.update({
            'other_encounters': _generate_stored_parameter(self._other_encounters),
            'unapproved_encounters': _generate_stored_parameter(self._unapproved_encounters),
            'hotwords': _generate_stored_parameter(self._hotwords)
        })

    def _send_message(self):
        audit_queue_url = os.environ['AUDIT_QUEUE_URL']
        session = boto3.session.Session()
        sqs = session.resource('sqs')
        queue = sqs.Queue(audit_queue_url)
        batch = []
        while True:
            entry = self._send_queue.get()
            if entry is None:
                if batch:
                    queue.send_messages(
                        Entries=[
                            {
                                'Id': str(x),
                                'MessageBody': ajson.dumps(y)
                            } for x, y in enumerate(batch)
                        ]
                    )
                return
            package = self._build_package(entry)
            batch.append(package)
            if len(batch) == 10:
                queue.send_messages(
                    Entries=[
                        {
                            'Id': str(x),
                            'MessageBody': ajson.dumps(y)
                        } for x, y in enumerate(batch)
                    ]
                )
                batch = []
            self._send_queue.task_done()
            print(self._send_queue.qsize())

    def _start_whisperer(self, task_pieces):
        client = boto3.client('stepfunctions')
        machine_input = {
            'flow_id': self._flow_id,
            'timeout': 3600,
            'task_pieces': _generate_stored_parameter(task_pieces)
        }
        client.start_execution(
            stateMachineArn=os.environ['WHISPER_MACHINE_ARN'],
            input=ajson.dumps(machine_input)
        )

    def run_audit(self):
        pool = ThreadPool(25)
        unapproved_commsupt = [x for x in self._unapproved_encounters if x['Service Type'] == 'CommSupp']
        self._store_statics()
        task_pieces = pool.map(self._build_package, unapproved_commsupt)
        pool.close()
        pool.join()
        self._start_whisperer(task_pieces)
        #self._start_workers()
        #for encounter in unapproved_commsupt:
        #    self._send_queue.put(encounter)
        #self._shutdown_workers()
