import os
import sys
import uuid
from datetime import datetime

import boto3
import dateutil
import rapidjson
from algernon import rebuild_event, ajson
from algernon.aws import StoredData

LEECH_MACHINE_ARN = 'arn:aws:states:us-east-1:322652498512:stateMachine:Leech'
CREDIBLE_MACHINE_ARN = 'arn:aws:states:us-east-1:322652498512:stateMachine:CredibleWorker'
POST_PROCESS_MACHINE_ARN = 'arn:aws:states:us-east-1:322652498512:stateMachine:PostProcessEncounter'


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


def _start_machine(sfn_client, machine_arn,  machine_input):
    response = sfn_client.start_execution(
        stateMachineArn=machine_arn,
        input=machine_input
    )
    return response['executionArn']


def _pull_encounter_status(id_source, encounter_id, table_resource=None):
    if not table_resource:
        session = boto3.session.Session(profile_name='prod')
        table_resource = session.resource('dynamodb').Table('Migration')
    response = table_resource.get_item(Key={'identifier': f'#{id_source}#Encounter#', 'id_value': encounter_id})
    return response.get('Item')


def _fix_missing_encounter(id_source, encounter_id, results, sfn_client):
    machine_input = rapidjson.dumps({
        'id_source': id_source,
        'identifier': f'#{id_source}#Encounter#',
        'object_type': 'Encounter',
        'id_values': [encounter_id],
        'results': results
    })
    return _start_machine(sfn_client, CREDIBLE_MACHINE_ARN, machine_input)


def _fix_unleeched_encounter(extraction_results, sfn_client):
    leech_input = _build_leech_input(extraction_results)
    machine_input = StoredData.from_object(uuid.uuid4(), leech_input, full_unpack=True)
    _start_machine(sfn_client, LEECH_MACHINE_ARN, ajson.dumps(machine_input))


def _fix_unprocessed_encounter(source_vertex, sfn_client):
    machine_input = _build_post_process_input(source_vertex)
    _start_machine(sfn_client, POST_PROCESS_MACHINE_ARN, ajson.dumps(machine_input))


def fix_encounter(id_source, encounter_id, override_step=None):
    session = boto3.session.Session(profile_name='prod')
    table = session.resource('dynamodb').Table('Migration')
    sfn_client = session.client('stepfunctions')
    encounter_status = _pull_encounter_status(id_source, encounter_id, table)
    if encounter_status is None:
        raise NotImplemented(f'can not spot fix encounter that has not been extracted')
    if 'extraction' not in encounter_status:
        raise NotImplemented(f'can not spot fix encounter that has not been extracted')
    if 'generate_source_vertex' not in encounter_status or override_step == 'generate_source_vertex':
        extraction_results = encounter_status['extraction']['stage_results']
        return _fix_unleeched_encounter(extraction_results, sfn_client)
    return


if __name__ == '__main__':
    if 'ALGERNON_BUCKET_NAME' not in os.environ:
        os.environ['ALGERNON_BUCKET_NAME'] = 'algernonsolutions-leech-prod'
    args = sys.argv
    running_id_source = args[1]
    running_encounter_id = int(args[2])
    try:
        running_override_step = args[3]
    except IndexError:
        running_override_step = None
    fix_encounter(running_id_source, running_encounter_id, running_override_step)
