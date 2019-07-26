import os

import boto3
import rapidjson

from algernon.serializers import ExplosionJson


def _retrieve_team_data(id_source):
    client_data_table_name = os.environ['CLIENT_DATA_TABLE_NAME']
    resource = boto3.resource('dynamodb')
    table = resource.Table(client_data_table_name)
    response = table.get_item(Key={'identifier_stem': '#supervisors#', 'sid_value': id_source})
    return ExplosionJson.loads(rapidjson.dumps(response['Item']))


def build_clinical_teams(id_source, emp_data):
    team_json = _retrieve_team_data(id_source)
    teams = team_json['teams']
    manual_assignments = team_json['manual_assignments']
    first_level = team_json['first_level']
    default_team = team_json['default_team']

    for entry in emp_data:
        int_emp_id = int(entry['Employee ID'])
        str_emp_id = str(int_emp_id)
        supervisor_names = entry['Supervisors']
        profile_code = entry['profile_code']
        if supervisor_names is None or profile_code != 'CSA Community Support Worker NonLicensed':
            continue
        emp_record = {
            'emp_id': int_emp_id,
            'first_name': entry['First Name'],
            'last_name': entry['Last Name'],
            'profile_code': entry['profile_code'],
            'caseload': []
        }
        if str_emp_id in manual_assignments:
            teams[manual_assignments[str_emp_id]].append(emp_record)
            continue
        for name in first_level:
            if name in supervisor_names:
                teams[name].append(emp_record)
                break
        else:
            teams[default_team].append(emp_record)
    return teams
