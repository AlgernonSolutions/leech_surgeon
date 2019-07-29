import logging
import os
import re
from decimal import Decimal

from algernon import rebuild_event
from algernon.aws import lambda_logged

from toll_booth.tasks import s3_tasks, audit_tasks
from toll_booth.tasks import building_tasks
from toll_booth.tasks.building_tasks.invert_caseloads import invert_caseloads


def _build_teams(id_source, daily_data):
    return building_tasks.build_clinical_teams(id_source, daily_data['emp_data'])


def _build_caseloads(teams, daily_data):
    return building_tasks.build_clinical_caseloads(teams, daily_data['client_data'])


def _build_encounters(daily_data):
    return [{
        'clientvisit_id': int(x['Service ID']),
        'rev_timeout': x['Service Date'],
        'transfer_date': x['Transfer Date'],
        'visit_type': x['Service Type'],
        'non_billable': x['Non Billable'] == 'True',
        'emp_id': int(x['Staff ID']),
        'client_id': int(x['Consumer ID']),
        'base_rate': Decimal(re.sub(r'[^\d.]', '', x['Base Rate'])),
        'data_dict_ids': 83
    } for x in daily_data['encounter_data']]


def _build_unapproved_data(daily_data):
    return [{
        'clientvisit_id': int(x['Service ID']),
        'rev_timeout': x['Service Date'],
        'visit_type': x['Service Type'],
        'non_billable': bool(x['Non Billable']),
        'emp_id': int(x['Staff ID']),
        'client_id': int(x['Consumer ID']),
        'red_x': x['Manual RedX Note'],
        'base_rate': Decimal(re.sub(r'[^\d.]', '', x['Base Rate']))
    } for x in daily_data['unapproved_data']]


def _build_tx_plan_data(daily_data):
    return [{
        'rev_timeout': x['Service Date'],
        'emp_id':  int(x['Staff ID']),
        'client_id': int(x['Consumer ID'])
    } for x in daily_data['tx_data']]


def _build_da_data(daily_data):
    return [{
        'rev_timeout': x['Service Date'],
        'emp_id': int(x['Staff ID']),
        'client_id': int(x['Consumer ID'])
    } for x in daily_data['da_data']]


def _build_audit_results(id_source, teams, caseloads, daily_data, old_encounters):
    audit_results = audit_tasks.audit_encounters(id_source, daily_data, old_encounters)
    patients = invert_caseloads(caseloads)
    inverted_teams = {}
    for team_name, team in teams.items():
        for entry in team:
            inverted_teams[str(entry['emp_id'])] = (team_name, f"{entry['last_name']}, {entry['first_name']}")
    return building_tasks.build_audit_results(inverted_teams, patients, audit_results)


def _build_30_60_90_report(caseloads, encounters):
    report = {}
    base_report = building_tasks.build_not_seen_report(caseloads, encounters)
    for entry in base_report:
        team_name = entry['team_name']
        csw_name = entry['csw_name']
        if team_name not in report:
            report[team_name] = {}
        if csw_name not in report[team_name]:
            report[team_name][csw_name] = []
        report[team_name][csw_name].append(entry)
    return report


def _build_productivity_report(caseloads, encounters, unapproved):
    report = {}
    for team_name, employees in caseloads.items():
        if team_name == 'unassigned':
            continue
        page_name = f'productivity_{team_name}'
        productivity_results = building_tasks.build_team_productivity(team_name, employees, encounters, unapproved)
        report[page_name] = productivity_results
    return report


def _store_final_product(bucket_name, id_source, engine_data):
    file_key = s3_tasks.build_engine_file_key('built_reports', id_source)
    if not s3_tasks.check_for_engine_data(bucket_name, file_key):
        s3_tasks.store_engine_data(bucket_name, file_key, engine_data)


@lambda_logged
def builder_handler(event, context):
    logging.info(f'received a call to the report_handler: {event}/{context}')
    id_source = event['id_source']
    daily_report = {}
    bucket_name = os.environ['LEECH_BUCKET']
    client_data_table_name = os.environ['CLIENT_DATA_TABLE_NAME']
    daily_data = s3_tasks.retrieve_stored_engine_data(bucket_name, id_source, 'daily_data')
    daily_data = rebuild_event(daily_data)
    old_encounters = s3_tasks.retrieve_stored_engine_data(bucket_name, id_source, 'old_encounters')
    old_encounters = rebuild_event(old_encounters)
    teams = _build_teams(id_source, daily_data)
    caseloads = _build_caseloads(teams, daily_data)
    encounters = _build_encounters(daily_data)
    unapproved = _build_unapproved_data(daily_data)
    tx_plans = _build_tx_plan_data(daily_data)
    diagnostics = _build_da_data(daily_data)
    productivity = _build_productivity_report(caseloads, encounters, unapproved)
    tx_report = building_tasks.build_expiration_report(caseloads, tx_plans, 180)
    da_report = building_tasks.build_expiration_report(caseloads, diagnostics, 365)
    thirty_sixty_ninety = _build_30_60_90_report(caseloads, encounters)
    unassigned_report = building_tasks.build_unassigned_report(caseloads)
    audit_results = _build_audit_results(id_source, teams, caseloads, daily_data, old_encounters)
    built_report = {
        'tx_report': tx_report,
        'da_report': da_report,
        '30_60_90': thirty_sixty_ninety,
        'unassigned': unassigned_report,
        'audit': audit_results,
        'productivity': productivity
    }
    _store_final_product(bucket_name, id_source, built_report)
