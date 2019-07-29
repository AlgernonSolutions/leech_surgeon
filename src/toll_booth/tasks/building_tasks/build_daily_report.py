import re
from decimal import Decimal

from toll_booth.tasks.building_tasks.build_expiration_report import build_expiration_report
from toll_booth.tasks.building_tasks.build_not_seen_report import build_not_seen_report
from toll_booth.tasks.building_tasks.build_team_productivity import build_team_productivity
from toll_booth.tasks.building_tasks.build_unassigned_report import build_unassigned_report


def _format_encounters(encounter_data):
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
    } for x in encounter_data]


def _format_unapproved_data(unapproved_data):
    return [{
        'clientvisit_id': int(x['Service ID']),
        'rev_timeout': x['Service Date'],
        'visit_type': x['Service Type'],
        'non_billable': bool(x['Non Billable']),
        'emp_id': int(x['Staff ID']),
        'client_id': int(x['Consumer ID']),
        'red_x': x['Manual RedX Note'],
        'base_rate': Decimal(re.sub(r'[^\d.]', '', x['Base Rate']))
    } for x in unapproved_data]


def _format_tx_plan_data(tx_plan_data):
    return [{
        'rev_timeout': x['Service Date'],
        'emp_id':  int(x['Staff ID']),
        'client_id': int(x['Consumer ID'])
    } for x in tx_plan_data]


def _format_da_data(da_data):
    return [{
        'rev_timeout': x['Service Date'],
        'emp_id': int(x['Staff ID']),
        'client_id': int(x['Consumer ID'])
    } for x in da_data]


def build_daily_report(**kwargs):
    daily_report = {}
    encounters = _format_encounters(kwargs['encounter_data'])
    unapproved = _format_unapproved_data(kwargs['unapproved_data'])
    tx_plans = _format_tx_plan_data(kwargs['tx_data'])
    diagnostics = _format_da_data(kwargs['da_data'])
    caseloads = kwargs['caseloads']
    for team_name, employees in caseloads.items():
        if team_name == 'unassigned':
            continue
        page_name = f'productivity_{team_name}'
        productivity_results = build_team_productivity(employees, encounters, unapproved)
        daily_report[page_name] = productivity_results
    tx_report = build_expiration_report(caseloads, tx_plans, 180)
    da_report = build_expiration_report(caseloads, diagnostics, 180)
    thirty_sixty_ninety = build_not_seen_report(caseloads, encounters)
    unassigned_report = build_unassigned_report(caseloads)
    daily_report['tx_plans'] = tx_report
    daily_report['diagnostics'] = da_report
    daily_report['unassigned'] = unassigned_report
    daily_report['30, 60, 90'] = thirty_sixty_ninety
    daily_report['caseloads'] = caseloads
    return {'report_data': daily_report}
