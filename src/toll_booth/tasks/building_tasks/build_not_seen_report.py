from datetime import datetime

from toll_booth.tasks.building_tasks.invert_caseloads import invert_caseloads


def _calculate_thirty_sixty_ninety(today, max_encounter_date):
    encounter_age = (today - max_encounter_date).days
    if encounter_age <= 30:
        return '30'
    if encounter_age <= 60:
        return '60'
    return '90'


def build_not_seen_report(caseloads, encounter_data):
    today = datetime.now()
    results = []
    inverted = invert_caseloads(caseloads)
    for client_id, assignments in inverted.items():
        team = assignments['team']
        if team == 'unassigned':
            continue
        csw_id = assignments['emp_id']
        csw_name = assignments['csw']
        client_encounters = [
            x for x in encounter_data if int(x['client_id']) == int(client_id) and x['non_billable'] is False]
        if not client_encounters:
            results.append({
                'team_name': team,
                'csw_name': csw_name,
                'client_id': client_id,
                'client_name': f'{assignments["last_name"]}, {assignments["first_name"]}',
                'last_service_by_csw': '?',
                'last_bill_service': '?',
                '30_60_90_by_csw': '90',
                '30_60_90_by_last_billed': '90'
            })
            continue
        max_encounter_date = max([x['rev_timeout'] for x in client_encounters])
        per_billable = _calculate_thirty_sixty_ninety(today, max_encounter_date)
        csw_encounters = [x for x in client_encounters if int(x['emp_id']) == int(csw_id)]
        if not csw_encounters:
            results.append({
                'team_name': team,
                'csw_name': csw_name,
                'client_id': client_id,
                'client_name': f'{assignments["last_name"]}, {assignments["first_name"]}',
                'last_service_by_csw': '?',
                'last_bill_service': max_encounter_date,
                '30_60_90_by_csw': '90',
                '30_60_90_by_last_billed': per_billable
            })
            continue
        max_csw_date = max([x['rev_timeout'] for x in csw_encounters])
        per_csw = _calculate_thirty_sixty_ninety(today, max_csw_date)
        results.append({
            'team_name': team,
            'csw_name': csw_name,
            'client_id': client_id,
            'client_name': f'{assignments["last_name"]}, {assignments["first_name"]}',
            'last_service_by_csw': max_csw_date,
            'last_bill_service': max_encounter_date,
            '30_60_90_by_csw': per_csw,
            '30_60_90_by_last_billed': per_billable
        })
    filtered_results = [x for x in results if x['30_60_90_by_csw'] != '30']
    return filtered_results
