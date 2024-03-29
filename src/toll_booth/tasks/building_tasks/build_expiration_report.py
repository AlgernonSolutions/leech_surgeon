from datetime import timedelta, datetime

from toll_booth.tasks.building_tasks.invert_caseloads import invert_caseloads


def _search_client_data(client_data, client_id, default=None):
    for entry in client_data:
        if entry[' Id'] == client_id:
            return entry
    if default:
        return default
    raise RuntimeError(f'could not find client record for {client_id}')


def build_expiration_report(caseloads, assessment_data, client_data, assessment_lifespan):
    lifespan_delta = timedelta(days=assessment_lifespan)
    inverted = invert_caseloads(caseloads)
    now = datetime.now()
    max_assessments = {}
    results = {}
    # collect all the assessments for each client under the client_id
    for assessment in assessment_data:
        client_id = str(assessment['client_id'])
        if client_id not in max_assessments:
            max_assessments[client_id] = []
        max_assessments[client_id].append(assessment['rev_timeout'])
    for client_id, assessments in max_assessments.items():
        assignments = inverted.get(client_id, {'team': 'unassigned', 'csw': 'unassigned'})
        team_name, csw_name = assignments['team'], assignments['csw']
        max_assessment_date = max(assessments)
        expiration_date = max_assessment_date + lifespan_delta
        expired = False
        days_left = (expiration_date - now).days
        if expiration_date < now:
            expired = True
            days_left = 0
        if team_name not in results:
            results[team_name] = {}
        if csw_name not in results[team_name]:
            results[team_name][csw_name] = []
        try:
            client_name = f'{assignments["last_name"]}, {assignments["first_name"]}'
        except KeyError:
            default = {'last_name': 'UNKNOWN', 'first_name': 'UNKNOWN'}
            client_data_entry = _search_client_data(client_data, client_id, default)
            client_name = f'{client_data_entry["last_name"]}, {client_data_entry["first_name"]}'
        results[team_name][csw_name].append({
            'team_name': team_name,
            'csw_name': csw_name,
            'client_id': client_id,
            'client_name': client_name,
            'start_date': max_assessment_date,
            'end_date': expiration_date,
            'is_expired': expired,
            'days_left': days_left
        })
    no_assessments = set(inverted.keys()) - set(max_assessments.keys())
    for client_id in no_assessments:
        assignments = inverted.get(client_id, {'team': 'unassigned', 'csw': 'unassigned'})
        team_name, csw_name = assignments['team'], assignments['csw']
        if team_name not in results:
            results[team_name] = {}
        if csw_name not in results[team_name]:
            results[team_name][csw_name] = []
        results[team_name][csw_name].append({
            'team_name': team_name,
            'csw_name': csw_name,
            'client_id': client_id,
            'client_name': f'{assignments["last_name"]}, {assignments["first_name"]}',
            'start_date': None,
            'end_date': None,
            'is_expired': True,
            'days_left': 0
        })
    return results
