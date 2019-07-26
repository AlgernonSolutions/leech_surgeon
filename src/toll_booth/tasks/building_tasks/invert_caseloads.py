def invert_caseloads(caseloads):
    inverted_caseloads = {}
    for team_name, team_caseload in caseloads.items():
        if team_name == 'unassigned':
            for client in team_caseload:
                inverted_caseloads[client['client_id']] = {'team': 'unassigned', 'csw': 'unassigned', 'emp_id': 0}
            continue
        for emp_id, employee in team_caseload.items():
            csw = f'{employee["last_name"]}, {employee["first_name"]}'
            for client in employee['caseload']:
                inverted_caseloads[client['client_id']] = {'team': team_name, 'csw': csw, 'emp_id': emp_id}
    return inverted_caseloads
