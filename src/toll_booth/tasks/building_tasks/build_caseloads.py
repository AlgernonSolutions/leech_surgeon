def _parse_staff_names(primary_staff_line):
    import re

    staff = []
    if not primary_staff_line:
        return staff
    program_pattern = r'(\([^)]*\))'
    program_re = re.compile(program_pattern)
    staff_names = primary_staff_line.split(', ')
    for name in staff_names:
        program_match = program_re.search(name)
        if program_match:
            program_name = program_match.group(1)
            name = name.replace(program_name, '')
        staff.append(name)
    return staff


def build_clinical_caseloads(teams, client_data):
    caseloads = {}
    unassigned = []
    name_lookup = {}
    for team_name, employees in teams.items():
        if team_name not in caseloads:
            caseloads[team_name] = {}
        for employee in employees:
            emp_id = str(employee['emp_id'])
            last_name = employee['last_name']
            first_name = employee['first_name']
            list_name = f'{first_name[0]} {last_name}'
            name_lookup[list_name] = emp_id
            if emp_id not in caseloads[team_name]:
                caseloads[team_name][emp_id] = employee
    for client in client_data:
        client_id = client[' Id']
        primary_assigned = client['Primary Staff']
        if not primary_assigned:
            unassigned.append({
                'client_id': client_id,
                'last_name': client['Last Name'],
                'first_name': client['First Name'],
                'medicaid_id': client['Medicaid ID'],
                'dob': client['DOB'],
                'ssn': client['SSN'],
                'team': client['CSA (Team)']
            })
            continue
        primary_names = _parse_staff_names(primary_assigned)
        client_record = {
            'client_id': client_id,
            'last_name': client['Last Name'],
            'first_name': client['First Name'],
            'medicaid_id': client['Medicaid ID'],
            'dob': client['DOB'],
            'ssn': client['SSN'],
            'primary_staff': primary_names
        }
        found = False
        for staff_name in primary_names:
            found_emp_id = name_lookup.get(staff_name)
            if found_emp_id:
                for team_name, employees in caseloads.items():
                    for emp_id, employee in employees.items():
                        if emp_id == found_emp_id:
                            employee['caseload'].append(client_record)
                            found = True
        if not found:
            unassigned.append({
                'client_id': client_id,
                'last_name': client['Last Name'],
                'first_name': client['First Name'],
                'medicaid_id': client['Medicaid ID'],
                'dob': client['DOB'],
                'ssn': client['SSN'],
                'team': client['CSA (Team)'],
                'primary_staff': primary_assigned
            })
    client_ids = set()
    for team_name, team in caseloads.items():
        for emp_id, employee in team.items():
            client_ids.update([x['client_id'] for x in employee['caseload']])
    client_ids.update([x['client_id'] for x in unassigned])
    if client_ids - set([str(x[' Id']) for x in client_data]):
        raise RuntimeError('while creating caseloads, we seemed to have missed someone, '
                           'can not continue due to prime directive')
    caseloads['unassigned'] = unassigned
    return caseloads
