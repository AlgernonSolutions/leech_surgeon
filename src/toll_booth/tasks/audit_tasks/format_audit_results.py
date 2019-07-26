def _structure_teams(teams):
    results = {}
    for supervisor_name, team in teams.items():
        for provider in team:
            provider_id = str(provider['emp_id'])
            provider_name = f"{provider['last_name']}, {provider['first_name']}"
            results[provider_id] = (supervisor_name, provider_name)
    return results


def _structure_patients(caseloads):
    patients = {}
    for team_name, providers in caseloads.items():
        if not providers or team_name == 'unassigned':
            continue
        for provider_id, provider in providers.items():
            for entry in provider['caseload']:
                patient_id = str(entry['client_id'])
                patient_name = f'{entry["last_name"].lower().capitalize()}, {entry["first_name"].lower().capitalize()}'
                patients[patient_id] = patient_name
    return patients


def format_audit_results(teams, patients, audit_results):
    header = ['Encounter ID', 'Provider ID', 'Provider Name', 'Patient ID', 'Patient Name', 'Audit Results']
    formatted = {}
    for entry in audit_results:
        encounter_id = str(entry.encounter_id)
        provider_id = str(entry.provider_id)
        patient_id = str(entry.patient_id)
        if provider_id not in teams:
            continue
        team_data = teams[provider_id]
        team_name = team_data[0]
        if team_name not in formatted:
            formatted[team_name] = [header]
        findings = 'nothing found'
        if entry.findings:
            findings = '\n'.join(str(x) for x in entry.findings)
        patient_name = patients.get(patient_id, 'unknown')
        formatted[team_name].append([encounter_id, provider_id, team_data[1], patient_id, patient_name, findings])
    return formatted
