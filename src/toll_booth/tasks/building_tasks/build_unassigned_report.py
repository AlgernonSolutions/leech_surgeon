def build_unassigned_report(caseloads):
    report = []
    for client in caseloads['unassigned']:
        primary_staff = client.get('primary_staff')
        if primary_staff:
            if isinstance(primary_staff, list):
                primary_staff = ', '.join(primary_staff)
        report.append({
            'client_id': client['client_id'],
            'client_name': f'{client["last_name"]}, {client["first_name"]}',
            'dob': client['dob'],
            'ssn': client['ssn'],
            'assigned_csa': client['team'],
            'primary_staff': primary_staff
        })
    return report
