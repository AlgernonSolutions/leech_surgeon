from datetime import datetime, timedelta


def build_team_productivity(team_caseload, encounters, unapproved):
    results = []
    twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
    six_days_ago = datetime.now() - timedelta(days=6)
    past_day_encounters = [x for x in encounters if x['transfer_date'] >= twenty_four_hours_ago]
    next_six_days_encounters = [x for x in encounters if all(
        [x['transfer_date'] < twenty_four_hours_ago, x['transfer_date'] >= six_days_ago]
    )]
    for emp_id, employee in team_caseload.items():
        emp_id = int(emp_id)
        emp_past_day_encounters = [x for x in past_day_encounters if x['emp_id'] == emp_id]
        emp_next_six_days_encounters = [x for x in next_six_days_encounters if x['emp_id'] == emp_id]
        emp_red_x = [x for x in unapproved if x['emp_id'] == emp_id and x['red_x']]
        emp_unapproved = [x for x in unapproved if x['emp_id'] == emp_id and not x['red_x']]
        emp_productivity = {
            'csw_id': emp_id,
            'csw_name':  f'{employee["last_name"]}, {employee["first_name"]}',
            'past_one_day':  {
                'total': sum([x['base_rate'] for x in emp_past_day_encounters]),
                'detailed': emp_past_day_encounters
            },
            'past_six_days': {
                'total': sum([x['base_rate'] for x in emp_next_six_days_encounters]),
                'detailed': emp_next_six_days_encounters
            },
            'unapproved': {
                'total': sum([x['base_rate'] for x in emp_unapproved]),
                'detailed': emp_unapproved
            },
            'red_x': {
                'total': sum([x['base_rate'] for x in emp_red_x]),
                'detailed': emp_red_x
            }
        }
        results.append(emp_productivity)
    return results
