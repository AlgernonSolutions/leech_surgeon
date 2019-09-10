from toll_booth.obj.inspector import InspectionFinding


def check_days_billing(**kwargs):
    test_encounter = kwargs['test_encounter']
    provider_id = test_encounter['Staff ID']
    service_date = test_encounter['Service Date'].isoformat()
    other_encounters = kwargs['other_encounters']
    days_encounters = other_encounters.encounters_by_date[service_date]
    provider_encounters = [x for x in days_encounters
                           if x['Staff ID'] == provider_id and x['Non Billable'] == 'False']
    day_total_seconds = sum([(x['Time Out'] - x['Time In']).seconds for x in provider_encounters])
    day_total_hours = day_total_seconds/3600
    if day_total_hours > 8:
        return InspectionFinding(
            'high_daily_billing', 'encounter has unusually high billing on the same day, >=8 hours',
            {'total_hours': day_total_hours})