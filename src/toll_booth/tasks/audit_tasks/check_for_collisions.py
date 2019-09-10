from datetime import datetime

from toll_booth.obj.inspector import InspectionFinding


def check_for_collisions(**kwargs):
    test_encounter = kwargs['test_encounter']
    transfer_date = test_encounter['Transfer Date']
    transfer_day = datetime(transfer_date.year, transfer_date.month, transfer_date.day).isoformat()
    other_encounters = kwargs['other_encounters']
    days_encounters = other_encounters.encounters_by_date.get(transfer_day, [])
    provider_encounters = [x for x in days_encounters
                           if x['Staff ID'] == test_encounter['Staff ID'] and x['Non Billable'] == 'False']
    collisions = [x for x in provider_encounters if x['Time In'] <= transfer_date <= x['Time Out']]
    if collisions:
        return [
            InspectionFinding(
                'transfer_collision',
                'encounter was transferred into the system during another billable service',
                {
                    'transfer_date_time': transfer_date.isoformat(),
                    'collided_encounter_id': x['Service ID'],
                    'collided_encounter_time_in': x['Time In'],
                    'collided_encounter_time_out': x['Time Out'],
                }
            ) for x in collisions
        ]
