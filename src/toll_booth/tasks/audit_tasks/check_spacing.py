from toll_booth.obj.inspector import InspectionFinding


def _check_spacing(check_type, test_encounter_id, other_encounters):
    results = []
    threshold = 60 * 17
    other_encounters = sorted(other_encounters, key=lambda x: x['Time In'])
    for pointer, encounter in enumerate(other_encounters):
        encounter_id = encounter['Service ID']
        if test_encounter_id == encounter_id:
            test_time_in = encounter['Time In']
            test_time_out = encounter['Time Out']
            previous_encounter = None
            next_encounter = None
            if pointer != 0:
                previous_encounter = other_encounters[pointer-1]
            if pointer+1 != len(other_encounters):
                next_encounter = other_encounters[pointer+1]
            if previous_encounter:
                previous_encounter_time_out = previous_encounter['Time Out']
                spacing = test_time_in - previous_encounter_time_out
                if spacing.seconds <= threshold:
                    previous_encounter_id = previous_encounter['Service ID']
                    msg = f'previous encounter for {check_type} ended too soon to the start of this encounter'
                    inspection_name = f'encounter_crowding_{check_type}_prior'
                    inspection_details = {'previous_encounter_id': previous_encounter_id}
                    results.append(InspectionFinding(inspection_name, msg, inspection_details))
            if next_encounter:
                next_encounter_time_in = next_encounter['Time In']
                spacing = next_encounter_time_in - test_time_out
                if spacing.seconds <= threshold:
                    next_encounter_id = next_encounter['Service ID']
                    msg = f'next encounter for {check_type} begins too soon to the end of this encounter'
                    inspection_name = f'encounter_crowding_{check_type}_next'
                    inspection_details = {'next_encounter_id': next_encounter_id}
                    results.append(InspectionFinding(inspection_name, msg,inspection_details))
    return results


def check_encounter_spacing(**kwargs):
    test_encounter = kwargs['test_encounter']
    other_encounters = kwargs['other_encounters']
    unapproved = kwargs['unapproved_encounters']
    encounter_date = test_encounter['Service Date']
    provider_id = test_encounter['Staff ID']
    patient_id = test_encounter['Consumer ID']
    try:
        same_day = other_encounters.get_same_day_encounters_by_date(encounter_date)
    except KeyError:
        same_day = []
    unapproved_same_day = unapproved.get_same_day_encounters_by_date(encounter_date)
    provider_same_day = [x for x in same_day if x['Staff ID'] == provider_id]
    patient_same_day = [x for x in same_day if x['Consumer ID'] == patient_id]
    unapproved_provider_same_day = [x for x in unapproved_same_day if x['Staff ID'] == provider_id]
    unapproved_patient_same_day = [x for x in unapproved_same_day if x['Consumer ID'] == patient_id]
    provider_same_day.extend(unapproved_provider_same_day)
    patient_same_day.extend(unapproved_patient_same_day)
    patient_check = _check_spacing('patient', test_encounter['Service ID'], patient_same_day)
    provider_check = _check_spacing('provider', test_encounter['Service ID'], provider_same_day)
    patient_check.extend(provider_check)
    return patient_check
