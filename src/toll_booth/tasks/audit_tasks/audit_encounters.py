from toll_booth.obj.inspector import InspectionEncounterData, InspectionFindings
from toll_booth.tasks.audit_tasks.audit_encounter import audit_encounter


def audit_encounters(id_source, daily_data, old_encounters):
    results = []
    unapproved_encounters = InspectionEncounterData.from_raw_encounters(daily_data['unapproved_data'])
    other_encounters = daily_data['encounter_data']
    unapproved_commsupt = [x for x in unapproved_encounters if x['Service Type'] == 'CommSupp']
    other_encounters.extend(old_encounters)
    other_encounters = InspectionEncounterData.from_raw_encounters(other_encounters)
    for encounter in unapproved_commsupt:
        encounter_id = encounter['Service ID']
        encounter_results = audit_encounter(encounter, other_encounters, unapproved_encounters)
        provider_id = encounter['Staff ID']
        patient_id = encounter['Consumer ID']
        findings = InspectionFindings(id_source, encounter_id, provider_id, patient_id, encounter_results)
        results.append(findings)
    return results
