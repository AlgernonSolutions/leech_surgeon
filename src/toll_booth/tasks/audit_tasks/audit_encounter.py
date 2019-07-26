from toll_booth.tasks.audit_tasks.check_auth import check_auth
from toll_booth.tasks.audit_tasks.check_duration import check_duration
from toll_booth.tasks.audit_tasks.check_spacing import check_encounter_spacing


def audit_encounter(encounter, other_encounters, unapproved_encounters):
    checks = [check_duration, check_auth, check_encounter_spacing]
    results = []
    check_kwargs = {
        'test_encounter': encounter,
        'other_encounters': other_encounters,
        'unapproved_encounters': unapproved_encounters
    }
    for check in checks:
        check_result = check(**check_kwargs)
        if check_result:
            if isinstance(check_result, list):
                results.extend(check_result)
                continue
            results.append(check_result)
    return results
