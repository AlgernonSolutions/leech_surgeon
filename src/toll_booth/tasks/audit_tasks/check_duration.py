from toll_booth.obj.inspector import InspectionFinding


def check_duration(**kwargs):
    test_encounter = kwargs['test_encounter']
    time_in = test_encounter['Time In']
    time_out = test_encounter['Time Out']
    duration = time_out - time_in
    threshold = 3600 * 1.6
    if duration.seconds >= threshold:
        return InspectionFinding(
            'duration', 'encounter duration is greater than normal', {'encounter_duration': str(duration)})
