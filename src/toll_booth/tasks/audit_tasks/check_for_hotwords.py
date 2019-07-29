from toll_booth.obj.inspector import InspectionFinding


def check_for_hotwords(**kwargs):
    hotwords = kwargs['clinical_hotwords']
    encounter = kwargs['test_encounter']
    results = []
    for word in hotwords:
        if word in encounter:
            results.append(word)
    if results:
        msg = f'clinical hotwords found in documentation'
        return InspectionFinding('clinical_hotwords', msg, {'hot_words': results})
    return []
