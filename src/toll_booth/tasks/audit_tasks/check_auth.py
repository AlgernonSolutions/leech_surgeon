from toll_booth.obj.inspector import InspectionFinding


def check_auth(**kwargs):
    test_encounter = kwargs['test_encounter']
    auth_id = test_encounter.get('AuthID')
    auth_exceeded = test_encounter.get('Auth Exceeded', 'False') == 'True'
    if not auth_id:
        return InspectionFinding('missing_auth', 'encounter does not have applicable auth')
    if auth_exceeded:
        return InspectionFinding('exceeded_auth', 'encounter has exceeded the provided auth', {'auth_id': auth_id})