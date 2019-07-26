def get_providers(driver):
    search_data = {
        'emp_status_f': 'ACTIVE',
        'first_name': 1,
        'last_name': 1,
        'emp_id': 1,
        'profile_code': 1,
        'asgn_supervisors': 1,
        'asgn_supervisees': 1
    }
    return driver.process_advanced_search('Employees', search_data)
