def get_tx_plans(driver, earliest_encounter_date, today):
    search_data = {
        'clientvisit_id': 1,
        'service_type': 1,
        'non_billable': 1,
        'consumer_name': 1,
        'staff_name': 1,
        'client_int_id': 1,
        'emp_int_id': 1,
        'visittype': 1,
        'orig_rate_amount': 1,
        'timein': 1,
        'data_dict_ids': [3, 4, 6, 70, 74, 83, 86, 87, 218, 641],
        'non_billable1': 3,
        'auth_number': 1,
        'location_code': 1,
        'recipient_code': 1,
        'visittype_id': 3
    }
    return driver.process_advanced_search('ClientVisit', search_data, earliest_encounter_date, today)
