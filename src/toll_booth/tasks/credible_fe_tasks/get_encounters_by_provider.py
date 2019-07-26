def get_encounters_by_provider(driver, provider_id):
    encounter_search_data = {
        'clientvisit_id': 1,
        'service_type': 1,
        'non_billable': 1,
        'consumer_name': 1,
        'staff_name': 1,
        'client_int_id': 1,
        'emp_int_id': 1,
        'non_billable1': 3,
        'visittype': 1,
        'orig_rate_amount': 1,
        'timein': 1,
        'wh_fld1': 'cv.by_emp_id',
        'wh_cmp1': '=',
        'wh_val1': provider_id,
        'data_dict_ids': [3, 4, 6, 70, 74, 83, 86, 87, 218, 641]
    }
    return driver.process_advanced_search('ClientVisit', encounter_search_data)
