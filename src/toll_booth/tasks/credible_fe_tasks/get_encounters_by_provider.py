def get_encounters_by_provider(driver, provider_id, start_date, end_date):
    encounter_search_data = {
        'emp_id': provider_id,
        'clientvisit_id': 1,
        'service_type': 1,
        'non_billable': 1,
        'consumer_name': 1,
        'staff_name': 1,
        'client_int_id': 1,
        'emp_int_id': 1,
        'location_code': 1,
        'recipient_code': 1,
        'non_billable1': 3,
        'auth_number': 1,
        'visittype': 1,
        'orig_rate_amount': 1,
        'timein': 1,
        'wh_fld1': 'cv.transfer_date',
        'wh_cmp1': '>=',
        'wh_val1': start_date,
        'wh_fld2': 'cv.transfer_date',
        'wh_cmp2': '<=',
        'wh_val2': end_date,
        'wh_andor': 'AND',
        'data_dict_ids': [3, 4, 6, 70, 83, 86, 87, 218, 641]
        # 'data_dict_ids': [3, 4, 6, 70, 74, 83, 86, 87, 218, 641]
    }
    return driver.process_advanced_search('ClientVisit', encounter_search_data)
