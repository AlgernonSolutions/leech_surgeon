CREDIBLE_DATE_FORMAT = '%m/%d/%Y'


def get_encounters(driver, earliest_transfer_date):
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
        'wh_fld1': 'cv.transfer_date',
        'wh_cmp1': '>=',
        'wh_val1': earliest_transfer_date.strftime(CREDIBLE_DATE_FORMAT),
        'data_dict_ids': [3, 4, 6, 70, 74, 83, 86, 87, 218, 641]
    }
    return driver.process_advanced_search('ClientVisit', encounter_search_data)
