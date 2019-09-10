from datetime import datetime

from algernon import rebuild_event

from toll_booth.tasks import s3_tasks


def get_old_audit_encounters(driver, provider_id, bucket_name):
    id_source = driver.id_source
    encounters = []
    data_name = f'provider_audit_encounters_{provider_id}'
    stored_data = s3_tasks.retrieve_stored_engine_data(bucket_name, id_source, data_name)
    provider_encounters = rebuild_event(stored_data)
    unapproved_commsupt = [x for x in provider_encounters if x['Service Type'] == 'CommSupp']
    old_commsupt_dates = set(
        x['Service Date'].timestamp() for x in unapproved_commsupt)

    for encounter_date in [datetime.fromtimestamp(x) for x in old_commsupt_dates]:
        encounter_search_data = {
            'clientvisit_id': 1,
            'service_type': 1,
            'non_billable': 3,
            'consumer_name': 1,
            'staff_name': 1,
            'client_int_id': 1,
            'emp_int_id': 1,
            'visittype': 1,
            'orig_rate_amount': 1,
            'timein': 1,
            'data_dict_ids': [3, 4, 6, 70, 83, 86, 87, 218, 641]
        }
        search_args = ('ClientVisit', encounter_search_data, encounter_date, encounter_date)
        same_day_encounters = driver.process_advanced_search(*search_args)
        encounters.extend(same_day_encounters)
    return encounters
