import calendar
import re
from datetime import datetime

import bs4

from toll_booth.obj import CredibleFrontEndDriver
from toll_booth.tasks.credible_fe_tasks.get_client_attachments import get_client_attachments


def _get_post_intake_services(driver, patient_id, intake_date):
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
        'client_id': patient_id,
        'timein': 1,
        'ename': 1,
        'show_unappr': 1,
        'appr': 1,
        'appr_user': 1,
        'appr_date': 1
    }
    post_intake_services = driver.process_advanced_search('ClientVisit', encounter_search_data, intake_date)
    return post_intake_services


def _parse_documents(documents):
    returned = {}
    top_page = '<title>ConsumerService Multi-View</title>'
    page_separator = "<p class='page'>"
    encounter_pattern = re.compile(
        r'(Consumer)(?P<space>(&nbsp;)|(\s*))(Service)(?P=space)(ID:)((?P=space)*)(?P<encounter_id>\d*)')
    encounter_documents = documents.split(page_separator)
    for encounter_document in encounter_documents:
        if top_page in encounter_document:
            continue
        matches = encounter_pattern.search(encounter_document)
        encounter_id = matches.group('encounter_id')
        returned[encounter_id] = encounter_document
    return returned


def _find_strung_value(strung_values, value_name, offset=1):
    for pointer, entry in enumerate(strung_values):
        if entry == value_name:
            return strung_values[pointer + offset]


def _get_intake_patients(driver, month_start, month_end):
    id_source = driver.id_source
    encounter_search_data = {
        'clientvisit_id': 1,
        'service_type': 1,
        'non_billable': 1,
        'consumer_name': 1,
        'staff_name': 1,
        'client_name': 1,
        'client_int_id': 1,
        'emp_int_id': 1,
        'non_billable1': 3,
        'visittype': 1,
        'visittype_id': 58,
        'timein': 1,
        'team_name': 1
    }
    intake_encounter_data = driver.process_advanced_search('ClientVisit', encounter_search_data, month_start, month_end)
    intake_encounters = {str(x['Service ID']): x for x in intake_encounter_data}
    intake_docs = driver.retrieve_client_encounters(intake_encounters.keys())
    parsed = _parse_documents(intake_docs)
    enrolled_patients = []
    for encounter_id, intake_doc in parsed.items():
        soup = bs4.BeautifulSoup(intake_doc, "lxml")
        strung = [x for x in soup.stripped_strings]
        intake_type = _find_strung_value(strung, 'Select Type of Request:')
        if intake_type == 'Enrollment':
            new_csa = _find_strung_value(strung, 'CSA they are enrolling in below:')
            if new_csa == id_source:
                encounter_data = intake_encounters[encounter_id]
                enrolled_patients.append({
                    'patient_id': encounter_data['Consumer ID'],
                    'patient_name': encounter_data['ConsumerName'],
                    'intake_date': encounter_data['Service Date']
                })
        if intake_type == 'CSA Transfer':
            transferred_csa = _find_strung_value(strung, 'CSA they are enrolling in below:')
            if transferred_csa == id_source:
                encounter_data = intake_encounters[encounter_id]
                enrolled_patients.append({
                    'patient_id': encounter_data['Consumer ID'],
                    'patient_name': encounter_data['ConsumerName'],
                    'intake_date': encounter_data['Service Date']
                })
    return enrolled_patients


def _check_consents(post_intake_events):
    services = [x['Service Type'] for x in post_intake_events]
    return {
        'tx_consent': 'Consent' in services,
        'med_consent': 'InMdsConsA' in services or 'MedConsChi' in services
    }


def _check_for_attached_assessment(assessment_name, patient_attachments, intake_date):
    attachments = sorted([x for x in patient_attachments
                                if assessment_name.lower() in x['attachment_name'].lower()
                                and x['date_attached'] >= intake_date],
                               key=lambda x: x['date_attached'], reverse=True)
    if not attachments:
        return {
            f'{assessment_name}_completed': 'N/A',
            f'{assessment_name}_attached_date': 'N/A'
        }
    return {
        f'{assessment_name}_completed': True,
        f'{assessment_name}_attached_date': attachments[0]['date_attached']
    }


def _check_for_gains(attachments):
    pass


def _check_for_cafas_pecfas(post_intake_events):
    pass


def _check_for_da(post_intake_events):
    da_services = [x for x in post_intake_events if x['Service Type'] == 'DiagAssmt']
    if not da_services:
        return 'no Diagnostic Assessment after Intake'
    return {
        'da_date': min([x['Service Date'] for x in da_services])
    }


def _check_for_tx_plan(post_intake_events):
    tx_services = [x for x in post_intake_events if x['Service Type'] == 'TxPlan']
    if not tx_services:
        return 'no Treatment Plan after Intake'
    return {'tx_date': min([x['Service Date'] for x in tx_services])}


def _check_for_crisis_plan(post_intake_events):
    tx_services = [x for x in post_intake_events if x['Service Type'] == 'CrisisPlan']
    if not tx_services:
        return 'no Crisis Plan after Intake'
    return {'crisis_date': min([x['Service Date'] for x in tx_services])}


def _check_for_service(post_intake_events, service_type):
    not_found = 'N/A'
    services = [x for x in post_intake_events if x['Service Type'] == service_type]
    if not services:
        return {
            f'{service_type}_date': not_found,
            f'{service_type}_provider': not_found,
            f'{service_type}_id': not_found
        }
    first_service = sorted(services, key=lambda x: x['Service Date'])[0]
    return {
        f'{service_type}_date': first_service['Service Date'],
        f'{service_type}_provider': first_service['Staff Name'],
        f'{service_type}_id': first_service['Service ID']
    }


def _check_for_csw_service(post_intake_events):
    pass


def get_months_intake(id_source, intake_month_number, intake_year_number=None, **kwargs):
    results = [
        [
            'Patient ID', 'Patient Name', 'Intake Date', 'Staff Assigned', 'PCP Noted', 'Emergency Contact Present',
            'Consent for Treatment Present', 'Consent for Medications Present', 'Labs Completed',
            'LOCUS Date', 'GAIN Date', 'CAFAS/PECFAS Date', 'CAFAS/PECFAS Uploaded',
            'Diagnostic Assessment Date', '2 AQP Approvals', 'Crisis Plan Date', 'Treatment Plan Date',
            'Treatment Plan Signed by AQP/QP', 'Treatment Plan Signed by Patient', 'Insurance', 'Authorization Dates',
            'CSW Service Provided within 48 Hours'
        ]
    ]
    today = datetime.utcnow()
    if not intake_year_number:
        intake_year_number = today.year
    weekday, last_day = calendar.monthrange(intake_year_number, intake_month_number)
    month_start = datetime(intake_year_number, intake_month_number, 1)
    month_end = datetime(intake_year_number, intake_month_number, last_day)
    driver = CredibleFrontEndDriver(id_source)
    intake_patients = _get_intake_patients(driver, month_start, month_end)
    audit_tasks = [
        _check_consents
    ]
    services = [
        'DiagAssmt',
        'TxPlan',
        'CrisisPlan',
        'CommSupp',
    ]
    attached_assessments = ['locus', 'gain']
    for patient in intake_patients:
        intake_date = patient['intake_date']
        patient_id = patient['patient_id']
        patient_name = patient['patient_name']
        post_intake_services = _get_post_intake_services(driver, patient_id, intake_date)
        patient_attachments = get_client_attachments(driver, patient_id)
        # patient_profile = get_client_profile(driver, patient_id)
        result = {
            'patient_id': patient_id,
            'patient_name': patient_name,
            'intake_date': intake_date
        }
        for assessment in attached_assessments:
            result.update(_check_for_attached_assessment(assessment, patient_attachments, intake_date))
        for task in audit_tasks:
            result.update(task(post_intake_services))
        for service in services:
            service_results = _check_for_service(post_intake_services, service)
            result.update(service_results)
        results.append(result)
    return results


if __name__ == '__main__':
    test_id_source = 'PSI'
    test_month = 5
    test_results = get_months_intake(test_id_source, test_month)
    print(test_results)
