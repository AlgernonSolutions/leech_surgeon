import logging

import pytz
import requests
from algernon import ajson
from retrying import retry

from toll_booth.obj.incredible.credible_csv_parser import CredibleCsvParser
from toll_booth.obj.incredible.credible_fe_credentials import CredibleLoginCredentials

_url_stems = {
    'Clients': '/client/client_hipaalog.asp',
    'Employees': '/employee/emp_hipaalog.asp',
    'DataDict': '/common/hipaalog_datadict.asp',
    'ChangeDetail': '/common/hipaalog_details.asp',
    'Employee Advanced': '/employee/list_emps_adv.asp',
    'Global': '/admin/global_hipaalog.aspx',
    'Encounter': '/visit/clientvisit_view.asp',
    'Versions': "/services/lookups_service.asmx/GetVisitDocVersions",
    'ViewVersions': '/visit/clientvisit_documentation_version_view.aspx'
}

_tz_map = {
    'PSI': pytz.timezone('America/New_York'),
    'ICFS': pytz.timezone('America/New_York')
}


def _login_required(function):
    def wrapper(*args, **kwargs):
        driver = args[0]
        driver.credentials.refresh_if_stale(session=driver.session)
        return function(*args, **kwargs)

    return wrapper


class CredibleFrontEndDriver:
    _monitor_extract_stems = {
        'Employees': '/employee/list_emps_adv.asp',
        'Clients': '/client/list_clients_adv.asp',
        'ClientVisit': '/visit/list_visits_adv.asp'
    }
    _field_value_params = {
        'Clients': 'client_id'
    }
    _field_value_maps = {
        'Date': 'datetime',
        'Service ID': 'number',
        'UTCDate': 'utc_datetime',
        'change_date': 'datetime',
        'by_emp_id': 'number'
    }

    def __init__(self, id_source, credentials=None):
        if not credentials:
            credentials = CredibleLoginCredentials.retrieve(id_source)
        self._id_source = id_source
        self._session = credentials.authenticated_session
        self._credentials = credentials
        self._base_stem = credentials.site_url

    def __enter__(self):
        session = requests.Session()
        if not self._credentials:
            credentials = CredibleLoginCredentials.retrieve(self._id_source, session=session)
            self._credentials = credentials
        session.cookies = self._credentials.as_request_cookie_jar
        self._session = session
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._credentials.destroy(self._session)
        if exc_type:
            raise exc_val
        return True

    @property
    def credentials(self):
        return self._credentials

    @property
    def session(self):
        return self._session

    @property
    def id_source(self):
        return self._id_source

    @_login_required
    def process_advanced_search(self, id_type, selected_fields, start_date=None, end_date=None):
        credible_date_format = '%m/%d/%Y'
        url = self._base_stem + self._monitor_extract_stems[id_type]
        data = {
            'submitform': 'true',
            'btn_export': ' Export ',
        }
        data.update(selected_fields)
        if start_date:
            data['start_date'] = start_date.strftime(credible_date_format)
        if end_date:
            data['end_date'] = end_date.strftime(credible_date_format)
        logging.debug(f'firing a command to url: {url} to process an advanced search: {data}')
        response = self._session.post(url, data=data, verify=False)
        logging.debug(f'received a response a command to url: {url} with data: {data}, response: {response.content}')
        possible_objects = CredibleCsvParser(self._id_source, _tz_map[self._id_source]).parse_csv_response(response.text)
        return possible_objects

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
    @_login_required
    def retrieve_client_encounter(self, encounter_id):
        url = self._base_stem + _url_stems['Encounter']
        response = self._session.get(url, data={'clientvisit_id': encounter_id}, verify=False)
        if response.status_code != 200:
            raise RuntimeError(f'could not get the encounter data for {encounter_id}, '
                               f'response code: {response.status_code}')
        encounter = response.text
        if '<title>ConsumerService View</title>' not in encounter:
            raise RuntimeError(f'something is wrong with this extracted encounter: {encounter}, do not pass it on')
        return encounter

    @_login_required
    def retrieve_client_encounters(self, encounter_ids):
        url = self._base_stem + '/visit/clientvisit_viewall.asp'
        response = self._session.get(url, data={'clientvisit_ids': encounter_ids})
        if response.status_code != 200:
            raise RuntimeError(f'could not get the encounter data for {encounter_ids}, '
                               f'response code: {response.status_code}')
        encounters = response.text
        if '<title>ConsumerService Multi-View</title>' not in encounters:
            raise RuntimeError(f'something is wrong with this extracted encounters: {encounters}, do not pass it on')
        return encounters

    @_login_required
    def retrieve_client_encounter_version(self, encounter_id, version_id):
        url = self._base_stem + _url_stems['ViewVersions']
        data = {
            'visitdocversion_id': str(version_id),
            'clientvisit_id': str(encounter_id)
        }
        response = self._session.get(url, data=data, verify=False)
        if response.status_code != 200:
            raise RuntimeError(
                f'could not get the encounter data for {encounter_id}, response code: {response.status_code}')
        return response.text

    @_login_required
    def retrieve_documentation_versions(self, encounter_id):
        url = self._base_stem + _url_stems['Versions']
        response = self._session.post(url, data={'clientvisit_id': encounter_id}, verify=False)
        if response.status_code != 200:
            raise RuntimeError(f'could not get the version data for {encounter_id}, '
                               f'response code: {response.status_code}')
        return ajson.loads(response.text)['data']

    @_login_required
    def list_client_attachments(self, client_id):
        url = self._base_stem + '/client/files.aspx'
        data = {'client_id': client_id}
        response = self._session.get(url, data=data, verify=False)
        if response.status_code != 200:
            raise RuntimeError(f'could not get the attachments for {client_id}, '
                               f'response code: {response.status_code}')
        attachments = response.text
        if 'FILE ATTACHMENTS' not in attachments:
            raise RuntimeError(f'something is wrong with this extracted attachments: {attachments}, do not pass it on')
        return attachments

    @_login_required
    def download_client_attachment(self, download_link):
        url = self._base_stem + download_link
        return self._session.get(url)

    @_login_required
    def get_client_profile(self, client_id):
        url = self._base_stem + '/client/client_view.asp'
        data = {'client_id': client_id}
        response = self._session.get(url, data=data, verify=False)
        if response.status_code != 200:
            raise RuntimeError(f'could not get the profile for {client_id}, '
                               f'response code: {response.status_code}')
        profile = response.text
        if 'Consumer Info' not in profile:
            raise RuntimeError(f'something is wrong with this extracted profile: {profile}, do not pass it on')
        return profile
