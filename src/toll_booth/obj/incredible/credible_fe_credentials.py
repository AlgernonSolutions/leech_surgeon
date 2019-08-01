import datetime

import requests
from algernon import AlgObject
from algernon.aws import Opossum
from requests import cookies
from requests.exceptions import ConnectionError


class CredibleFrontEndLoginException(Exception):
    pass


class CredibleLoginCredentials(AlgObject):
    def __init__(self, id_source, domain_name, site_url, authenticated_session, time_generated):
        self._id_source = id_source
        self._domain_name = domain_name
        self._site_url = site_url
        self._authenticated_session = authenticated_session
        self._time_generated = time_generated

    @property
    def domain_name(self):
        return self._domain_name

    @property
    def site_url(self):
        site_url = self._site_url.replace('/index.aspx', '')
        return site_url

    @property
    def authenticated_session(self):
        return self._authenticated_session

    @property
    def time_generated(self):
        return self._time_generated

    @classmethod
    def retrieve(cls, id_source, session=None, username=None, password=None, domain_name=None):
        jar = cookies.RequestsCookieJar()
        if not session:
            session = requests.Session()
        if not username or not password:
            credentials = Opossum.get_untrustworthy_credentials(id_source)
            username = credentials['username']
            password = credentials['password']
            domain_name = credentials['domain_name']
        attempts = 0
        while attempts < 3:
            try:
                login_url = "https://login.cbh2.crediblebh.com"
                domain_url = ".crediblebh.com"
                login_api_url = 'https://login-api.cbh2.crediblebh.com/api/'
                data_center_path = 'Authenticate/CheckDataCenter'
                check_login_path = 'Authenticate/CheckLogin'
                first_payload = {'UserName': username,
                                 'Password': password,
                                 'DomainName': domain_name}
                headers = {'DomainName': domain_name}
                data_center_post = session.post(login_api_url+data_center_path, json=first_payload, headers=headers, verify=False)
                data_center_json = data_center_post.json()
                if data_center_json['Status'] == 14:
                    login_api_url = data_center_json['ApiUrl']
                    login_url = data_center_json['LoginUrl']
                login_post = session.post(login_api_url+check_login_path, json=first_payload, headers=headers, verify=False)
                login_json = login_post.json()
                if login_json['Status'] != 0:
                    raise CredibleFrontEndLoginException()
                session_cookie = login_json['SessionCookie']
                website_url = login_json['WebsiteURL']
                jar.set('SessionId', session_cookie, domain=domain_url, path='/')
                session.get(website_url, data={'SessionId': session_cookie}, cookies=jar, verify=False)
                return cls(id_source, domain_name, website_url, session, datetime.datetime.now())
            except KeyError or ConnectionError or IndexError:
                attempts += 1
        raise CredibleFrontEndLoginException()

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['id_source'], json_dict['domain_name'], json_dict['site_url'],
            json_dict['authenticated_session'], json_dict['time_generated']
        )

    def is_stale(self, lifetime_minutes=30):
        cookie_age = (datetime.datetime.now() - self._time_generated).seconds
        return cookie_age >= (lifetime_minutes * 60)

    def refresh_if_stale(self, lifetime_minutes=30, **kwargs):
        if self.is_stale(lifetime_minutes):
            self.refresh(**kwargs)
            return True
        return False

    def refresh(self, session=None, username=None, password=None):
        new_credentials = self.retrieve(self._id_source, session, username, password)
        self._authenticated_session = new_credentials.authenticated_session

    def destroy(self):
        logout_url = 'https://www.cbh2.crediblebh.com/secure/logout.aspx'
        self._authenticated_session.get(logout_url)

    def validate(self):
        validation_url = 'https://www.crediblebh.com'
        test_get = self._authenticated_session.get(validation_url)
        request_history = test_get.history
        for response in request_history:
            if response.is_redirect:
                redirect = response.headers['Location']
                if '/secure/login.asp' in redirect:
                    return False
        return True

    def refresh_if_invalid(self, **kwargs):
        if not self.validate():
            self.refresh(**kwargs)
            return True
        return False

    def __str__(self):
        return str(self._authenticated_session)
