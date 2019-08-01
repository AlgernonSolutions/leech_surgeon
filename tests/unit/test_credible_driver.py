from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from toll_booth.obj.incredible.credible_fe import CredibleFrontEndDriver
from toll_booth.obj.incredible.credible_fe_credentials import CredibleLoginCredentials


@pytest.mark.credible_credentials
class TestCredibleCredentials:
    def test_credentials_retrieval(self, id_source, mock_opossum, mock_credible_login):
        first_login_url = 'https://login-api.cbh2.crediblebh.com/api/Authenticate/CheckLogin'
        credentials = CredibleLoginCredentials.retrieve(id_source)
        assert isinstance(credentials, CredibleLoginCredentials)
        assert mock_credible_login.call_count == 2
        assert mock_credible_login.called_with(first_login_url, json=mock_opossum.return_value)
        assert mock_opossum.called
        assert credentials.is_stale() is False
        setattr(credentials, '_time_generated', datetime.now()-timedelta(hours=1))
        assert credentials.is_stale() is True


@pytest.mark.credible_driver
class TestCredibleDriver:
    def test_driver_creation(self, id_source):
        credentials = MagicMock()
        driver = CredibleFrontEndDriver(id_source, credentials=credentials)
        assert isinstance(driver, CredibleFrontEndDriver)
