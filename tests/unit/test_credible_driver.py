import pytest

from toll_booth.obj.incredible.credible_fe import CredibleLoginCredentials, CredibleFrontEndDriver


@pytest.mark.credible_credentials
class TestCredibleCredentials:
    def test_credentials_retrieval(self, id_source, mock_opossum, mock_credible_login):
        credentials = CredibleLoginCredentials.retrieve(id_source)
        assert isinstance(credentials, CredibleLoginCredentials)


@pytest.mark.credible_driver
class TestCredibleDriver:
    def test_driver_creation(self, id_source):
        driver = CredibleFrontEndDriver(id_source)
        assert isinstance(driver, CredibleFrontEndDriver)
