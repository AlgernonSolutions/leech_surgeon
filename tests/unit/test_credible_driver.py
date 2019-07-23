import pytest

from toll_booth.obj.incredible.credible_fe import CredibleLoginCredentials, CredibleFrontEndDriver


@pytest.mark.credible_credentials
class TestCredibleCredentials:
    @pytest.mark.usefixtures('mock_opossum')
    def test_credentials_creation(self, id_source):
        credentials = CredibleLoginCredentials.retrieve(id_source)
        assert isinstance(credentials, CredibleLoginCredentials)



@pytest.mark.credible_driver
class TestCredibleDriver:
    def test_driver_creation(self, id_source):
        driver = CredibleFrontEndDriver(id_source)
        assert isinstance(driver, CredibleFrontEndDriver)
