import pytest

from toll_booth import handler


@pytest.mark.handler
class TestHandler:
    def test_handler(self, mock_context):
        event = {}
        results = handler(event, mock_context)
        assert results
