from mdmpyclient.codelist.codelists import Codelists
from mock import patch

config = {'url_base': 'http://test.com', 'nodeId': 'ESC01', 'languages': ['en', 'es']}
get = {'data': {'codelists': [{'agencyID': 0, 'id': 0, 'version': 0, 'names': {}}]}}


@patch('requests.Session')
def test_init(mock_request_session):
    codelists = Codelists(mock_request_session, config)
    assert codelists.configuracion
    assert codelists.session
    assert codelists.data
    return codelists


# @patch('requests.Session.get.Response.json', MagicMock(return_value=get))
# def test_get():
#     codelists = test_init()
#     codelists.get(False)
#     assert codelists.data
