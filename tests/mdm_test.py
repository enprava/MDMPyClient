from mdmpyclient.mdm import MDM

from mock import patch
config = {'url_base': 'http://test.com', 'nodeId': 'ESC01', 'languages': ['en', 'es'],'cache': "configuracion/traducciones.yaml"}


@patch('requests.session')
def test_init(mock_requests_session):
    client = MDM(config,None)
    assert mock_requests_session.call_count == 1
    assert client.configuracion
    assert client.session['Authorization']
    assert client.codelists
    assert client.concept_schemes
    assert client.category_schemes
    assert client.dsds
    assert client.cubes
    assert client.mappings
    assert client.dataflows
    assert client.msds
    assert client.metadataflows
    assert client.metadatasets

