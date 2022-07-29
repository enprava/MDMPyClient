import logging
import sys

import requests

from src.mdmpyclient.codelist import Codelist

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Codelists:
    """ Clase que representa el conjunto de codelists del M&D Manager.

    Args:
        session (:class:`requests.session.Session`): Sesión autenticada en la API.
        configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
         parámetros necesarios como la url de la API. Debe ser inicializado a partir del
         fichero de configuración configuracion/configuracion.yaml.
        codelist_id (:class:`String`): Identificador de la codelist.
        version (:class:`String`): Versión de la codelist.
        agency_id (:class:`String`): Agencia vinculada a la codelist.
        init_data (:class:`Boolean`): True para traer todos los datos de la codelist,
         False para traer solo id, agencia y versión. Por defecto toma el valor False.

    Attributes:
        data
    """

    def __init__(self, session, configuracion, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.session = session
        self.configuracion = configuracion

        self.data = self.get(init_data)

    def get(self, init_data=True):
        codelists = {}
        self.logger.info('Solicitando información de las codelists')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}codelist').json()['data']['codelists']
        except KeyError:
            self.logger.warning('No se han extraído las codelist debido a un error inesperado')
            return codelists
        self.logger.info('Codelist extraídas correctamente')

        for codelist in response:
            agency = codelist['agencyID']
            codelist_id = codelist['id']
            version = codelist['version']

            if agency not in codelists:
                codelists[agency] = {}
            if codelist_id not in codelists[agency].keys():
                codelists[agency][codelist_id] = {}
            codelists[agency][codelist_id][version] = Codelist(self.session, self.configuracion, codelist_id, agency,
                                                               version, init_data)
        return codelists

    def create(self, agencia, id, version, descripciones, nombres):
        json = {'data': {'codelists': [
            {'agencyID': agencia, 'id': id, 'isFinal': 'true', 'names': nombres, 'descriptions': descripciones,
             'version': str(version)}]},
            'meta': {}}

        try:

            response = \
                self.session.post(f'{self.configuracion["url_base"]}createArtefacts',
                                  json=json)

            response.raise_for_status()
        except Exception as e:
            raise e

    def put(self, agencia, id, version, descripciones, nombres):
        json = {'data': {'codelists': [
            {'agencyID': agencia, 'id': id, 'isFinal': 'true', 'names': nombres, 'descriptions': descripciones,
             'version': str(version)}]},
            'meta': {}}

        try:
            response = \
                self.session.put(f'{self.configuracion["url_base"]}updateArtefacts',
                                 json=json)

            response.raise_for_status()
        except Exception as e:
            raise e
