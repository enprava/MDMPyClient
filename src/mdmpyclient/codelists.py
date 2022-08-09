import logging
import sys

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
        init_data (:class:`Boolean`): True para traer todos los códigos de las listas,
         False para no traerlos. Por defecto toma el valor False.

    Attributes:
        data (:obj:`Dicconario`) Diccionario con todas las codelists

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
            self.logger.error(
                'No se han extraído los esquemas de concepto debido a un error de conexión con el servidor')
            return {}
        except Exception as e:
            raise e
        self.logger.info('Codelist extraídas correctamente')

        for codelist in response:
            agency = codelist['agencyID']
            codelist_id = codelist['id']
            version = codelist['version']
            names = codelist['names']
            des = codelist['descriptions'] if 'descriptions' in codelists else None
            if agency not in codelists:
                codelists[agency] = {}
            if codelist_id not in codelists[agency]:
                codelists[agency][codelist_id] = {}
            codelists[agency][codelist_id][version] = Codelist(self.session, self.configuracion, codelist_id, agency,
                                                               version, names, des, init_data=init_data)
        return response

    def create(self, agencia, codelist_id, version, nombres, descripciones):
        json = {'data': {'codelists': [
            {'agencyID': agencia, 'id': codelist_id, 'isFinal': 'true', 'names': nombres, 'descriptions': descripciones,
             'version': str(version)}]},
            'meta': {}}

        try:

            response = \
                self.session.post(f'{self.configuracion["url_base"]}createArtefacts',
                                  json=json)

            response.raise_for_status()
        except Exception as e:
            raise e

    def put(self, agencia, codelist_id, version, descripciones, nombres):
        json = {'data': {'codelists': [
            {'agencyID': agencia, 'id': codelist_id, 'isFinal': 'true', 'names': nombres, 'descriptions': descripciones,
             'version': version}]},
            'meta': {}}

        self.logger.info('Creando o actualizando codelist con id: %s', id)
        try:
            response = self.session.put(f'{self.configuracion["url_base"]}updateArtefacts', json=json)
            response.raise_for_status()
        except Exception as e:
            raise e
        self.logger.info('Codelist creada o actualizada correctamente')
