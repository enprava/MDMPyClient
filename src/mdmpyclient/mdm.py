import logging
import sys

import requests

from src.mdmpyclient.categoryschemes import CategorySchemes
from src.mdmpyclient.codelists import Codelists
from src.mdmpyclient.conceptschemes import ConceptSchemes
from src.mdmpyclient.cubes import Cubes
from src.mdmpyclient.dataflows import Dataflows
from src.mdmpyclient.dsds import DSDs
from src.mdmpyclient.mappings import Mappings
from src.mdmpyclient.metadataflows import Metadataflows
from src.mdmpyclient.metadatasets import Metadatasets
from src.mdmpyclient.msds import MSDs

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class MDM:
    """ Clase encargada de gestionar todas las peticiones a la API del M&D Manager.
    Hace uso del fichero de configuración configuracion/configuracion.yaml

    Args:
        configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
         parámetros necesarios como la url de la API. Debe ser inicializado a partir del
         fichero de configuración configuracion/configuracion.yaml

    Attributes:
        codelists (:obj:`Dicconario`): Diccionario que contiene todos las codelist
         de la API indexados por agencia, id y versión.
        concept_schemes (:obj:`Diccionario`): Diccionario que contiene todos los esquemas de
         conceptos de la API indexados por agencia, id y versión.
        category_schemes (:obj:`Diccionario`): Diccionario que contiene todos los esquemas de
         categorías de la API indexados por agencia, id y versión.

    """

    def __init__(self, configuracion):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.configuracion = configuracion

        self.session = self.authenticate()

        self.codelists = Codelists(self.session, self.configuracion)

        self.concept_schemes = ConceptSchemes(self.session, self.configuracion)

        self.category_schemes = CategorySchemes(self.session, self.configuracion)

        self.dsds = DSDs(self.session, self.configuracion, self.codelists, self.concept_schemes)

        self.cubes = Cubes(self.session, self.configuracion)

        self.mappings = Mappings(self.session, self.configuracion)

        self.dataflows = Dataflows(self.session, self.configuracion)

        self.msds = MSDs(self.session, self.configuracion)

        self.metadataflows = Metadataflows(self.session, self.configuracion)

        self.metadatasets = Metadatasets(self.session, self.configuracion)

    def authenticate(self):
        headers = {'nodeId': self.configuracion['nodeId'], 'language': self.configuracion['languages'][0],
                   'Content-Type': 'application/json;charset=utf-8'}
        session = requests.session()

        session.headers = headers
        self.logger.info('Solicitando acceso a la NODE_API.')

        try:
            response = session.post(f'{self.configuracion["url_base"]}api/Security/Authenticate/',
                                    json={'username': 'admin'})
            session.headers['Authorization'] = f'bearer {response.json()["token"]}'
        except KeyError:
            self.logger.error('El usuario o la contraseña no existe')
            sys.exit(1)
        except requests.exceptions.ConnectTimeout:
            self.logger.error('No se ha podido establecer conexión con el servidor')
            sys.exit(1)
        except Exception as e:
            raise e

        if session.headers['Authorization']:
            self.logger.info('Acceso completado con éxito')
        else:
            self.logger.warning('Acceso denegado')
        return session

    def logout(self):
        self.logger.info('Finalizando conexión con la API')
        self.session.post(f'{self.configuracion["url_base"]}api/Security/Logout')
