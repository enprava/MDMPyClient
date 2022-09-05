import logging
import sys

import requests

from mdmpyclient.categoryscheme.categoryschemes import CategorySchemes
from mdmpyclient.codelist.codelists import Codelists
from mdmpyclient.conceptscheme.conceptschemes import ConceptSchemes
from mdmpyclient.cube.cubes import Cubes
from mdmpyclient.dataflow.dataflows import Dataflows
from mdmpyclient.dsd.dsds import DSDs
from mdmpyclient.mapping.mappings import Mappings
from mdmpyclient.metadataflow.metadataflows import Metadataflows
from mdmpyclient.metadataset.metadatasets import Metadatasets
from mdmpyclient.msd.msds import MSDs

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("deepl")
logger.setLevel(logging.WARNING)

class MDM:
    """ Clase encargada de gestionar todas las peticiones a la API del M&D Manager.
    Hace uso del fichero de configuración configuracion/configuracion.yaml

    Args:
        configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
         parámetros necesarios como la url de la API. Debe ser inicializado a partir del
         fichero de configuración configuracion/configuracion.yaml

    Attributes:
        codelists (:obj:`Codelists`): Objeto que contiene todos las codelist
         de la API y encargado de gestionarlas
        concept_schemes (:obj:`ConceptSchemes`): Objeto que contiene todos los esquemas de
         conceptos de la API y encargado de gestionarlos
        category_schemes (:obj:`CategorySchemes`): Objeto que contiene todos los esquemas de
         categorías de la API y encargado de gestionarlos.
        dsds (:obj:`DSDs`): Objeto que contiene todos los DSDs de la API y encargado de gestionarlos.
        cubes (:obj:`Cubes`): Objeto que contiene todos los cubos de la API y encargado de gestionarlos.
        mappings (:obj:`Mappings`): Objeto que contiene todos los mappings de la API y
         encargado de gestionarlos.
        dataflows (:obj:`Dataflows`): Objeto que contiene todos los dataflows de la API
         y encargado de gestionarlos.
        msds (:obj:`MSDs`): Objeto que contiene todos los MSDs de la API y encargado de gestionarlos.
        metadataflows (:obj:`Metadataflows`): Objeto que contiene todos los metadataflows de la API
         y encargado de gestionarlos.
        metadatasets (:obj:`Metadatasets`): Objeto que contiene todos los metatatasets de la API
         y encargado de gestionarlos.
    """

    def __init__(self, configuracion, translator, translator_cache):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.configuracion = configuracion
        self.translator = translator
        self.translator_cache = translator_cache

        self.session = self.authenticate()

        self.codelists = Codelists(self.session, self.configuracion, self.translator, self.translator_cache)

        self.concept_schemes = ConceptSchemes(self.session, self.configuracion, self.translator, self.translator_cache)

        self.category_schemes = CategorySchemes(self.session, self.configuracion)

        self.dsds = DSDs(self.session, self.configuracion)

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