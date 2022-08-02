import logging
import sys

import requests

from src.mdmpyclient.categoryscheme import CategoryScheme
from src.mdmpyclient.categoryschemes import CategorySchemes
from src.mdmpyclient.codelists import Codelists
from src.mdmpyclient.conceptscheme import ConceptScheme
from src.mdmpyclient.conceptschemes import ConceptSchemes
from src.mdmpyclient.cube import Cube
from src.mdmpyclient.dsd import DSD

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

        self.codelists = Codelists(self.session,self.configuracion,init_data=False)

        self.concept_schemes = ConceptSchemes(self.session, self.configuracion, init_data=False)

        self.category_schemes = CategorySchemes(self.session, self.configuracion, init_data=False)

        # self.dsds = self.get_all_dsd()

        # self.cubes = self.get_all_cube()

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

    def get_all_dsd(self):
        dsd = {}
        self.logger.info('Solicitando información de los DSDs')
        try:
            response = self.session.get(f'{self.configuracion["url_base"]}dsd')
            response_data = response.json()['data']['dataStructures']
        except KeyError:
            self.logger.info(
                'No se han extraído los DSDs debido a un error de conexión con el servidor %s',
                response.text)
            return dsd
        except Exception as e:
            raise e
        self.logger.info('DSDs estraídos correctamente')

        for data_structure in response_data:
            dsd_id = data_structure['id']
            agency = data_structure['agencyID']
            version = data_structure['version']
            names = data_structure['names']
            des = data_structure['descriptions'] if 'descriptions' in data_structure.keys() else None

            if agency not in dsd:
                dsd[agency] = {}
            if dsd_id not in dsd[agency].keys():
                dsd[agency][dsd_id] = {}
            dsd[agency][dsd_id][version] = DSD(self.session, self.configuracion, dsd_id, agency, version, names, des,
                                               True)
        return dsd

    def get_all_cube(self):
        cubes = {}
        try:
            response = self.session.get(f'{self.configuracion["url_base"]}cubesNoFilter')
            response_data = response.json()
        except KeyError:
            self.logger.error('No se han extraído los cubos debido a un error de conexión con el servidor %s',
                              response.text)
            return cubes
        except Exception as e:
            raise e

        for cube in response_data:
            cube_id = cube['IDCube']
            cube_code = cube['Code']
            cat_id = cube["IDCat"]
            dsd_code = cube['DSDCode']
            names = cube['labels']

            cubes[cube_id] = Cube(self.session, self.configuracion, cube_id, cube_code, cat_id, dsd_code, names, True)

        return cubes
