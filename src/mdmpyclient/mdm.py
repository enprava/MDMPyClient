import logging
import sys

import requests

from src.mdmpyclient.categoryscheme import CategoryScheme
from src.mdmpyclient.codelist import Codelist
from src.mdmpyclient.conceptscheme import ConceptScheme
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
        self.logger = logging.getlogger(f'{self.__class__.__name__}')
        self.configuracion = configuracion

        self.session = self.authenticate()

        self.codelists = self.get_all_codelist()

        self.concept_schemes = self.get_all_concept_scheme()

        self.category_schemes = self.get_all_category_scheme()

        self.dsds = self.get_all_dsd()

        self.cubes = self.get_all_cube()

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

    def get_all_codelist(self):
        codelists = {}
        self.logger.info('Solicitando información de las codelists')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}codelist')
            response_data = response.json()['data']['codelists']

        except KeyError:
            self.logger.error('No se han extraído las codelist debido a un error de conexión con el servidor: %s',
                              response.text)
            return codelists
        except Exception as e:
            raise e
        self.logger.info('Codelist extraídas correctamente')

        for codelist in response_data:
            agency = codelist['agencyID']
            codelist_id = codelist['id']
            version = codelist['version']
            names = codelist['names']
            des = codelist['descriptions'] if 'descriptions' in codelist.keys() else None

            if agency not in codelists:
                codelists[agency] = {}
            if codelist_id not in codelists[agency].keys():
                codelists[agency][codelist_id] = {}
            codelists[agency][codelist_id][version] = Codelist(self.session, self.configuracion, codelist_id, agency,
                                                               version, names, des, True)
        return codelists

    def get_all_concept_scheme(self):
        concept_schemes = {}
        self.logger.info('Solicitando información de los esquemas de concepto')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}conceptScheme')
            response_data = response.json()['data'][
                'conceptSchemes']
        except KeyError:
            self.logger.error(
                'No se han extraído los esquemas de concepto debido a un error de conexión con el servidor: %s',
                response.text)
            return concept_schemes
        except Exception as e:
            raise e
        self.logger.info('Esquemas de concepto extraídos correctamente')

        for cs in response_data:
            agency = cs['agencyID']
            cs_id = cs['id']
            version = cs['version']
            names = cs['names']
            des = cs['descriptions'] if 'descriptions' in cs.keys() else None

            if agency not in concept_schemes:
                concept_schemes[agency] = {}
            if cs_id not in concept_schemes[agency].keys():
                concept_schemes[agency][cs_id] = {}
            concept_schemes[agency][cs_id][version] = ConceptScheme(self.session, self.configuracion, cs_id, agency,
                                                                    version, names, des, True)
        return concept_schemes

    def get_all_category_scheme(self):
        category_schemes = {}
        self.logger.info('Solicitando información de los esquemas de categorías')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}categoryScheme')
            response_data = response.json()['data'][
                'categorySchemes']
        except KeyError:
            self.logger.error(
                'No se han extraído los esquemas de categoría debido a un error de conexión con el servidor %s',
                response.text)
            return category_schemes
        except Exception as e:
            raise e
        self.logger.info('Esquemas de categoría extraídos correctamente')

        for category_scheme in response_data:
            category_scheme_id = category_scheme['id']
            agency = category_scheme['agencyID']
            version = category_scheme['version']
            names = category_scheme['names']
            des = category_scheme['descriptions'] if 'descriptions' in category_scheme.keys() else None

            if agency not in category_schemes:
                category_schemes[agency] = {}
            if category_scheme_id not in category_schemes[agency].keys():
                category_schemes[agency][category_scheme_id] = {}
            category_schemes[agency][category_scheme_id][version] = CategoryScheme(self.session, self.configuracion,
                                                                                   category_scheme_id, agency, version,
                                                                                   names, des, True)
            return category_schemes

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
