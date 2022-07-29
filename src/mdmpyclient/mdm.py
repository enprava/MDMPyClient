import logging
import sys

import requests

from src.mdmpyclient.categoryscheme import CategoryScheme
from src.mdmpyclient.codelists import Codelists
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
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.configuracion = configuracion

        self.session = self.authenticate()

        self.codelists = Codelists(self.session,self.configuracion,init_data=False)


        # self.concept_schemes = self.get_all_concept_scheme()
        #
        # self.category_schemes = self.get_all_category_scheme()
        #
        # self.dsds = self.get_all_dsd()

        # self.cubes = self.get_all_cube()

    def authenticate(self):
        headers = {'nodeId': self.configuracion['nodeId'], 'language': self.configuracion['languages'][0],
                   'Content-Type': 'application/json;charset=utf-8'}
        session = requests.session()

        session.headers = headers
        self.logger.info('Solicitando acceso a la NODE_API.')
        response = session.post(f'{self.configuracion["url_base"]}api/Security/Authenticate/',
                                json={'username': 'admin'})
        try:
            session.headers['Authorization'] = f'bearer {response.json()["token"]}'
        except KeyError:
            session.headers['Authorization'] = None

        if session.headers['Authorization']:
            self.logger.info('Acceso completado con éxito')
        else:
            self.logger.warning('Acceso denegado')
        return session

    def logout(self):
        self.logger.info('Finalizando conexión con la API')
        self.session.post(f'{self.configuracion["url_base"]}api/Security/Logout')



    def get_all_concept_scheme(self):
        concept_schemes = {}
        self.logger.info('Solicitando información de los esquemas de concepto')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}conceptScheme').json()['data'][
                'conceptSchemes']
        except KeyError:
            self.logger.warning('No se han extraído los esquemas de concepto debido a un error inesperado')
            return concept_schemes
        self.logger.info('Esquemas de concepto extraídos correctamente')

        for cs in response:
            agency = cs['agencyID']
            cs_id = cs['id']
            version = cs['version']

            if agency not in concept_schemes:
                concept_schemes[agency] = {}
            if cs_id not in concept_schemes[agency].keys():
                concept_schemes[agency][cs_id] = {}
            concept_schemes[agency][cs_id][version] = ConceptScheme(self.session, self.configuracion, cs_id, agency,
                                                                    version)
        return concept_schemes

    def get_all_category_scheme(self):
        category_schemes = {}
        self.logger.info('Solicitando información de los esquemas de categorías')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}categoryScheme').json()['data'][
                'categorySchemes']
        except KeyError:
            self.logger.info('No se han extraído los esquemas de categoría debido a un error inesperado')
            return category_schemes
        self.logger.info('Esquemas de categoría extraídos correctamente')

        for category_scheme in response:
            category_scheme_id = category_scheme['id']
            agency = category_scheme['agencyID']
            version = category_scheme['version']

            if agency not in category_schemes:
                category_schemes[agency] = {}
            if category_scheme_id not in category_schemes[agency].keys():
                category_schemes[agency][category_scheme_id] = {}
            category_schemes[agency][category_scheme_id][version] = CategoryScheme(self.session, self.configuracion,
                                                                                   category_scheme_id, agency, version,
                                                                                   True)
        return category_schemes

    def get_all_dsd(self):
        dsd = {}
        self.logger.info('Solicitando información de los DSDs')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}dsd').json()['data']['dataStructures']
        except KeyError:
            self.logger.info('No se han extraído los DSDs debido a un error inesperado')

            return dsd
        self.logger.info('DSDs estraídos correctamente')

        for data_structure in response:
            dsd_id = data_structure['id']
            agency = data_structure['agencyID']
            version = data_structure['version']

            if agency not in dsd:
                dsd[agency] = {}
            if dsd_id not in dsd[agency].keys():
                dsd[agency][dsd_id] = {}
            dsd[agency][dsd_id][version] = DSD(self.session, self.configuracion, dsd_id, agency, version, True)
        return dsd

    def get_all_cube(self):
        cubes = {}
        try:
            response = self.session.get(f'{self.configuracion["url_base"]}cubesNoFilter').json()
        except KeyError:
            return cubes
        for cube in response:
            cube_id = cube['IDCube']
            cube_code = cube['Code']
            cat_id = cube["IDCat"]
            dsd_code = cube['DSDCode']
            name_es = cube['labels']['es'] if 'es' in cube['labels'].keys() else None
            name_en = cube['labels']['en'] if 'en' in cube['labels'].keys() else None

            cubes[cube_id] = Cube(self.session, self.configuracion, cube_id, cube_code, cat_id, dsd_code, name_es,
                                  name_en, True)

        return cubes
