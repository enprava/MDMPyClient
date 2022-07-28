import requests
from src.mdmpyclient.codelist import Codelist
from src.mdmpyclient.conceptscheme import ConceptScheme
from src.mdmpyclient.categoryscheme import CategoryScheme
import logging
import sys

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

        self.logger.info('Solicitando acceso a la NODE_API.')
        self.session = self.authenticate()
        if self.session.headers['Authorization']:
            self.logger.info('Acceso completado con éxito')
        else:
            self.logger.warning('Acceso denegado')

        # self.logger.info('Solicitando información de las codelists')
        # self.codelists = self.get_all_codelist()
        # if self.codelists:
        #     self.logger.info('Codelist extraídas correctamente')
        # else:
        #     self.logger.warning('No se han extraído las codelist debido a un error inesperado')
        #
        # self.logger.info('Solicitando información de los esquemas de concepto')
        # self.concept_schemes = self.get_all_concept_scheme()
        # if self.concept_schemes:
        #     self.logger.info('Esquemas de concepto extraídos correctamente')
        # else:
        #     self.logger.warning('No se han extraído los esquemas de concepto debido a un error inesperado')
        #
        # self.logger.info('Solicitando información de los esquemas de categorías')
        # self.category_schemes = self.get_all_category_scheme()
        # if self.category_schemes:
        #     self.logger.info('Esquemas de categoría extraídos correctamente')
        # else:
        #     self.logger.info('No se han extraído los esquemas de categoría debido a un error inesperado')

        # self.dsds = self.get_all_dsd()

    def authenticate(self):
        headers = {'nodeId': self.configuracion['nodeId'], 'language': self.configuracion['language']}
        session = requests.session()

        session.headers = headers
        response = session.post(f'{self.configuracion["url_base"]}api/Security/Authenticate/',
                                json={'username': 'admin'})
        try:
            session.headers['Authorization'] = f'bearer {response.json()["token"]}'
        except KeyError:
            session.headers['Authorization'] = None
        return session

    def logout(self):
        self.logger.info('Finalizando conexión con la API')
        self.session.post(f'{self.configuracion["url_base"]}api/Security/Logout')

    def get_all_codelist(self):
        codelists = {}
        try:
            response = self.session.get(f'{self.configuracion["url_base"]}codelist').json()['data']['codelists']
        except KeyError:
            return codelists

        for codelist in response:
            agency = codelist['agencyID']
            codelist_id = codelist['id']
            version = codelist['version']

            if agency not in codelists.keys():
                codelists[agency] = {}
            if codelist_id not in codelists[agency].keys():
                codelists[agency][codelist_id] = {}
            codelists[agency][codelist_id][version] = Codelist(self.session, self.configuracion, codelist_id, agency,
                                                               version)
        return codelists

    def get_all_concept_scheme(self):
        concept_schemes = {}
        try:
            response = self.session.get(f'{self.configuracion["url_base"]}conceptScheme').json()['data'][
                'conceptSchemes']
        except KeyError:
            return concept_schemes

        for cs in response:
            agency = cs['agencyID']
            cs_id = cs['id']
            version = cs['version']

            if agency not in concept_schemes.keys():
                concept_schemes[agency] = {}
            if cs_id not in concept_schemes[agency].keys():
                concept_schemes[agency][cs_id] = {}
            concept_schemes[agency][cs_id][version] = ConceptScheme(self.session, self.configuracion, cs_id, agency,
                                                                    version)
        return concept_schemes

    def get_all_category_scheme(self):
        category_schemes = {}
        try:
            response = self.session.get(f'{self.configuracion["url_base"]}categoryScheme').json()['data'][
                'categorySchemes']
        except KeyError:
            return category_schemes

        for category_scheme in response:
            category_scheme_id = category_scheme['id']
            agency = category_scheme['agencyID']
            version = category_scheme['version']

            if agency not in category_schemes.keys():
                category_schemes[agency] = {}
            if category_scheme_id not in category_schemes[agency].keys():
                category_schemes[agency][category_scheme_id] = {}
            category_schemes[agency][category_scheme_id][version] = CategoryScheme(self.session, self.configuracion,
                                                                                   category_scheme_id, agency, version,
                                                                                   True)
        return category_schemes

    def get_all_dsd(self):
        dsd = {}
        try:
            response = self.session.get(f'{self.configuracion["url_base"]}dsd').json()['data']['dataStructures']
        except KeyError:
            return dsd

        for data_structure in response:
            dsd_id = data_structure['id']
            agency = data_structure['agencyID']
            version = data_structure['version']

            if agency not in dsd.keys():
                dsd[agency] = {}
            if dsd_id not in dsd[agency].keys():
                dsd[agency][dsd_id] = {}
            dsd[agency][dsd_id][version] = DSD(self.session, self.configuracion, dsd_id, agency, version, True)
        return dsd

    def get_all_cube(self):
        cubes = {}
        try:
            response_cubes = self.session.get(f'{self.configuracion["url_base"]}cubesNoFilter')
        except KeyError:
            return cubes

        return cubes
