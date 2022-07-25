import requests
from src.mdmpyclient.codelist import Codelist
from src.mdmpyclient.conceptscheme import ConceptScheme
from src.mdmpyclient.categoryscheme import CategoryScheme
import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class MDM:
    """ Clase encargada de gestionar todas las peticiones a la API del M&D Manager.
    Hace uso del fichero de configuración configuracion/configuracion.yaml

    Args:
        configuracion (:class:'Diccionario'): Diccionario del que se obtienen algunos
         parámetros necesarios como la url de la API. Debe ser inicializado a partir del
         fichero de configuración configuracion/configuracion.yaml

    Attributes:
        codelists (:obj:'Dicconario'): Diccionario que contiene id, agencia y versión de
         todas las codelist de la API.


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

        self.logger.info('Solicitando información de las codelists')
        self.codelists = self.get_all_codelist()
        if self.codelists:
            self.logger.info('Codelist extraídas correctamente')
        else:
            self.logger.warning('No se han extraído las codelist debido a un error inesperado')

        # self.logger.info('Solicitando información de los esquemas de concepto')
        # self.concept_schemes = self.get_all_concept_scheme()
        # if self.concept_schemes:
        #     self.logger.info('Esquemas de concepto extraídos correctamente')
        # else:
        #     self.logger.warning('No se han extraído los esquemas de concepto debido a un error inesperado')

        # self.category_scheme = self.get_all_category_scheme()

    def authenticate(self):
        headers = {'nodeId': self.configuracion['nodeId'], 'language': self.configuracion['language']}
        session = requests.session()

        session.headers = headers
        response = session.post(f'{self.configuracion["url_base"]}api/Security/Authenticate/',
                                json={'username': 'admin'})

        session.headers['Authorization'] = f'bearer {response.json()["token"]}'
        return session

    def logout(self):
        self.logger.info('Finalizando conexión con la API')
        self.session.post(f'{self.configuracion["url_base"]}api/Security/Logout')

    def get_all_codelist(self):
        response = self.session.get(f'{self.configuracion["url_base"]}codelist').json()['data']['codelists']

        codelists = {}

        for codelist in response:
            agency = codelist['agencyID']
            codelist_id = codelist['id']
            version = codelist['version']

            if agency not in codelists.keys():
                codelists[agency] = {}
            if codelist_id not in codelists[agency].keys():
                codelists[agency][codelist_id] = {}
            codelists[agency][codelist_id][version] = Codelist(self.session, self.configuracion, codelist_id, version,
                                                               agency, True)
        return codelists

    def get_all_concept_scheme(self):
        response = self.session.get(f'{self.configuracion["url_base"]}conceptScheme').json()['data']['conceptSchemes']

        concept_schemes = {}

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
        response = self.session.get(f'{self.configuracion["url_base"]}categoryScheme').json()['data']['categorySchemes']

        category_schemes = {}

        for category_scheme in response:
            category_scheme_id = category_scheme['id']
            agency = category_scheme['agencyID']
            version = category_scheme['version']

            if agency not in category_schemes.keys():
                category_schemes[agency] = {}
            if category_scheme_id not in category_schemes[agency].keys():
                category_schemes[agency][category_scheme_id] = {}
            category_schemes[agency][category_scheme_id][version] = CategoryScheme(self.session, self.configuracion,
                                                                                   category_scheme_id, agency, version)
        return category_schemes
