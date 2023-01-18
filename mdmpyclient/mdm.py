import logging
import sys
import os
import copy
import requests
import yaml

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

    def __init__(self, configuracion, translator, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.configuracion = configuracion
        self.translator = translator
        with open(self.configuracion['cache'], 'r', encoding='utf-8') as cache_file:
            self.translator_cache = yaml.safe_load(cache_file)

        self.session = self.authenticate()
        self.initialize(init_data)

    def initialize(self, init_data):
        self.codelists = Codelists(self.session, self.configuracion, self.translator, self.translator_cache, init_data)

        self.concept_schemes = ConceptSchemes(self.session, self.configuracion, self.translator, self.translator_cache,
                                              init_data)

        self.category_schemes = CategorySchemes(self.session, self.configuracion, self.translator,
                                                self.translator_cache, init_data)

        self.dsds = DSDs(self.session, self.configuracion)

        self.cubes = Cubes(self.session, self.configuracion)

        self.mappings = Mappings(self.session, self.configuracion)

        self.dataflows = Dataflows(self.session, self.configuracion, self.translator, self.translator_cache,init_data)

        self.msds = MSDs(self.session, self.configuracion)

        self.metadataflows = Metadataflows(self.session, self.configuracion)

        self.metadatasets = Metadatasets(self.session, self.configuracion, init_data)

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

    def ddb_reset(self):
        self.logger.info('Se va a reinicar la DDB')
        try:
            response = self.session.post(f'{self.configuracion["url_base"]}DDBReset')
            response.raise_for_status()
        except Exception as e:
            raise e

    def delete_all(self, agency, category_scheme_id, version):
        self.logger.info('Se van a borrar todo los datos')
        self.metadatasets.delete_all()
        self.metadataflows.delete_all()
        self.ddb_reset()
        cs = self.category_schemes.data[agency][category_scheme_id][version]
        cs.import_dcs()
        cs.set_permissions()
        self.dsds.delete_all(agency)
        self.concept_schemes.delete_all(agency)
        self.codelists.delete_all(agency)
        self.initialize(True)


    def put(self,directory):

        path = os.path.join(directory, "origin")
        self.put_all_sdmx(path)
        path = os.path.join(directory, "categoryschemes")
        self.put_all_sdmx(path)
        path = os.path.join(directory, "dsds")
        self.put_all_sdmx(path)
        path = os.path.join(directory, "conceptschemes")
        self.put_all_sdmx(path)
        path = os.path.join(directory, "codelists")
        self.put_all_sdmx(path)
        path = os.path.join(directory, "dataflows")
        self.put_all_sdmx(path)



    def put_all_sdmx(self, directory):

        for filename in os.scandir(directory):
            path = os.path.join(directory, filename.name)
            imported_items = []
            importData = False
            with open(path, 'rb') as file:
                body = {'file': ('test.xml', file, 'application/xml', {})}
                data, content_type = requests.models.RequestEncodingMixin._encode_files(body, {})
                upload_headers = copy.deepcopy(self.session.headers)
                upload_headers['Content-Type'] = content_type
                try:
                    response = self.session.post(
                        f'{self.configuracion["url_base"]}checkImportedFileXmlSdmxObjects', data=data,
                        headers=upload_headers)
                    response_body = response.json()
                    imported_items = response_body["importedItem"]
                    response.raise_for_status()
                except Exception as e:
                    raise e
                self.logger.info('Reporte subido correctamente a la API, realizando importacion')

                request_post_body = {"hashImport": response_body["hashImport"] , "importedItem" : []}
                for importedItem in imported_items:

                    if importedItem["isOk"]:
                        importData = True
                        request_post_body["importedItem"].append(importedItem)
                if importData:
                    try:
                        response = self.session.post(
                            f'{self.configuracion["url_base"]}importFileXmlSdmxObjects',
                            json=request_post_body)
                        response.raise_for_status()
                    except Exception as e:
                        raise e





