import logging
import sys
import os
# from requests import Request
import copy
import requests

from categoryscheme import CategoryScheme

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class CategorySchemes:
    """ Clase que representa el conjunto de esquemas de categorías del M&D Manager.

       Args:
           session (:class:`requests.session.Session`): Sesión autenticada en la API.
           configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
            parámetros necesarios como la url de la API. Debe ser inicializado a partir del
            fichero de configuración configuracion/configuracion.yaml.
           init_data (:class:`Boolean`): True para traer todas las categorías de los esquemas,
            False para no traerlas. Por defecto toma el valor True.

       Attributes:
           data (:obj:`Diccionario`): Diccionario con todas las codelists

       """

    def __init__(self, session, configuracion, translator, translator_cache, init_data=True):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.translator = translator
        self.translator_cache = translator_cache
        self.data = self.get(init_data)

    def get(self, init_data=True):
        category_schemes = {}
        self.logger.info('Solicitando información de los esquemas de categorías')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}categoryScheme')
            response_data = response.json()['data']['categorySchemes']
        except KeyError:
            self.logger.error(
                'No se han extraído los esquemas de categoría debido a un error de conexión con el servidor %s',
                response.text)
            return category_schemes
        except Exception as e:
            raise e
        self.logger.info('Esquemas de categoría extraídos correctamente')
        self.categoryscheme_list = []
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
            cs = CategoryScheme(self.session, self.configuracion, self.translator,
                                self.translator_cache, category_scheme_id, agency,
                                version, names, des, init_data=init_data)
            category_schemes[agency][category_scheme_id][version] = cs
            self.categoryscheme_list.append(cs)
        return category_schemes

    def get_all_sdmx(self, directory):
        """

              Args:
                  directory: (:class:`String`) Directorio donde se van a guardar todas los esquemas de categoría en formato sdmx

              Returns: None

              """
        self.logger.info('Obteniendo todos los esquemas de categoría en formato sdmx')
        for cs in self.categoryscheme_list:
            cs.get_sdmx(directory)

    def put(self, agencia, cat_id, version, descripciones, nombres):
        json = {'data': {'categorySchemes': [
            {'agencyID': agencia, 'id': cat_id, 'isFinal': 'true', 'names': nombres, 'descriptions': descripciones,
             'version': version}]},
            'meta': {}}
        self.logger.info('Creando o actualizando esquema de categorías con id: %s', cat_id)
        try:
            response = self.session.put(f'{self.configuracion["url_base"]}updateArtefacts', json=json)
            response.raise_for_status()

        except Exception as e:
            raise e
        self.logger.info('Esquema de categorías creado o actualizado correctamente')
