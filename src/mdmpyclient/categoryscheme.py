import pandas
import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class CategoryScheme:
    """ Clase que representa un esquema de conceptos del M&D Manager.

    Args:
        session (:class:`requests.session.Session`): Sesión autenticada en la API.
        configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
         parámetros necesarios como la url de la API. Debe ser inicializado a partir del
         fichero de configuración configuracion/configuracion.yaml.
        category_scheme_id (class: `String`): Identificador del esquema de categorías.
        agency_id (class: `String`): Identificador de la agencia vinculada
        version (class: `String`): Versión del esquema de categorías
        init_data (class: `Boolean`): True para traer todos los datos del esquema de
         categorías, False para traer solo id, agencia y versión. Por defecto toma el valor False.
    Attributes:

    """

    def __init__(self, session, configuracion, category_scheme_id, agency_id, version, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = category_scheme_id
        self.agency_id = agency_id
        self.version = version
        self.data = self.get_data() if init_data else None

    def get_data(self):
        categories = {'id': [], 'name_es': [], 'name_en': [], 'des_es': [], 'des_en': [],
                      'parent': []}
        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}categoryScheme/{self.id}/{self.agency_id}/{self.version}').json()[
                'data']['categorySchemes'][0]['categories']
        except KeyError:
            self.logger.warning(
                f'Ha ocurrido un error inesperado mientras se cargaban los datos del esquema de categorías con id: {self.id}')
            return categories

        self.__extract_categories(response, None, categories)
        return pandas.DataFrame(data=categories)

    def __extract_categories(self, response, parent, categories):
        for categorie in response:
            category_id = categorie['id']
            category_name_es = categorie['names']['es'] if 'es' in categorie['names'].keys() else None
            category_name_en = categorie['names']['en'] if 'en' in categorie['names'].keys() else None

            categories['id'].append(category_id)
            categories['name_es'].append(category_name_es)
            categories['name_en'].append(category_name_en)
            categories['parent'].append(parent)
            if 'descriptions' in categorie.keys():
                category_des_es = categorie['descriptions']['es'] if 'es' in categorie['descriptions'].keys() else None
                category_des_en = categorie['descriptions']['en'] if 'en' in categorie['descriptions'].keys() else None
            else:
                category_des_es = None
                category_des_en = None

            categories['des_es'].append(category_des_es)
            categories['des_en'].append(category_des_en)

            if 'categories' in categorie.keys():
                self.extract_categories(categorie['categories'], category_id, categories)

    def __repr__(self):
        return f'{self.id} {self.version}'
