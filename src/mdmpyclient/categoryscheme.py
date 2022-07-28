import logging
import sys

import pandas

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
        categories = {'id': [], 'parent': [], 'id_cube_cat': []}
        for language in self.configuracion['languages']:
            categories[f'name_{language}'] = []
            categories[f'des_{language}'] = []

        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}categoryScheme/{self.id}/{self.agency_id}/{self.version}').json()[
                'data']['categorySchemes'][0]['categories']
            response_dcs = self.session.get(f'{self.configuracion["url_base"]}dcs').json()

        except KeyError:
            self.logger.warning(
                'Ha ocurrido un error inesperado mientras se cargaban los datos del esquema de categorías con id: %s',
                self.id)
            return categories

        dcs = self.__dcs_to_dict(response_dcs)
        categories = self.__merge_categories(response, None, dcs, categories)
        print(pandas.DataFrame(data=categories).to_string())
        return pandas.DataFrame(data=categories, dtype='string')

    def __merge_categories(self, response, parent, dcs, categories):
        for categorie in response:
            category_id = categorie['id']

            categories['id'].append(category_id)
            categories['parent'].append(parent)

            for language in self.configuracion['languages']:
                if language in categorie['names']:
                    categories[f'name_{language}'].append(categorie['names'][language])
                else:
                    categories[f'name_{language}'].append(None)

                if 'descriptions' in categorie.keys() and language in categorie['descriptions']:
                    categories[f'des_{language}'].append(categorie['description'][language])
                else:
                    categories[f'des_{language}'].append(None)

            if category_id in dcs.keys():
                categories['id_cube_cat'].append(dcs[category_id])
            else:
                categories['id_cube_cat'].append(None)

            if 'categories' in categorie.keys():
                self.__merge_categories(categorie['categories'], category_id, dcs, categories)
        return categories

    # TODO
    def __dcs_to_dict(self, response_dcs):
        dcs = {}
        for dc in response_dcs:
            dcs[dc['CatCode']] = dc['IDCat']
        return dcs

    def __repr__(self):
        return f'{self.id} {self.version}'
