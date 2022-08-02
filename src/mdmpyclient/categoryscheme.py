import logging
import sys

import pandas
import requests

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
        agency_id (class: `String`): Identificador de la agencia vinculada.
        version (class: `String`): Versión del esquema de categorías.
        names (class: `Diccionario`): Diccionario con los nombres del esquema de categorías
         en varios idiomas.
        des (class: `String`): Diccionario con las descripciones del esquema de categorías
         en varios idiomas.
        init_data (class: `Boolean`): True para traer todos los datos del esquema de
         categorías, False para traer solo id, agencia y versión. Por defecto toma el valor False.

    Attributes:
        categories (obj: `DataFrame`): DataFrame con todas las categorías del esquema

    """

    def __init__(self, session, configuracion, category_scheme_id, agency_id, version, names, des, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = category_scheme_id
        self.agency_id = agency_id
        self.version = version
        self.names = names
        self.des = des
        self.categories = self.get() if init_data else None

    def get(self):
        categories = {'id': [], 'parent': [], 'id_cube_cat': []}
        for language in self.configuracion['languages']:
            categories[f'name_{language}'] = []
            categories[f'des_{language}'] = []

        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}categoryScheme/{self.id}/{self.agency_id}/{self.version}')
            response_data = response.json()['data']['categorySchemes'][0]['categories']
            response_dcs = self.session.get(f'{self.configuracion["url_base"]}dcs').json()

        except KeyError:
            self.logger.error(
                'Ha ocurrido un error mientras se cargaban los datos del esquema de categorías con id: %s', self.id)
            self.logger.error(response.text)
            return pandas.DataFrame(data=categories, dtype='string')
        except Exception as e:
            raise e

        dcs = self.__dcs_to_dict(response_dcs)
        categories = self.__merge_categories(response_data, None, dcs, categories)
        return pandas.DataFrame(data=categories, dtype='string')

    def put(self, csv_file_route=None):
        upload_headers = self.session.headers.copy()
        with open(csv_file_route, 'r', encoding='utf-8') as csv:
            custom_data = str(
                {"type": "categoryScheme",
                 "identity": {"ID": self.id, "Agency": self.agency_id, "Version": self.version},
                 "lang": 'en',
                 "firstRowHeader": 'true',
                 "columns": {"id": 0, "name": 1, "description": 2, "parent": 3, "order": -1, "fullName": -1,
                             "isDefault": -1}, "textSeparator": ";", "textDelimiter": 'null'}
            )
            files = {'file': (
                csv_file_route, csv, 'application/vnd.ms-excel', {}),
                'CustomData': (None, custom_data)}
            body, content_type = requests.models.RequestEncodingMixin._encode_files(files, {})

        upload_headers['Content-Type'] = content_type
        try:
            self.logger.info('Subiendo el archivo %s a la API', csv_file_route)
            response = self.session.post(
                f'{self.configuracion["url_base"]}CheckImportedFileCsvItem', headers=upload_headers, data=body)

            response.raise_for_status()

        except Exception as e:
            raise e
        self.logger.info('Archivo subido correctamente')
        response = response.json()
        response['identity']['ID'] = self.id
        response['identity']['Agency'] = self.agency_id
        response['identity']['Version'] = self.version

        try:
            self.logger.info('Importando categorías al esquema')
            self.session.post(f'{self.configuracion["url_base"]}importFileCsvItem', json=response)
        except Exception as e:
            raise e
        self.logger.info('Categorías importadas correctamente')

    def __merge_categories(self, response, parent, dcs, categories):
        for categorie in response:
            category_id = categorie['id']

            categories['id'].append(category_id)
            categories['parent'].append(parent)

            for language in self.configuracion['languages']:
                if language in categorie['names'].keys():
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

    def init_data(self):
        self.categories = self.get()

    def __repr__(self):
        return f'{self.id} {self.version}'
