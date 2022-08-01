import logging
import sys

import pandas
import numpy as np
import requests.models

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Codelist:
    """ Clase que representa una codelist del M&D Manager.

    Args:
        session (:class:`requests.session.Session`): Sesión autenticada en la API.
        configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
         parámetros necesarios como la url de la API. Debe ser inicializado a partir del
         fichero de configuración configuracion/configuracion.yaml.
        codelist_id (:class:`String`): Identificador de la codelist.
        version (:class:`String`): Versión de la codelist.
        agency_id (:class:`String`): Agencia vinculada a la codelist.
        names (class: `Diccionario`): Diccionario con los nombres de la codelist en varios idiomas.
        des (class: `String`): Diccionario con las descripciones de la codelist en varios idiomas.
        init_data (:class:`Boolean`): True para traer todos los datos de la codelist,
         False para traer solo id, agencia y versión. Por defecto toma el valor False.

    Attributes:

        data (:obj:`DataFrame`) DataFrame con todos los códigos de la lista

    """

    def __init__(self, session, configuracion, codelist_id, agency_id, version, names, des, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = codelist_id
        self.version = version
        self.agency_id = agency_id
        self.names = names
        self.des = des
        self.codes = self.get() if init_data else None

    def get(self):
        codes = {'id': [], 'parent': []}
        for language in self.configuracion['languages']:
            codes[f'name_{language}'] = []
            codes[f'des_{language}'] = []
        try:
            response = self.session.post(f'{self.configuracion["url_base"]}NOSQL/codelist/',
                                         json={"id": self.id, "agencyId": self.agency_id, "version": self.version,
                                               "lang": "es", "pageNum": 1, "pageSize": 2147483647, "rebuildDb": False})

            response_data = response.json()['data']['codelists'][0]['codes']

        except KeyError:
            if 'data' in response.json().keys():
                self.logger.error('La codelist con id: %s está vacía', self.id)
            else:
                self.logger.error(
                    'Ha ocurrido un error mientras se cargaban los datos de la codelist con id: %s', self.id)
                self.logger.error(response.text)
            return pandas.DataFrame(data=codes, dtype='string')

        except Exception as e:
            raise e

        for code in response_data:
            for key in codes:
                try:
                    if key == 'id' or key == 'parent':
                        codes[key].append(code[key])
                    else:
                        language = key[-2:]
                        if 'name' in key:
                            codes[key].append(code['names'][language])
                        if 'des' in key:
                            codes[key].append(code['descriptions'][language])

                except Exception as e:
                    codes[key].append(None)
        return pandas.DataFrame(data=codes, dtype='string')

    def put(self, csv_file_route=None):
        upload_headers = self.session.headers.copy()
        # if csv_file_route:
        #     for language in self.configuracion['languages']:
        #         self.logger.info('Actualizando códigos en: %s', language)
        #         # Name and Description selection for each language.
        #         selection = self.codes[['id', 'name_' + language, 'des_' + language, 'parent']]
        #         selection.columns = ['Id', 'Name', 'Description', 'ParentCode']
        # else:
        with open(csv_file_route, 'r', encoding='utf-8') as csv:
            custom_data = str(
                {"type": "codelist", "identity": {"ID": self.id, "Agency": self.agency_id, "Version": self.version},
                 "lang": 'es',  # HE DADO POR HECHO QUE SE VA A SUBIR EN ESPANYOL, CUIDAO
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
                f'{self.configuracion["url_base"]}CheckImportedFileCsvItem', headers=upload_headers,
                data=body)

            response.raise_for_status()

        except Exception as e:
            raise e
        self.logger.info('Archivo subido correctamente')
        response = response.json()
        response['identity']['ID'] = self.id
        response['identity']['Agency'] = self.agency_id
        response['identity']['Version'] = self.version

        try:
            self.logger.info('Importando códigos a la lista')
            self.session.post(
                f'{self.configuracion["url_base"]}importFileCsvItem',
                json=response)

            # self.logger.info('Codigos Actualizados correctamente en: %s', language)

        except Exception as e:
            raise e
        self.logger.info('Códigos importados correctamente')
        ## Utilizar decoradores correctamente para los getters y setters sería clave.
        # la sintaxis tb se puede mejorar seguro.

    def init_codes(self):
        self.codes = self.get()

    def __repr__(self):
        return f'{self.agency_id} {self.id} {self.version}'

    def translate(self, traductor, translations_cache):
        columns = self.codes.columns[2:]
        to_be_translated = self.codes[columns]  # Discarding ID and PARENTCODE
        for column in columns:
            target_language = column[-2:]
            source_languages = self.configuracion['languages'].copy()
            source_languages.remove(target_language)
            source_language = source_languages[-1]
            source_column = column[:-2] + source_language

            to_be_translated_indices = self.codes[self.codes[column].isnull()][column].index
            print(self.codes[source_column][to_be_translated_indices])
            self.codes[column][to_be_translated_indices] = self.codes[source_column][to_be_translated_indices].map(
                lambda value: translations_cache[value][
                    target_language] if value in translations_cache.keys() else traductor.translate_text(value,
                                                                                                         target_lang=target_language))
        print(self.codes.to_string())
