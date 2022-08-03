import logging
import sys

import pandas
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
            for key, column in codes.items():
                try:
                    if 'id' in key or 'parent' in key:
                        column.append(code[key])
                    else:
                        language = key[-2:]
                        if 'name' in key:
                            column.append(code['names'][language])
                        if 'des' in key:
                            column.append(code['descriptions'][language])

                except Exception:
                    column.append(None)
        return pandas.DataFrame(data=codes, dtype='string')

    def put(self, csv_file_path=None, lang='es'):
        if csv_file_path:
            with open(csv_file_path, 'r', encoding='utf-8') as csv:
                columns = {"id": 0, "name": 1, "description": 2, "parent": 3, "order": -1, "fullName": -1,
                           "isDefault": -1}
                response = self.__upload_csv(csv, columns, csv_file_path=csv_file_path, lang=lang)
                self.__export_csv(response)
        else:
            for language in self.configuracion['languages']:
                csv = self.codes.copy()
                for column_name in csv.columns[2:]:
                    if language not in column_name[-2:]:
                        del csv[column_name]
                csv.columns = ['Id', 'ParentCode', 'Name', 'Description']
                path = f'traduccion_{self.id}_{language}.csv'
                csv = csv.to_csv(sep=';', index=False).encode(encoding='utf-8')
                columns = {"id": 0, "parent": 1, "name": 2, "description": 3, "order": -1, "fullName": -1,
                           "isDefault": -1}
                response = self.__upload_csv(csv, columns, csv_file_path=path, lang=language)
                self.__export_csv(response)

    def __upload_csv(self, csv, columns, csv_file_path='', lang='es'):
        upload_headers = self.session.headers.copy()
        custom_data = str(
            {"type": "codelist",
             "identity": {"ID": self.id, "Agency": self.agency_id, "Version": self.version},
             "lang": lang,
             "firstRowHeader": 'true',
             "columns": columns, "textSeparator": ";", "textDelimiter": 'null'})

        files = {'file': (
            csv_file_path, csv, 'application/vnd.ms-excel', {}),
            'CustomData': (None, custom_data)}
        body, content_type = requests.models.RequestEncodingMixin._encode_files(files, {})
        upload_headers['Content-Type'] = content_type
        upload_headers['language'] = lang
        try:
            self.logger.info('Subiendo el archivo %s a la API', csv_file_path)
            response = self.session.post(
                f'{self.configuracion["url_base"]}CheckImportedFileCsvItem', headers=upload_headers,
                data=body)

            response.raise_for_status()

        except Exception as e:
            raise e
        response = response.json()
        response['identity']['ID'] = self.id
        response['identity']['Agency'] = self.agency_id
        response['identity']['Version'] = self.version
        return response

    def __export_csv(self, json):
        try:
            self.logger.info('Importando conceptos al esquema')
            self.session.post(
                f'{self.configuracion["url_base"]}importFileCsvItem',
                json=json)

        except Exception as e:
            raise e
        self.logger.info('Conceptos importados correctamente')
        ## Utilizar decoradores correctamente para los getters y setters sería clave.
        # la sintaxis tb se puede mejorar seguro.

    def init_codes(self):
        self.codes = self.get()

    def __repr__(self):
        return f'{self.agency_id} {self.id} {self.version}'

    def translate(self, translator, translations_cache):
        columns = self.codes.columns[2:]
        codes = self.codes.copy()
        for column in columns:
            target_language = column[-2:]
            source_languages = self.configuracion['languages'].copy()
            source_languages.remove(target_language)
            if target_language == 'en':
                target_language = 'EN-GB'
            source_language = source_languages[-1]
            source_column = column[:-2] + source_language

            to_be_translated_indexes = codes[codes[column].isnull()][column].index
            fake_indexes = codes[codes[source_column].isnull()][source_column].index
            to_be_translated_indexes = to_be_translated_indexes.difference(fake_indexes, sort=False)
            codes[column][to_be_translated_indexes] = codes[source_column][to_be_translated_indexes].map(
                lambda value: self.__get_translate(translator, value, target_language, translations_cache))
        return codes

    def __get_translate(self, translator, value, target_language, translations_cache):
        return str(translator.translate_text(value, target_lang=target_language))
