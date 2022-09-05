import copy
import logging
import sys

import pandas
import requests.models
import yaml

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Codelist:
    """ Clase que representa una codelist del M&D Manager.

    Args:
        session (:class:`requests.session.Session`): Sesión autenticada en la API.
        configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
         parámetros necesarios como la url de la API. Debe ser inicializado a partir del
         fichero de configuración configuracion/configuracion.yaml.
        id (:class:`String`): Identificador de la codelist.
        version (:class:`String`): Versión de la codelist.
        agency_id (:class:`String`): Agencia vinculada a la codelist.
        names (class: `Diccionario`): Diccionario con los nombres de la codelist en varios idiomas.
        des (class: `String`): Diccionario con las descripciones de la codelist en varios idiomas.
        init_data (:class:`Boolean`): True para traer todos los datos de la codelist,
         False para no traerlos. Por defecto toma el valor False.

    Attributes:

        codes (:obj:`DataFrame`) DataFrame con todos los códigos de la lista

    """

    def __init__(self, session, configuracion, translator, translator_cache, codelist_id, agency_id, version, names,
                 des, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.translator = translator
        self.translator_cache = translator_cache
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

    def put(self, data=None, lang='es'):
        languages = copy.deepcopy(self.configuracion['languages'])
        languages.remove(lang
                         )
        csv = data.copy(deep=True)
        csv.columns = ['Id', 'Name', 'Description', 'ParentCode', 'order']
        csv = csv[['Id', 'Name', 'Description', 'ParentCode']]
        csv = csv.to_csv(sep=';', index=False).encode(encoding='utf-8')
        # csv.columns = ['ID', 'COD', 'NAME', 'DESCRIPTION', 'PARENTCODE', 'ORDER']
        columns = {"id": 0, "name": 2, "description": 3, "parent": 4, "order": -1, "fullName": -1,
                   "isDefault": -1}
        response = self.__upload_csv(csv, columns, lang=lang)
        self.__import_csv(response)
        self.init_codes()

        codes = self.translate()
        columns = {"id": 0, "name": 2, "description": 3, "parent": 1, "order": -1, "fullName": -1,
                   "isDefault": -1}
        for language in languages:
            codes_to_upload = codes.copy(deep=True)
            codes_to_upload = codes_to_upload[['id', 'parent', f'name_{language}', f'des_{language}']]
            codes_to_upload.columns = ['Id', 'Parent', 'Name', 'Description']
            csv = codes_to_upload.to_csv(sep=';', index=False).encode(encoding='utf-8')

            response = self.__upload_csv(csv, columns, lang=language)
            self.__import_csv(response)
        self.init_codes()

    def __upload_csv(self, csv, columns, lang='es'):
        upload_headers = self.session.headers.copy()
        custom_data = str(
            {"type": "codelist",
             "identity": {"ID": self.id, "Agency": self.agency_id, "Version": self.version},
             "lang": lang,
             "firstRowHeader": 'true',
             "columns": columns, "textSeparator": ";", "textDelimiter": 'null'})

        files = {'file': (
            'hehe.csv', csv, 'application/vnd.ms-excel', {}),
            'CustomData': (None, custom_data)}
        body, content_type = requests.models.RequestEncodingMixin._encode_files(files, {})
        upload_headers['Content-Type'] = content_type
        upload_headers['language'] = lang
        try:
            self.logger.info('Subiendo códigos a la codelist con id: %s', self.id)
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

    def __import_csv(self, json):
        try:
            self.logger.info('Importando códigos al esquema')
            self.session.post(
                f'{self.configuracion["url_base"]}importFileCsvItem',
                json=json)

        except Exception as e:
            raise e
        self.logger.info('Códigos importados correctamente')
        ## Utilizar decoradores correctamente para los getters y setters sería clave.
        # la sintaxis tb se puede mejorar seguro.

    def init_codes(self):
        self.codes = self.get()

    def __repr__(self):
        return f'{self.agency_id} {self.id} {self.version}'

    def translate(self):
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
            self.logger.info('Traduciendo los códigos de la codelist %s al %s', self.id, target_language)
            codes[column][to_be_translated_indexes] = codes[source_column][to_be_translated_indexes].map(
                lambda value, tl=target_language: self.__get_translate(self.translator, value, tl,
                                                                       self.translator_cache))
            with open(f'{self.configuracion["cache"]}', 'w', encoding='utf=8') as file:
                yaml.dump(self.translator_cache, file)
        return codes

    def __get_translate(self, translator, value, target_language, translations_cache):
        self.logger.info('Traduciendo el término %s al %s', value, target_language)
        if value in translations_cache:
            if 'EN-GB' in target_language:
                target_language = 'en'
            self.logger.info('Valor encontrado en la caché de traducciones')
            translation = translations_cache[value][target_language]
        else:
            self.logger.info('Realizando petición a deepl para traducir el valor %s al %s', value, target_language)
            translation = str(translator.translate_text(value, target_lang=target_language))
            if 'EN-GB' in target_language:
                target_language = 'en'
            translations_cache[value] = {}
            translations_cache[value][target_language] = translation
        return translation
