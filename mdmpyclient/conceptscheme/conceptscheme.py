import copy
import logging
import sys

import pandas
import requests

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class ConceptScheme:
    """ Clase que representa un esquema de conceptos del M&D Manager.

    Args:
        session (:class:`requests.session.Session`): Sesión autenticada en la API.
        configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
         parámetros necesarios como la url de la API. Debe ser inicializado a partir del
         fichero de configuración configuracion/configuracion.yaml.
        cs_id (class: 'String'): Identificador del esquema de conceptos.
        agency_id (class: `String`): Identificador de la agencia vinculada.
        version (class: `String`): Version del esquema de conceptos.
        names (class: `Diccionario`): Diccionario con los nombres del esquema de conceptos
         en varios idiomas.
        des (class: `String`): Diccionario con las descripciones del esquema de conceptos
         en varios idiomas.
        init_data (class: `Boolean`): True para traer todos los datos del esquema de
         conceptos, False para traer solo id, agencia y versión. Por defecto toma el valor False.

    Attributes:

        concepts (:obj:`DataFrame`) DataFrame con todos los conceptos del esquema


    """

    def __init__(self, session, configuracion, translator, translator_cache, cs_id, agency_id, version, names, des,
                 init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.translator = translator
        self.translator_cache = translator_cache
        self.id = cs_id
        self.agency_id = agency_id
        self.version = version
        self.names = names
        self.des = des
        self.concepts = self.get() if init_data else None

    def get(self):
        concepts = {'id': [], 'parent': []}
        for language in self.configuracion['languages']:
            concepts[f'name_{language}'] = []
            concepts[f'des_{language}'] = []
        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}conceptScheme/{self.id}/{self.agency_id}/{self.version}')
            response_data = response.json()['data']['conceptSchemes'][0]['concepts']
        except KeyError:
            if 'data' in response.json().keys():
                self.logger.error('El esquema de conceptos con id: %s está vacío', self.id)
            else:
                self.logger.error(
                    'Ha ocurrido un error mientras se cargaban los datos de la codelist con id: %s', self.id)
                self.logger.error(response.text)
            return pandas.DataFrame(data=concepts, dtype='string')
        except Exception as e:
            raise e

        for concept in response_data:
            for key, column in concepts.items():
                try:
                    if 'id' in key or 'parent' in key:
                        column.append(concept[key])
                    else:
                        language = key[-2:]
                        if 'name' in key:
                            column.append(concept['names'][language])
                        if 'des' in key:
                            column.append(concept['descriptions'][language])

                except Exception:
                    column.append(None)
        return pandas.DataFrame(data=concepts, dtype='string')

    def put(self, data=None, lang='es'):
        languages = copy.deepcopy(self.configuracion['languages'])
        languages.remove(lang)
        csv = data.copy(deep=True)
        csv.columns = ['Id', 'Name', 'Description', 'ParentCode', 'order']
        csv = csv[['Id', 'Name', 'Description', 'ParentCode']]
        csv = csv.to_csv(sep=';', index=False).encode(encoding='utf-8')
        # csv.columns = ['ID', 'COD', 'NAME', 'DESCRIPTION', 'PARENTCODE', 'ORDER']
        columns = {"id": 0, "name": 2, "description": 3, "parent": 4, "order": -1, "fullName": -1,
                   "isDefault": -1}
        response = self.__upload_csv(csv, columns, lang=lang)
        self.__import_csv(response)
        self.init_concepts()

        concepts = self.translate()
        columns = {"id": 0, "name": 2, "description": 3, "parent": 1, "order": -1, "fullName": -1,
                   "isDefault": -1}
        for language in languages:
            codes_to_upload = concepts.copy(deep=True)
            codes_to_upload = codes_to_upload[['id', 'parent', f'name_{language}', f'des_{language}']]
            codes_to_upload.columns = ['Id', 'Parent', 'Name', 'Description']
            csv = codes_to_upload.to_csv(sep=';', index=False).encode(encoding='utf-8')

            response = self.__upload_csv(csv, columns, lang=language)
            self.__import_csv(response)
        self.init_concepts()

    def __upload_csv(self, csv, columns, lang='es'):
        upload_headers = self.session.headers.copy()
        custom_data = str(
            {"type": "conceptScheme",
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
            self.logger.info('Subiendo conceptos al esquema con id %s', self.id)
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
            self.logger.info('Importando conceptos al esquema')
            self.session.post(
                f'{self.configuracion["url_base"]}importFileCsvItem',
                json=json)

        except Exception as e:
            raise e
        self.logger.info('Conceptos importados correctamente')

    def translate(self):
        columns = self.codes.columns[2:]
        concepts = self.concepts.copy()
        for column in columns:
            target_language = column[-2:]

            self.logger.info('Traduciendo los códigos de la codelist %s al %s', self.id, target_language)

            source_languages = self.configuracion['languages'].copy()
            source_languages.remove(target_language)
            if target_language == 'en':
                target_language = 'EN-GB'
            source_language = source_languages[-1]
            source_column = column[:-2] + source_language
            to_be_translated_indexes = concepts[concepts[column].isnull()][column].index
            fake_indexes = concepts[concepts[source_column].isnull()][source_column].index
            to_be_translated_indexes = to_be_translated_indexes.difference(fake_indexes, sort=False)
            concepts[column][to_be_translated_indexes] = concepts[source_column][to_be_translated_indexes].map(
                lambda value, tl=target_language: self.__get_translate(self.translator, value, tl,
                                                                       self.translator_cache))
        return concepts

    def __get_translate(self, translator, value, target_language, translations_cache):
        if value in translations_cache:
            translation = translations_cache[value][target_language]
        else:
            translation = str(translator.translate_text(value, target_lang=target_language))
            translations_cache[value] = {}
            translations_cache[value][target_language] = translation
        return translation

    def init_concepts(self):
        self.concepts = self.get()

    def __repr__(self):
        return f'{self.id} {self.version}'
