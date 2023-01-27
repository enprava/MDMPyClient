import copy
import logging
import sys
import os
import pandas
import requests
import yaml
from ftfy import fix_encoding

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

        concepts (:obj:`DataFrame`): DataFrame con todos los conceptos del esquema


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
        self.concepts = self.get(init_data)
        self.concepts_to_upload = pandas.DataFrame(columns=['Id', 'ParentCode', 'Name', 'Description'], dtype='string')

    def get(self, init_data):
        concepts = {'id': [], 'parent': []}
        for language in self.configuracion['languages']:
            concepts[f'name_{language}'] = []
            concepts[f'des_{language}'] = []
        if init_data:
            try:
                response = self.session.get(
                    f'{self.configuracion["url_base"]}conceptScheme/{self.id}/{self.agency_id}/{self.version}')
                response_data = response.json()['data']['conceptSchemes'][0]['concepts']
            except KeyError:
                if 'data' in response.json().keys():
                    self.logger.warning('El esquema de conceptos con id: %s está vacío', self.id)
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


    def get_sdmx(self, directory):
        self.logger.info('Obteniendo esquema conceptual con id %s en formato sdmx', self.id)
        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}downloadMetadati/conceptScheme/{self.id}/{self.agency_id}/'
                f'{self.version}/structure/true/false/es')
            response.raise_for_status()
        except Exception as e:
            raise e
        path = os.path.join(directory, self.id + '.xml')
        with open(path, 'w', encoding='utf-8') as file:
            file.write(response.text)
            file.close()

    def add_concept(self, concept_id, parent, names, des):
        if concept_id.upper() not in self.concepts.id.values:
            if names:
                names = fix_encoding(names)
            if des:
                des = fix_encoding(des)
            self.concepts_to_upload.loc[len(self.concepts_to_upload)] = [concept_id.upper(), parent, names, des]

    def add_concepts(self, concepts):
        concepts.apply(
            lambda conceptos: self.add_code(conceptos['ID'], conceptos['PARENTCODE'], conceptos['NAME'],
                                            conceptos['DESCRIPTION']), axis=1)

    def put(self, lang='es'):
        to_upload = len(self.concepts_to_upload)
        if to_upload:
            csv = self.concepts_to_upload.drop_duplicates(subset='Id')
            to_upload = len(csv)
            self.logger.info('Se han detectado %s conceptos para subir al esquema con id %s', to_upload, self.id)
            csv = csv.to_csv(sep=';', index=False).encode(encoding='utf-8')
            columns = {"id": 0, "name": 2, "description": 3, "parent": 1, "order": -1, "fullName": -1,
                       "isDefault": -1}
            response = self.__upload_csv(csv, columns, lang=lang)
            self.__import_csv(response)
            # self.init_concepts()
            self.concepts_to_upload = self.concepts_to_upload[0:0]
        else:
            self.logger.info('El esquema de conceptos con id %s está actualizado', self.id)

    def __upload_csv(self, csv, columns, lang='es'):
        upload_headers = self.session.headers.copy()
        custom_data = str(
            {"type": "conceptScheme",
             "identity": {"ID": self.id, "Agency": self.agency_id, "Version": self.version},
             "lang": lang,
             "firstRowHeader": 'true',
             "columns": columns, "textSeparator": ";", "textDelimiter": '\"'})

        files = {'file': (
            'hehe.csv', csv, 'application/vnd.ms-excel', {}),
            'CustomData': (None, custom_data)}
        body, content_type = requests.models.RequestEncodingMixin._encode_files(files, {})

        upload_headers['Content-Type'] = content_type
        upload_headers['language'] = lang
        try:
            self.logger.info('Subiendo conceptos a la API')
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
            response = self.session.post(
                f'{self.configuracion["url_base"]}importFileCsvItem',
                json=json)
            response.raise_for_status()
        except Exception as e:
            raise e
        self.logger.info('Conceptos importados correctamente')

    def delete(self):
        self.logger.info('Eliminando el esquema de conceptos con id %s', self.id)
        try:
            response = self.session.delete(
                f'{self.configuracion["url_base"]}artefact/ConceptScheme/{self.id}/{self.agency_id}/{self.version}')
            response.raise_for_status()
        except Exception as e:
            raise e
        if response.text.lower() == 'true':
            self.logger.info('Esquema de conceptos eliminado correctamente')
        else:
            self.logger.info('El esquema de conceptos no ha sido eliminado debido a un error en el servidor')

    def translate(self):
        languages = copy.deepcopy(self.configuracion['languages'])

        concepts = self.__translate(self.concepts)
        columns = {"id": 0, "name": 2, "description": 3, "parent": 1, "order": -1, "fullName": -1,
                   "isDefault": -1}
        n_translations = len(concepts)
        self.logger.info('Se han traducido %s conceptos', n_translations)
        if n_translations:
            for language in languages:
                concepts_to_upload = concepts[['id', 'parent', f'name_{language}', f'des_{language}']].copy(deep=True)
                concepts_to_upload.columns = ['Id', 'Parent', 'Name', 'Description']
                csv = concepts_to_upload.to_csv(sep=';', index=False).encode(encoding='utf-8')
                response = self.__upload_csv(csv, columns, lang=language)
                self.__import_csv(response)
        self.concepts = concepts

    def __translate(self, data):
        self.logger.info('Iniciando proceso de traducción del esquema de conceptos con id %s', self.id)
        columns = data.columns[2:]
        concepts = data.copy()
        concepts_translated = pandas.DataFrame(columns=concepts.columns)
        for column in columns:
            target_language = column[-2:]
            source_languages = self.configuracion['languages'].copy()
            source_languages.remove(target_language)
            if target_language == 'en':
                target_language = 'EN-GB'
            source_language = source_languages[-1]
            source_column = column[:-2] + source_language
            to_be_translated_indexes = concepts[concepts[column].isnull()][column].index
            fake_indexes = concepts[concepts[source_column].isnull()][source_column].index
            to_be_translated_indexes = to_be_translated_indexes.difference(fake_indexes, sort=False)
            indexes_size = len(to_be_translated_indexes)
            if not indexes_size:
                continue
            concepts[column][to_be_translated_indexes] = concepts[source_column][to_be_translated_indexes].map(
                lambda value, tl=target_language: self.__get_translate(value, tl))
            with open(f'{self.configuracion["cache"]}', 'w', encoding='utf=8') as file:
                yaml.dump(self.translator_cache, file)
            concepts_translated = pandas.concat(
                [concepts_translated, concepts.iloc[to_be_translated_indexes]])  # Se guardan los conceptos traducidos
        self.logger.info('Proceso de traducción finalizado')
        return concepts_translated

    def __get_translate(self, value, target_language):
        self.logger.info('Traduciendo el término %s al %s', value, target_language)
        if valself.logger.info('Traduciendo el término %s al %s', value, target_language)
        if 'EN-GB' in target_language:
            target_language = 'en'
        if value in self.translator_cache and target_language in self.translator_cache[value]:
            self.logger.info('Valor encontrado en la caché de traducciones')
            translation = self.translator_cache[value][target_language]
        else:
            if 'en' in target_language:
                target_language = 'EN-GB'
            self.logger.info('Realizando petición a deepl para traducir el valor %s al %s', value, target_language)
            translation = str(self.translator.translate_text(value, target_lang=target_language))
            if 'EN-GB' in target_language:
                target_language = 'en'
            self.translator_cache[value] = {}
            self.translator_cache[value][target_language] = translation
        self.logger.info('Traducido el término %s como %s', value, translation)
        return translation
    def init_concepts(self):
        self.concepts = self.get(True)

    def __repr__(self):
        return f'{self.id} {self.version}'
