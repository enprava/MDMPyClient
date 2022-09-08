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
        self.codes = self.get(init_data)
        self.codes_to_upload = pandas.DataFrame(columns=['Id', 'ParentCode', 'Name', 'Description'])

    def get(self, init_data):
        codes = {'id': [], 'parent': []}
        for language in self.configuracion['languages']:
            codes[f'name_{language}'] = []
            codes[f'des_{language}'] = []
        if init_data:
            try:
                # response = self.session.post(f'{self.configuracion["url_base"]}NOSQL/codelist/',
                #                              json={"id": self.id, "agencyId": self.agency_id, "version": self.version,
                #                                    "lang": "es", "pageNum": 1, "pageSize": 2147483647,
                #                                    "rebuildDb": False})
                response = self.session.get(
                    f'{self.configuracion["url_base"]}codelist/{self.id}/{self.agency_id}/{self.version}/1/2147483647')
                response_data = response.json()['data']['codelists'][0]['codes']

            except KeyError:
                if 'data' in response.json().keys():
                    self.logger.warning('La codelist con id: %s está vacía', self.id)
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

    def add_code(self, code_id, parent, name, des):
        if code_id.upper() not in self.codes.id.values:
            self.codes_to_upload.loc[len(self.codes_to_upload)] = [code_id.upper(), parent, name, des]

    def add_codes(self, codes):
        codes.apply(
            lambda codigos: self.add_code(codigos['ID'], codigos['PARENTCODE'], codigos['NAME'],
                                          codigos['DESCRIPTION']), axis=1)

    def put(self, lang='es'):
        to_upload = len(self.codes_to_upload)
        if to_upload:
            self.logger.info('Se han detectado %s códigos para subir a la codelist con id %s', to_upload, self.id)
            csv = self.codes_to_upload
            csv = csv.to_csv(sep=';', index=False).encode(encoding='utf-8')
            columns = {"id": 0, "name": 2, "description": 3, "parent": 1, "order": -1, "fullName": -1,
                       "isDefault": -1}
            response = self.__upload_csv(csv, columns, to_upload, lang=lang)
            self.__import_csv(response)
            self.init_codes()
            self.codes_to_upload = self.codes_to_upload[0:0]

            if self.configuracion['translate']:
                languages = copy.deepcopy(self.configuracion['languages'])
                languages.remove(lang)

                codes = self.translate(self.codes)

                columns = {"id": 0, "name": 2, "description": 3, "parent": 1, "order": -1, "fullName": -1,
                           "isDefault": -1}
                for language in languages:
                    codes_to_upload = codes.copy(deep=True)
                    codes_to_upload = codes_to_upload[['id', 'parent', f'name_{language}', f'des_{language}']]
                    codes_to_upload.columns = ['Id', 'Parent', 'Name', 'Description']
                    to_upload = len(codes_to_upload)
                    csv = codes_to_upload.to_csv(sep=';', index=False).encode(encoding='utf-8')

                    response = self.__upload_csv(csv, columns, to_upload, lang=language)
                    self.__import_csv(response)
                self.init_codes()
        else:
            self.logger.info('La codelist con id %s está actualizada', self.id)

    def __upload_csv(self, csv, columns, to_upload, lang='es'):
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
            self.logger.info('Se van a subir %s códigos a la codelist con id: %s', to_upload, self.id)
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
            self.logger.info('Importando códigos a la lista')
            self.session.post(
                f'{self.configuracion["url_base"]}importFileCsvItem',
                json=json)

        except Exception as e:
            raise e
        self.logger.info('Códigos importados correctamente')
        ## Utilizar decoradores correctamente para los getters y setters sería clave.
        # la sintaxis tb se puede mejorar seguro.

    def init_codes(self):
        self.codes = self.get(True)

    def __repr__(self):
        return f'{self.agency_id} {self.id} {self.version}'

    def translate(self, data):
        self.logger.info('Iniciando proceso de traducción para la codelist con id %s', self.id)
        columns = data.columns[2:]
        codes = data.copy()
        codes_translated = pandas.DataFrame(columns=codes.columns)
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
            if not len(to_be_translated_indexes):
                continue
            codes[column][to_be_translated_indexes] = codes[source_column][to_be_translated_indexes].map(
                lambda value, tl=target_language: self.__get_translate(value, tl))

            with open(f'{self.configuracion["cache"]}', 'w', encoding='utf=8') as file:
                yaml.dump(self.translator_cache, file)
            # codes_translated = pandas.concat([codes_translated, codes.iloc[to_be_translated_indexes]]) # Se guardan los codigos traducidos
        return codes_translated

    def __get_translate(self, value, target_language):
        self.logger.info('Traduciendo el término %s al %s', value, target_language)
        if value in self.translator_cache:
            if 'EN-GB' in target_language:
                target_language = 'en'
            self.logger.info('Valor encontrado en la caché de traducciones')
            translation = self.translator_cache[value][target_language]
        else:
            self.logger.info('Realizando petición a deepl para traducir el valor %s al %s', value, target_language)
            translation = str(self.translator.translate_text(value, target_lang=target_language))
            if 'EN-GB' in target_language:
                target_language = 'en'
            self.translator_cache[value] = {}
            self.translator_cache[value][target_language] = translation
        self.logger.info('Traducido el término %s como %s', value, translation)
        return translation
