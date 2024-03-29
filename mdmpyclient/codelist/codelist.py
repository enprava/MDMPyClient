import copy
import logging
import sys
import os
import pandas
import requests.models
import yaml

from ftfy import fix_encoding

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

        codes (:obj:`DataFrame`): DataFrame con todos los códigos de la lista

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
        self.codes_to_upload = pandas.DataFrame(columns=['Id', 'ParentCode', 'Name', 'Description'], dtype='string')

    def get(self, init_data):
        codes = {'id': [], 'parent': []}
        for language in self.configuracion['languages']:
            codes[f'name_{language}'] = []
            codes[f'des_{language}'] = []
        if init_data:
            try:
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

    def delete(self):
        self.logger.info('Eliminando la codelist con id %s', self.id)
        try:
            response = self.session.delete(
                f'{self.configuracion["url_base"]}artefact/Codelist/{self.id}/{self.agency_id}/{self.version}')
            response.raise_for_status()
        except Exception as e:
            raise e
        if response.text.lower() == 'true':
            self.logger.info('Codelist eliminada correctamente')
        else:
            self.logger.info('La codelist no ha sido eliminada debido a un error en el servidor')

    def get_sdmx(self, directory):
        """

        Args:
            directory: (:class:`String`) Directorio en el que se escribira el fichero con la codelist en formato sdmx

        Returns: None

        """
        self.logger.info('Obteniendo codelist con id %s en formato sdmx', self.id)
        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}downloadMetadati/codelist/{self.id}/{self.agency_id}/'
                f'{self.version}/structure/true/false/es')
            response.raise_for_status()
        except Exception as e:
            raise e
        path = os.path.join(directory, self.id + '.xml')
        with open(path, 'w', encoding='utf-8') as file:
            file.write(response.text)
            file.close()

    def add_code(self, code_id, parent, name, des):
        """

        Args:
            code_id: Id del código que queremos añadir
            parent: Parent del código que queremos añadir
            name: Nombre del código que queremos añadir
            des: Descricpión del código que queremos añadir

        Returns: None

        """
        if code_id.upper() not in self.codes.id.values:
            if name:
                name = fix_encoding(name)
            if des:
                des = fix_encoding(des)
            self.codes_to_upload.loc[len(self.codes_to_upload)] = [code_id.upper(), parent, name, des]

    def add_codes(self, codes):
        """

        Args:
            codes: (:class:`pandas.Dataframe`) Códigos a traducir

        Returns: None

        """
        try:  # SOLUCION TEMPORAL QUE HABRA QUE CAMBIAR
            codes.columns = ['Id', 'Name', 'Description', 'ParentCode', 'ORDER']
        except:
            codes.columns = ['Id', 'Name', 'Description', 'ParentCode']
        codes = codes[['Id', 'Name', 'Description', 'ParentCode']]
        codigos = codes[~codes['Id'].isin(self.codes['id'])]
        self.codes_to_upload = pandas.concat([self.codes_to_upload, codigos], ignore_index=True)

    def put(self, lang='es'):
        to_upload = len(self.codes_to_upload)
        if to_upload:
            csv = self.codes_to_upload.drop_duplicates(subset='Id')
            to_upload = len(csv)
            self.logger.info('Se han detectado %s códigos para subir a la codelist con id %s', to_upload, self.id)
            csv = csv.to_csv(sep=';', index=False, encoding='utf_8')
            columns = {"id": 0, "name": 2, "description": 3, "parent": 1, "order": -1, "fullName": -1,
                       "isDefault": -1}
            response = self.__upload_csv(csv, columns, lang=lang)
            self.__import_csv(response)
            # self.init_codes()
            self.codes_to_upload = self.codes_to_upload[0:0]
        else:
            self.logger.info('La codelist con id %s está actualizada', self.id)

    def __upload_csv(self, csv, columns, lang='es'):
        upload_headers = self.session.headers.copy()
        custom_data = str(
            {"type": "codelist",
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
            self.logger.info('Subiendo códigos a la API')
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
            response = self.session.post(
                f'{self.configuracion["url_base"]}importFileCsvItem',
                json=json)
            response.raise_for_status()
        except Exception as e:
            raise e
        self.logger.info('Códigos importados correctamente')
        ## Utilizar decoradores correctamente para los getters y setters sería clave.
        # la sintaxis tb se puede mejorar seguro.

    def init_codes(self):
        self.codes = self.get(True)

    def __repr__(self):
        return f'{self.agency_id} {self.id} {self.version}'

    def translate(self):
        """
        Hace uso de la variable 'languages' definida en el fichero de configuración

        Returns: (:class:`pandas.Dataframe`) Con los códigos traducidos

        """
        languages = copy.deepcopy(self.configuracion['languages'])

        codes = self.__translate(self.codes)
        codes = codes.drop_duplicates('id', keep='last')  # TODO
        columns = {"id": 0, "name": 2, "description": 3, "parent": 1, "order": -1, "fullName": -1,
                   "isDefault": -1}
        n_translations = len(codes)
        self.logger.info('Se han traducido %s códigos', n_translations)
        if n_translations:
            for language in languages:
                codes_to_upload = codes[['id', 'parent', f'name_{language}', f'des_{language}']].copy(deep=True)
                codes_to_upload.columns = ['Id', 'Parent', 'Name', 'Description']
                csv = codes_to_upload.to_csv(sep=';', index=False).encode(encoding='utf-8')
                response = self.__upload_csv(csv, columns, lang=language)
                self.__import_csv(response)
        self.codes = codes

    def __translate(self, data):
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
            indexes_size = len(to_be_translated_indexes)
            if not indexes_size:
                continue
            codes[column][to_be_translated_indexes] = codes[source_column][to_be_translated_indexes].map(
                lambda value, tl=target_language: self.__get_translate(value, tl))

            with open(f'{self.configuracion["cache"]}', 'w', encoding='utf=8') as file:
                yaml.dump(self.translator_cache, file)
            codes_translated = pandas.concat(
                [codes_translated, codes.iloc[to_be_translated_indexes]])  # Se guardan los codigos traducidos
        return codes_translated

    def __get_translate(self, value, target_language):
        self.logger.info('Traduciendo el término %s al %s', value, target_language)
        if 'EN-GB' in target_language:
            target_language = 'en'
        try:
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
        except TypeError:
            if value == 'España':
                translation = 'Spain'
        return translation.replace("\n", ' ')
