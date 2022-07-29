import logging
import sys

import pandas
import numpy as np
import pandas as pd

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
        init_data (:class:`Boolean`): True para traer todos los datos de la codelist,
         False para traer solo id, agencia y versión. Por defecto toma el valor False.

    Attributes:



    """

    def __init__(self, session, configuracion, codelist_id, agency_id, version, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = codelist_id
        self.version = version
        self.agency_id = agency_id
        self.codes = self.init_codes() if init_data else None

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
            self.logger.error(
                'Ha ocurrido un error mientras se cargaban los datos de la codelist con id: %s', self.id)
            self.logger.error(response.text)
            return codes

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

    def put(self):
        headers = self.session.headers
        upload_headers = self.session.headers
        del upload_headers['Content-Type']
        for language in self.configuracion['languages']:
            self.logger.info('Actualizando códigos en: %s', language)
            # Name and Description selection for each language.
            selection = self.codes[['id', 'name_' + language, 'des_' + language, 'parent']]
            selection.columns = ['Id', 'Name', 'Description', 'ParentCode']

            csv = selection.to_csv(index=False, sep=';')
            custom_data = str(
                {"type": "codelist", "identity": {"ID": self.id, "Agency": self.agency_id, "Version": self.version},
                 "lang": 'es',
                 "firstRowHeader": 'true',
                 "columns": {"id": 0, "name": 1, "description": 2, "parent": 3, "order": -1, "fullName": -1,
                             "isDefault": -1}, "textSeparator": ";", "textDelimiter": 'null'}
            )
            files = {'file': (
                'USELESSss.csv', csv, 'application/vnd.ms-excel', {}),
                'CustomData': (None, custom_data)}
            try:
                response = self.session.post(
                    f'{self.configuracion["url_base"]}CheckImportedFileCsvItem', headers=upload_headers,
                    files=files)

                response.raise_for_status()

            except Exception as e:
                raise e

            response = response.json()
            response['identity']['ID'] = self.id
            response['identity']['Agency'] = self.agency_id
            response['identity']['Version'] = self.version

            try:
                response = self.session.post(
                    f'{self.configuracion["url_base"]}importFileCsvItem',
                    json=response)

                self.logger.info('Codigos Actualizados correctamente en: %s', language)

            except Exception as e:
                raise e

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
            self.codes[column][to_be_translated_indices] = self.codes[source_column][to_be_translated_indices].map(
                lambda value: translations_cache[value][
                    target_language] if value in translations_cache.keys() else traductor.translate_text(value,
                                                                                                         target_lang=target_language))
