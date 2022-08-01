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

        data (:obj:`DataFrame`) DataFrame con todos los conceptos del esquema


    """

    def __init__(self, session, configuracion, cs_id, agency_id, version, names, des, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
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
            concept_id = concept['id']
            concept_parent = concept['parent'] if 'parent' in concept.keys() else None
            concepts['id'].append(concept_id)
            concepts['parent'].append(concept_parent)

            for language in self.configuracion['languages']:
                if language in concept['names'].keys():
                    concepts[f'name_{language}'].append(concept['names'][language])
                else:
                    concepts[f'name_{language}'].append(None)
                if 'descriptions' in concept.keys() and language in concept['descriptions'].keys():
                    concepts[f'des_{language}'].append(concept['descriptions'][language])
                else:
                    concepts[f'des_{language}'].append(None)
        return pandas.DataFrame(data=concepts, dtype='string')

    def put(self, csv_file_route=None):
        upload_headers = self.session.headers.copy()
        with open(csv_file_route, 'r', encoding='utf-8') as csv:
            custom_data = str(
                {"type": "conceptScheme", "identity": {"ID": self.id, "Agency": self.agency_id, "Version": self.version},
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
            self.logger.info('Importando conceptos al esquema')
            self.session.post(
                f'{self.configuracion["url_base"]}importFileCsvItem',
                json=response)

        except Exception as e:
            raise e
        self.logger.info('Conceptos importados correctamente')

    def init_concepts(self):
        self.concepts = self.get()

    def __repr__(self):
        return f'{self.id} {self.version}'
