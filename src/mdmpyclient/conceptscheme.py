import logging
import sys

import pandas

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
        self.data = self.get_data() if init_data else None

    def get_data(self):
        concepts = {'id': []}
        for language in self.configuracion['languages']:
            concepts[f'name_{language}'] = []
            concepts[f'des_{language}'] = []
        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}conceptScheme/{self.id}/{self.agency_id}/{self.version}')
            response_data = response.json()['data']['conceptSchemes'][0]['concepts']
        except KeyError:
            self.logger.error(
                'Ha ocurrido un error mientras se cargaban los datos del esquema de conceptos con id: %s', self.id)
            self.logger.error(response.text)
            return concepts
        except Exception as e:
            raise e
        for concept in response_data:
            concept_id = concept['id']
            concepts['id'].append(concept_id)

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

    def __repr__(self):
        return f'{self.id} {self.version}'
