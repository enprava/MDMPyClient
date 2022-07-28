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
        agency_id (class: `String`): Identificador de la agencia vinculada
        version (class: `String`): Version del esquema de conceptos
        init_data (class: `Boolean`): True para traer todos los datos del esquema de
         conceptos, False para traer solo id, agencia y versión. Por defecto toma el valor False.

    Attributes:


    """

    def __init__(self, session, configuracion, cs_id, agency_id, version, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = cs_id
        self.agency_id = agency_id
        self.version = version
        self.data = self.get_data() if init_data else None

    def get_data(self):
        concepts = {'id': [], 'name_es': [], 'name_en': [], 'des_es': [], 'des_en': []}
        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}conceptScheme/{self.id}/{self.agency_id}/{self.version}').json()[
                'data']['conceptSchemes'][0]['concepts']
        except KeyError:
            self.logger.warning(
                'Ha ocurrido un error inesperado mientras se cargaban los datos del esquema de conceptos con id: %s',
                self.id)
            return concepts

        for concept in response:
            concept_id = concept['id']
            name_es = concept['names']['es'] if 'es' in concept['names'].keys() else None
            name_en = concept['names']['en'] if 'en' in concept['names'].keys() else None

            concepts['id'].append(concept_id)
            concepts['name_es'].append(name_es)
            concepts['name_en'].append(name_en)
            if 'descriptions' in concept.keys():
                des_es = concept['descriptions']['es'] if 'es' in concept['descriptions'].keys() else None
                des_en = concept['descriptions']['en'] if 'en' in concept['descriptions'].keys() else None

            else:
                des_es = None
                des_en = None
            concepts['des_es'].append(des_es)
            concepts['des_en'].append(des_en)
        return pandas.DataFrame(data=concepts, dtype='string')

    def __repr__(self):
        return f'{self.id} {self.version}'
