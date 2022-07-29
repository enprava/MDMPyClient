import logging
import sys

import pandas

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
        self.data = self.get_data() if init_data else None

    def get_data(self):
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
            code_id = code['id']
            code_parent = code['parent'] if 'parent' in code.keys() else None

            codes['id'].append(code_id)
            codes['parent'].append(code_parent)

            for language in self.configuracion['languages']:
                if language in code['names'].keys():
                    codes[f'name_{language}'].append(code['names'][language])
                else:
                    codes[f'name_{language}'].append(None)
                if 'descriptions' in code.keys() and language in code['descriptions'].keys():
                    codes[f'des_{language}'].append(code['descriptions'][language])
                else:
                    codes[f'des_{language}'].append(None)
        print(pandas.DataFrame(data=codes, dtype='string').to_string())
        return pandas.DataFrame(data=codes, dtype='string')

    def __repr__(self):
        return f'{self.id} {self.version}'
