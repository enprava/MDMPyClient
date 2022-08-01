import logging
import sys

import pandas

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Cube:
    """ Clase que representa un Cubo del M&D Manager.

           Args:
               session (:class:`requests.session.Session`): Sesión autenticada en la API.
               configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                fichero de configuración configuracion/configuracion.yaml.
               cube_id (class: 'Integer'): Identificador del cubo.
               cube_code (class: `String`): Código del cubo. Coincide con el ID que se le da cuando
                se crea desde el cliente
               cat_id (class: `Integer`): ID de la categoría a la que pertenece el cubo. La categoría
                perteneciente al esquema de categorías de los cubos en la api se refieren a estas como
                como 'dcs'.
               names (class: `Diccionario`): Diccionario con los nombres del cubo en varios idiomas.
               init_data (class: `Boolean`): True para traer todos los datos del cubo,
                False para traer solo id, agencia y versión. Por defecto toma el valor False.

           Attributes:


           """

    def __init__(self, session, configuracion, cube_id, cube_code, cat_id, dsd_code, names, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.cube_id = cube_id
        self.cube_code = cube_code
        self.cat_id = cat_id
        self.dsd_code = dsd_code
        self.names = names
        self.data = self.get_data() if init_data else None

    def get_data(self):
        data = {}
        request_data = {'FilterTable': [], 'PageNum': 1, 'PageSize': 2147483647, 'SortByDesc': False, 'SortCols': None}
        try:
            response = self.session.post(
                f'{self.configuracion["url_base"]}getTablePreview/Dataset_{self.cube_id}_ViewCurrentData',
                json=request_data)
            response_data = response.json()
        except KeyError:
            self.logger.error('Ha ocurrido un error mientras se cargaban los datos del cubo con id: %s', self.cube_id)
            self.logger.error(response.text)
            return data
        except Exception as e:
            raise e

        for column in response_data['Columns']:
            data[column] = []

        for row in response_data['Data']:
            for measure in row.keys():
                data[measure].append(row[measure])
        return pandas.DataFrame(data=data, dtype='string')

    def __repr__(self):
        return f'ID: {self.cube_id} name: {self.name_es}'
