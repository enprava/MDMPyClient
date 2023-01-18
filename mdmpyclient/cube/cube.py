import logging
import sys

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




           """

    def __init__(self, session, configuracion, cube_id, code, cat_id, dsd_code, names, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = cube_id
        self.cube_code = code
        self.cat_id = cat_id
        self.dsd_code = dsd_code
        self.names = names
        self.components = self.get() if init_data else None

    def get(self):
        components = {'Measures': [], 'Attributes': [], 'Dimensions': []}

        self.logger.info('Solicitando información del cubo con id %s', self.id)
        try:
            response = self.session.get(f'{self.configuracion["url_base"]}cube/{self.id}')
            response_data = response.json()
        except Exception as e:
            raise e

        for key, item in response_data.items():  # Queda mas reducido pero no se si es lo recomendable
            if isinstance(item, list):
                for component in item:
                    if 'Dimensions' in key:
                        cube_id = component['IDDim']
                    elif 'Measures' in key:
                        cube_id = component['IDMeas']
                    else:
                        cube_id = component['IDAtt']
                    code = component['Code']
                    column_name = component['ColName']
                    codelist_code = component['CodelistCode'] if 'CodelistCode' in component else None
                    codes = component['codes'] if 'codes' in component else None
                    components[key].append(
                        {'id': cube_id, 'code': code, 'column_name': column_name, 'codelist': codelist_code,
                         'codes': codes})
        return components

    # def get(self):
    #     data = {}
    #    request_data = {'FilterTable': [], 'PageNum': 1, 'PageSize': 2147483647, 'SortByDesc': False, 'SortCols': None}
    #     self.logger.info('Solicitando información del cubo con id %s', self.cube_id)
    #     try:
    #         response = self.session.post(
    #             f'{self.configuracion["url_base"]}getTablePreview/Dataset_{self.cube_id}_ViewCurrentData',
    #             json=request_data)
    #         response_data = response.json()
    #     except KeyError:
    #         self.logger.error('Ha ocurrido un error mientras se cargaban los datos del cubo con id: %s', self.cube_id)
    #         self.logger.error(response.text)
    #         return data
    #     except Exception as e:
    #         raise e
    #
    #     for column in response_data['Columns']:
    #         data[column] = []
    #
    #     for row in response_data['Data']:
    #         for measure, item in row.items():
    #             data[measure].append(item)
    #     return pandas.DataFrame(data=data, dtype='string')

    def init_data(self):
        self.components = self.get()

    def __repr__(self):
        return f'ID: {self.id} name: {self.names}'
