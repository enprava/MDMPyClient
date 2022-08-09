import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Mapping:
    """ Clase que representa un mapping del M&D Manager.

       Args:
           session (:class:`requests.session.Session`): Sesión autenticada en la API.
           configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
            parámetros necesarios como la url de la API. Debe ser inicializado a partir del
            fichero de configuración configuracion/configuracion.yaml.
           id (:class:`Integer`): Identificador del mapping.
           cube_id (:class:`Integer`): Identificador del cubo al que pertenece el mapping.
           name (:class:`String`): Nombre del mapping.
           des (class: `String`): Descripción del mapping.
           init_data (:class:`Boolean`): True para traer todos los datos del mapping,
            False para no traerlos. Por defecto toma el valor False.

       Attributes:
           components (:obj:`List`) Lista con todos los componentes del mapping.
       """

    def __init__(self, session, configuracion, mapping_id, cube_id, name, des, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = mapping_id
        self.cube_id = cube_id
        self.code = name
        self.des = des
        self.components = self.get if init_data else None

    def get(self):
        components = []
        self.logger.info('Solicitando información del mapping con id %s', self.id)

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}fileMapping/{self.id}')
            response_data = response.json()['Components']
        except KeyError:
            self.logger.error('Ha ocurrido un error mientras se extraían datos del mapping con id %s', self.id)
            return components
        except Exception as e:
            raise e
        self.logger.info('Datos extraídos correctamente')

        for component in response_data:
            comp_id = component['IDComp']
            column = component['ColumnName']
            column_mapped = component['CubeComponentCode']
            comp_type = ['CubeComponentType']

            components.append({'id_comp': comp_id, 'column': column, 'column_mapped': column_mapped, 'type': comp_type})
        return components

    def init_data(self):
        self.components = self.get()
