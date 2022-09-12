import logging
import sys

from mdmpyclient.mapping.mapping import Mapping

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Mappings:
    """ Clase que representa el conjunto de mappings del M&D Manager.

           Args:
               session (:class:`requests.session.Session`): Sesión autenticada en la API.
               configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                fichero de configuración configuracion/configuracion.yaml.
               init_data (:class:`Boolean`): True para traer todos los datos de los mappings,
                False para no traerlos. Por defecto toma el valor False.

           Attributes:
               data (:obj:`Dicconario`) Diccionario con todos los mappings

           """

    def __init__(self, session, configuracion, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.data = self.get(init_data)

    def get(self, init_data=True):
        data = {}
        self.logger.info('Solicitando información de los mappings')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}fileMapping')
            response_data = response.json()
        except Exception as e:
            raise e
        self.logger.info('Mappings extraídos correctamente')

        for mapping in response_data:
            mapping_id = mapping['IDMapping']
            cube_id = mapping['IDCube']
            name = mapping['Name']
            des = mapping['Description'] if 'Description' in mapping else None
            data[name] = Mapping(self.session, self.configuracion, mapping_id, cube_id, name, des, init_data=init_data)
        return data

    def put(self, columns, dimensions, cube_id, name):
        # POST http://192.168.1.123/sdmx_172/ws/NODE_API/uploadFileOnServer
        # GET http://192.168.1.123/sdmx_172/ws/NODE_API/getCSVHeader/%3B/true?filePath=\Temp\67910_20220912_112500785.csv
        # POST http://192.168.1.123/sdmx_172/ws/NODE_API/fileMapping
        components = []  # TODO
        for dim in dimensions:

            col = dim if dim in columns else self.configuracion['mappings'][dim]
            dim_type = 'Dimension'  # Tambien se podria hacer con un diccionario en config pero por ahora se queda asi
            if 'OBS_STATUS' in dim:
                dim_type = 'Attribute'
            if 'TIME_PERIOD' in dim:
                dim_type = 'TimeDimension'
            if 'OBS_VALUE' in dim:
                dim_type = 'Measure'

            components.append({'ColumnName': col, 'CubeComponentCode': dim, 'CubeComponentType': dim_type})
        json = {'Components': components,
                'CSVDelimiter': None, 'CSVSeparator': ";", 'Description': None, 'HasHeader': True,
                'HasSpecialTimePeriod': False, 'IDCube': cube_id, 'Name': name, 'Tid': None,
                'XMLFilePath': None}
        try:
            response = self.session.post(f'{self.configuracion["url_base"]}fileMapping', json=json)
            response.raise_for_status()
        except Exception as e:
            raise e
