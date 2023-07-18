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
               data (:obj:`Dicconario`): Diccionario con todos los mappings

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
            data[cube_id] = Mapping(self.session, self.configuracion, mapping_id, cube_id, name, des,
                                    init_data=init_data)
        return data

    def put(self, columns, cube_id, name):
        for mapping in self.data.values():
            if cube_id == mapping.cube_id:
                return mapping.id
        self.logger.info('Se va a realizar un mapping del cubo con id %s', cube_id)
        # if cube_id in self.data:
        #     self.logger.info('El Mapping ya se encuentra en la API. Con  %s', cube_id)
        #     return self.data[cube_id].id
        components = []
        for col, dim in columns.items():
            dim = 'TIME_PERIOD' if 'TEMPORAL' in col else dim
            dim_type = 'Dimension'
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
        mapping_id = int(response.text)
        self.logger.info('Mapping creado correctamente con id %s', mapping_id)

        self.data[cube_id] = Mapping(self.session, self.configuracion, mapping_id, cube_id, name, name, False)

        return mapping_id
