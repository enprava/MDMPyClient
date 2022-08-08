import logging
import sys

import pandas

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Dataflow:
    """ Clase que representa un dataflow del M&D Manager.

           Args:
               session (:class:`requests.session.Session`): Sesión autenticada en la API.
               configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                fichero de configuración configuracion/configuracion.yaml.
               code (:class:`String`): Identificador del dataflow.
               agency_id (:class:`String`): Identificador de la agencia asociada al dataflow.
               version (:class:`String`): Versión del dataflow.
               id (:class:`Integer`): Identificador numérico del dataflow.
               cube_id (:class:`Integer`): Identificador del cubo al que asociado al dataflow.
               filter (:class:`Diccionario`): Filtros del dataflow.
               name (:class:`Diccionario`): Nombres del dataflow.
               des (class: `Diccionario`): Descripciones del dataflow.
               init_data (:class:`Boolean`): True para traer todos los datos del dataflow,
                False para no traerlos. Por defecto toma el valor False.

           Attributes:
               data (:obj:`List`) Lista con todos los datos del dataflow.
           """
    def __init__(self, session, configuracion, code, agency_id, version, id, cube_id, filter, names, des,
                 init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.code = code
        self.agency_id = agency_id
        self.version = version
        self.id = id
        self.cube_id = cube_id
        self.names = names
        self.des = des
        self.filter = filter
        self.data = self.get() if init_data else None

    def get(self):
        self.logger.info('Solicitando estructura del dataflow con id %s', self.code)

        try:  # Dos trys para tener mas separados los errores
            response = self.session.get(f'{self.configuracion["url_base"]}ddbDataflow/{self.id}')
            response_data = response.json()['DataflowColumns']
        except KeyError:
            self.logger.error('Ha habido un error solicitando estructura del dataflow con id %s', self.code)
            return pandas.DataFrame.empty()
        except Exception as e:
            raise e
        self.logger.info('Estructura extraída correctamente')

        columns = response_data
        json = {"Filter": {"FiltersGroupAnd": {}, "FiltersGroupOr": {}},
                "SqlData": {"SelCols": columns, "SortCols": None, "SortByDesc": False, "NumPage": 1,
                            "PageSize": 2147483647}, "iDDataflow": self.id, "iDCube": self.cube_id}

        self.logger.info('Solicitando datos del dataflow con id %s', self.code)
        try:
            response = self.session.post(f'{self.configuracion["url_base"]}getDDBDataflowPreview/true', json=json)
            response_data = response.json()
        except Exception as e:
            raise e
        self.logger.info('Datos extraídos correctamente')

        columns = response_data['Columns']
        data = {}
        for column in columns:
            data[column] = []

        for info in response_data['Data']:
            for key, item in info.items():
                data[key].append(item)

        # Esta forma es mucho mas lenta
        # data = pandas.DataFrame(columns=columns)
        # for info in response_data['Data']:
        #     aux = pandas.DataFrame(info, columns=columns, index=[0])
        #     data = pandas.concat([data, aux])
        # return data
        return pandas.DataFrame(data=data, dtype='string')

    def init_data(self):
        self.data = self.get()
