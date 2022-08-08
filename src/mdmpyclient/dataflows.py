import logging
import sys

from src.mdmpyclient.dataflow import Dataflow

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Dataflows:
    """ Clase que representa el conjunto de dataflows del M&D Manager.

               Args:
                   session (:class:`requests.session.Session`): Sesión autenticada en la API.
                   configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                    parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                    fichero de configuración configuracion/configuracion.yaml.
                   init_data (:class:`Boolean`): True para traer todos los datos dataflows, False para no
                    traerlos. Por defecto toma el valor False.

               Attributes:
                   data (:obj:`Dicconario`) Diccionario con todos los dataflows

               """
    def __init__(self, session, configuracion, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.data = self.get(init_data)

    def get(self, init_data=True):
        data = {}
        self.logger.info('Solicitando información de los dataflows')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}ddbDataflow')
            response_data = response.json()
        except Exception as e:
            raise e
        self.logger.info('Dataflows extraídos correctamente')

        for dataflow in response_data:
            code = dataflow['ID']
            id = dataflow['IDDataflow']
            cube_id = dataflow['IDCube']
            agency_id = dataflow['Agency']
            version = dataflow['Version']
            filter = dataflow['Filter']
            names = dataflow['labels']
            des = dataflow['Descriptions'] if 'Descriptions' in dataflow else None

            if agency_id not in data:
                data[agency_id] = {}
            if code not in data[agency_id]:
                data[agency_id][code] = {}
            data[agency_id][code][version] = Dataflow(self.session, self.configuracion, code, agency_id, version, id,
                                                      cube_id, filter, names, des, init_data)

        return data
