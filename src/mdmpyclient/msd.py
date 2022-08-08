import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class MSD:
    """ Clase que representa el conjunto de MSDs del M&D Manager.

            Args:
                session (:class:`requests.session.Session`): Sesión autenticada en la API.
                configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                 parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                 fichero de configuración configuracion/configuracion.yaml.
                init_data (:class:`Boolean`): True para traer todos los MSDs, False para no traerlos.
                 Por defecto toma el valor False.

            Attributes:
                data (:obj:`Dicconario`) Diccionario con todos los MSDs
            """

    def __init__(self, session, configuracion, msd_id, agency_id, version, names, des, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = msd_id
        self.agency_id = agency_id
        self.version = version
        self.names = names
        self.des = des
        if init_data:
            self.data, self.meta = self.get()

    def get(self):
        self.logger.info('Solicitando información del MSD con id %s', self.id)
        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}msd/{self.id}/{self.agency_id}/{self.version}')
            response_data = response.json()['data']['msds'][0]['metadataStructureComponents']
            response_meta = response.json()['meta']
        except KeyError:
            self.logger.error('No se ha extraído la información del MDS con id %s debido a un error', self.id)
            return None, None
        except Exception as e:
            raise e
        return response_data, response_meta

    def init_data(self):
        self.data, self.meta = self.get()
