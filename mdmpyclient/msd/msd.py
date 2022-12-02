import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class MSD:
    """ Clase que representa un MSD del M&D Manager.

             Args:
                 session (:class:`requests.session.Session`): Sesión autenticada en la API.
                 configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                  parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                  fichero de configuración configuracion/configuracion.yaml.
                 msd_id (:class:`String`): Identificador del MSD.
                 agency_id (:class:`String`): Identificador de la agencia asociada al MSD.
                 version (:class:`String`): Versión del MSD.
                 name (:class:`Diccionario`): Nombres del MSD.
                 des (class: `Diccionario`): Descripciones del MSD.
                 init_data (:class:`Boolean`): True para traer todos los datos del MSD,
                  False para no traerlos. Por defecto toma el valor False.

             Attributes:
                 data (:obj:`Diccionario`): Diccionario con todos los datos del MSD.
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
        self.data = self.get() if init_data else None

    def get(self):
        self.logger.info('Solicitando información del MSD con id %s', self.id)
        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}msd/{self.id}/{self.agency_id}/{self.version}')
            response_data = response.json()['data']['msds'][0]['metadataStructureComponents']
        except KeyError:
            self.logger.error('No se ha extraído la información del MDS con id %s debido a un error', self.id)
            return None
        except Exception as e:
            raise e
        return response_data

    def init_data(self):
        self.data = self.get()
