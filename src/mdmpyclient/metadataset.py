import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Metadataset:
    """ Clase que representa un metadataset del M&D Manager.

               Args:
                   session (:class:`requests.session.Session`): Sesión autenticada en la API.
                   configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                    parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                    fichero de configuración configuracion/configuracion.yaml.
                   id (:class:`String`): Identificadordel metadataset
                   name (:class:`Diccionario`): Nombres del metadataset.
                   init_data (:class:`Boolean`): True para traer todos los datos del metadataset,
                    False para no traerlos. Por defecto toma el valor False.

               Attributes:
                   data (:obj:`Diccionario`) Diccionario con todos los datos del metadataset.
               """

    def __init__(self, session, configuracion, meta_id, names, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = meta_id
        self.names = names
        self.reports = self.get() if init_data else None

    def get(self):
        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}api/RM/getJsonMetadataset/{self.id}'
                f'/?excludeReport=false&withAttributes=false')
            response_data = response.json()['data']['metadataSets'][0]['reports']
        except KeyError:
            self.logger.error('Ha ocurrido un error solicitando información del metadataset con id %s', self.id)
            return None
        except Exception as e:
            raise e
        return response_data

    def init_data(self):
        self.reports = self.get()
