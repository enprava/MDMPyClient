import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Metadataflow:
    """ Clase que representa un metadataflow del M&D Manager.

           Args:
               session (:class:`requests.session.Session`): Sesión autenticada en la API.
               configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                fichero de configuración configuracion/configuracion.yaml.
               meta_id (:class:`String`): Identificadordel metadataflow.
               agency_id (:class:`String`): Identificador de la agencia asociada al
                metadataflow.
               version (:class:`String`): Versión del metadataflow.
               name (:class:`Diccionario`): Nombres del metadataflow.
               des (class: `Diccionario`): Descripciones del metadataflow.
               init_data (:class:`Boolean`): True para traer todos los datos del metadataflow,
                False para no traerlos. Por defecto toma el valor False.

           Attributes:
               data (:obj:`List`): Lista con todos los datos del metadataflow.
           """

    def __init__(self, session, configuracion, meta_id, agency_id, version, names, des, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.agency_id = agency_id
        self.version = version
        self.id = meta_id
        self.names = names
        self.des = des
        self.data = self.get() if init_data else None

    def get(self):
        self.logger.info('Solicitando información del metadataflow con id %s', self.id)
        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}metadataflow/{self.id}/{self.agency_id}/{self.version}')
            response_data = response.json()['data']
        except KeyError:
            self.logger.error('Ha ocurrido un error solicitando información  del metadataflow con id %s', self.id)
            return None, None
        except Exception as e:
            raise e
        self.logger.info('Metadataflows extraídos correctamente')
        return response_data

    def init_data(self):
        self.data = self.get()

    def delete(self):
        self.logger.info('Borrando el metadataflow con id %s', self.id)
        try:
            response = self.session.delete(
                f'{self.configuracion["url_base"]}artefact/Metadataflow/{self.id}/{self.agency_id}/{self.version}')
            response.raise_for_status()
        except Exception as e:
            raise e
