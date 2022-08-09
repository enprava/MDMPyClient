import logging
import sys

from src.mdmpyclient.metadataset import Metadataset

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Metadatasets:
    """ Clase que representa el conjunto de metadatasets del M&D Manager.

                   Args:
                       session (:class:`requests.session.Session`): Sesión autenticada en la API.
                       configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                        parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                        fichero de configuración configuracion/configuracion.yaml.
                       init_data (:class:`Boolean`): True para traer todos los datos metadatasets, False para no
                        traerlos. Por defecto toma el valor False.

                   Attributes:
                       data (:obj:`Dicconario`) Diccionario con todos los metadatasets"""

    def __init__(self, session, configuracion, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion

        self.data = self.get(init_data)

    def get(self, init_data=True):
        data = {}
        self.logger.info('Solicitando información de los metadatasets')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}api/RM/getJsonMetadatasetList')
            response_data = response.json()['data']['metadataSets']
        except Exception as e:
            raise e
        self.logger.info('Metadatasets extraídos correctamente')

        for metadata in response_data:
            meta_id = metadata['id']
            names = metadata['names']

            data[meta_id] = Metadataset(self.session, self.configuracion, meta_id, names, init_data)
        return data
