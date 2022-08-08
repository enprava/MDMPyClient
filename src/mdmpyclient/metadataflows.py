import logging
import sys

from src.mdmpyclient.metadataflow import Metadataflow

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Metadataflows:
    """ Clase que representa el conjunto de metadataflows del M&D Manager.

                Args:
                    session (:class:`requests.session.Session`): Sesión autenticada en la API.
                    configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                     parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                     fichero de configuración configuracion/configuracion.yaml.
                    init_data (:class:`Boolean`): True para traer todos los datos metadataflows, False para no
                     traerlos. Por defecto toma el valor False.

                Attributes:
                    data (:obj:`Dicconario`) Diccionario con todos los metadataflows

                """

    def __init__(self, session, configuracion, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion

        self.data, self.meta = self.get(init_data)

    def get(self, init_data=True):
        data = {}
        self.logger.info('Solicitando información de los metadataflows')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}metadataflow')
            response_data = response.json()['data']['metadataflows']
            response_meta = response.json()['meta']
        except Exception as e:
            raise e
        self.logger.info('Metadataflows extraídos correctamente')

        for metadata in response_data:
            id = metadata['id']
            agency = metadata['agencyID']
            version = metadata['version']
            names = metadata['names']
            des = metadata['descriptions']

            if agency not in data:
                data[agency] = {}
            if id not in data[agency]:
                data[agency][id] = {}
            data[agency][id][version] = Metadataflow(self.session, self.configuracion, id, agency, version, names, des,
                                                     init_data)
        return data, response_meta
