import logging
import sys

from mdmpyclient.msd.msd import MSD

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class MSDs:
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

    def __init__(self, session, configuracion, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion

        self.data, self.meta = self.get(init_data)

    def get(self, init_data):
        msd = {}

        self.logger.info('Solicitando información de los MSDs')
        try:
            response = self.session.get(f'{self.configuracion["url_base"]}msd')
            response_data = response.json()['data']['msds']
            response_meta = response.json()['meta']
        except KeyError:
            self.logger.info('No se han extraído los MSDs debido a un error de conexión con el servidor')
            return msd
        except Exception as e:
            raise e
        self.logger.info('MSDs estraídos correctamente')

        for metadata_structure in response_data:
            dsd_id = metadata_structure['id']
            agency = metadata_structure['agencyID']
            version = metadata_structure['version']
            names = metadata_structure['names']
            des = metadata_structure['descriptions'] if 'descriptions' in metadata_structure.keys() else None

            if agency not in msd:
                msd[agency] = {}
            if dsd_id not in msd[agency]:
                msd[agency][dsd_id] = {}
            msd[agency][dsd_id][version] = MSD(self.session, self.configuracion, dsd_id, agency, version, names,
                                               des, init_data)
        return msd, response_meta
