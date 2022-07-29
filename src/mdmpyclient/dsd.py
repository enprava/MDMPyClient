import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class DSD:
    """ Clase que representa un DSD del M&D Manager.

        Args:
            session (:class:`requests.session.Session`): Sesión autenticada en la API.
            configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
             parámetros necesarios como la url de la API. Debe ser inicializado a partir del
             fichero de configuración configuracion/configuracion.yaml.
            dsd_id (class: 'String'): Identificador del DSD.
            agency_id (class: `String`): Identificador de la agencia vinculada.
            version (class: `String`): Version del DSD.
            names (class: `Diccionario`): Diccionario con los nombres del DSD en varios idiomas.
            des (class: `String`): Diccionario con las descripciones del DSD en varios idiomas.
            init_data (class: `Boolean`): True para traer todos los datos del DSD,
             False para traer solo id, agencia y versión. Por defecto toma el valor False.

        Attributes:


        """
    def __init__(self, session, configuracion, dsd_id, agency_id, version, names, des, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = dsd_id
        self.agency_id = agency_id
        self.version = version
        self.names = names
        self.des = des
        self.data = self.get_data() if init_data else None

    def get_data(self):
        try:
            response = \
                self.session.get(
                    f'{self.configuracion["url_base"]}dsd/{self.id}/{self.agency_id}/{self.version}')
            response_data = response.json()['data'][
                'dataStructures'][0]['dataStructureComponents']
        except KeyError:
            self.logger.warning('Ha ocurrido un error mientras se cargaban los datos del DSD con id: %s',
                                self.id)
            self.logger.error(response.text)
            return {}
        except Exception as e:
            raise e
        return response_data

    def __repr__(self):
        return f'{self.id} {self.version}'
