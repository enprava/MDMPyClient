import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class DSD:
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
                    f'{self.configuracion["url_base"]}dsd/{self.id}/{self.agency_id}/{self.version}').json()['data'][
                    'dataStructures'][0]['dataStructureComponents']
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
