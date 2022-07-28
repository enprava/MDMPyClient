import pandas
import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class DSD:
    def __init__(self, session, configuracion, dsd_id, agency_id, version, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = dsd_id
        self.agency_id = agency_id
        self.version = version
        self.data = self.get_data() if init_data else None

    def get_data(self):
        try:
            response = \
                self.session.get(
                    f'{self.configuracion["url_base"]}dsd/{self.id}/{self.agency_id}/{self.version}').json()['data'][
                    'dataStructures'][0]['dataStructureComponents']
        except KeyError:
            return {}
        return response

    def __repr__(self):
        return f'{self.id} {self.version}'
