import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Metadataset:
    def __init__(self, session, configuracion, id, names, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = id
        self.names = names
        self.reports = self.get() if init_data else None

    def get(self):
        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}api/RM/getJsonMetadataset/{self.id}/?excludeReport=false&withAttributes=false')
            response_data = response.json()['data']['metadataSets'][0]['reports']
        except KeyError:
            self.logger.error('Ha ocurrido un error solicitando información del metadataset con id %s', self.id)
            return None
        except Exception as e:
            raise e
        return response_data

    def init_data(self):
        self.reports = self.get()
