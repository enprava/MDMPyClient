import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)
class Tags:
    def __init__(self, ckan):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.ckan = ckan
        self.logger.info('Obteniendo todos los tags')
        self.tags = ckan.call_action('tag_list', {'all_fields': True})
        self.logger.info('Tags obtenidos con exito')
