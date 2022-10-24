import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Datasets:
    def __init__(self, ckan):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.ckan = ckan
        self.datasets = ckan.call_action('package_list')

    def create(self, id, title, org_id):
        self.logger.info('Creando dataset con id %s', id)
        self.ckan.call_action('package_create', {'name': id, 'private': False, 'title': title, 'owner_org': org_id})
        self.datasets.append(id)
        self.logger.info('Dataset creado correctamente')

    def remove_all_datasets(self):
        self.logger.info('Se van a eliminar todos los datasets')
        for dataset in self.datasets:
            self.ckan.call_action('dataset_purge', {'id': dataset})
        self.datasets = []
        self.logger.info('Se han eliminado todos los datasets')