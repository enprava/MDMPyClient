from io import BytesIO
import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Resource:
    def __init__(self, ckan):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.ckan = ckan

    def create(self, data, name, format, dataset_id):
        self.logger.info('Creando recurso de nombre %s para el data set con id %s', name, dataset_id)
        to_upload = BytesIO(bytes(data.to_csv(index=False), 'utf-8'))
        to_upload.name = f'{name}.{format}'
        self.ckan.action.resource_create(
            package_id=dataset_id,
            format=format,
            name=name,
            upload=to_upload)
