from io import BytesIO
import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Resource:
    def __init__(self, ckan):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.ckan = ckan

    def create(self, data, name, res_format, dataset_id):
        self.logger.info('Creando recurso con nombre %s para el dataset con id %s', name, dataset_id)
        to_upload = BytesIO(bytes(data.to_csv(index=False), 'utf-8'))
        to_upload.name = f'{name}.{res_format}'
        self.ckan.action.resource_create(
            package_id=dataset_id,
            format=res_format,
            name=name,
            upload=to_upload)
        self.logger.info('Recurso creado satisfactoriamente')

    def create_from_file(self, path, name, res_format, dataset_id):
        self.logger.info('Creando recurso con nombre %s para el dataset con id %s', name, dataset_id)
        try: #TEMPORAL, QUITAR PARA SUBIR A PRODUCCION
            with open(path, 'r', encoding='utf=8') as o_upload:
                to_upload = o_upload
                if res_format in 'html':
                    print(to_upload)
                self.ckan.action.resource_create(
                    package_id=dataset_id,
                    format=res_format,
                    name=name,
                    upload=to_upload)
            self.logger.info('Recurso creado satisfactoriamente')

        except Exception as e:
            pass
