import io


class Resource:
    def __init__(self, ckan):
        self.ckan = ckan

    def create(self, data, name, format, dataset_id):
        to_upload = io.BytesIO(bytes(data.to_csv(index=False), 'utf-8'))
        to_upload.name = f'{name}.{format}'
        self.ckan.action.resource_create(
            package_id=dataset_id,
            format=format,
            name=name,
            upload=to_upload)