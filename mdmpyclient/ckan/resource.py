import io


class Resource:
    def __init__(self, ckan):
        self.ckan = ckan

    def create(self, data):
        to_upload = io.BytesIO(bytes(data.to_csv(index=False), 'utf-8'))
        to_upload.name = 'dataflow.csvjaja'
        self.ckan.action.resource_create(
            package_id='my_dataset_name',
            format='csv',
            name='dataflow.csv',
            upload=to_upload)
        # file = open('CL_OBS_STATUS+ESTAT+2.2.xml', 'rb')
        # self.ckan.action.resource_create(
        #     package_id='my_dataset_name',
        #     name='Recurso',
        #     upload=file)
        # print(file)
        # print('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        # print(data)
