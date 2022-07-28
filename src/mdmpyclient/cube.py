import pandas
import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)

class Cube:
    def __init__(self, session, configuracion, cube_id, cube_code, cat_id, dsd_code, name_es, name_en, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.cube_id = cube_id
        self.cube_code = cube_code
        self.cat_id = cat_id
        self.dsd_code = dsd_code
        self.name_es = name_es
        self.name_en = name_en
        self.data = self.get_data() if init_data else None

    def get_data(self):
        data = {}
        request_data = {'FilterTable': [], 'PageNum': 1, 'PageSize': 2147483647, 'SortByDesc': False, 'SortCols': None}
        try:
            response = self.session.post(
                f'{self.configuracion["url_base"]}getTablePreview/Dataset_{self.cube_id}_ViewCurrentData',
                json=request_data).json()
        except KeyError:
            return data

        for column in response['Columns']:
            data[column] = []

        for row in response['Data']:
            for measure in row.keys():
                data[measure].append(row[measure])
        print(pandas.DataFrame(data=data,dtype='string'))
        return pandas.DataFrame(data=data,dtype='string')

    def __repr__(self):
        return f'ID: {self.cube_id} name: {self.name_es}'
