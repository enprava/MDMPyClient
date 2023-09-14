import logging
import sys
import requests
import sys
from contextlib import redirect_stdout
import os

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Mapping:
    """ Clase que representa un mapping del M&D Manager.

       Args:
           session (:class:`requests.session.Session`): Sesión autenticada en la API.
           configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
            parámetros necesarios como la url de la API. Debe ser inicializado a partir del
            fichero de configuración configuracion/configuracion.yaml.
           id (:class:`Integer`): Identificador del mapping.
           cube_id (:class:`Integer`): Identificador del cubo al que pertenece el mapping.
           name (:class:`String`): Nombre del mapping.
           des (class: `String`): Descripción del mapping.
           init_data (:class:`Boolean`): True para traer todos los datos del mapping,
            False para no traerlos. Por defecto toma el valor False.

       Attributes:
           components (:obj:`List`): Lista con todos los componentes del mapping.
       """

    def __init__(self, session, configuracion, mapping_id, cube_id, name, des, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = mapping_id
        self.cube_id = cube_id
        self.code = name
        self.des = des
        self.components = self.get if init_data else None

    def get(self):
        components = []
        self.logger.info('Solicitando información del mapping con id %s', self.id)

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}fileMapping/{self.id}')
            response_data = response.json()['Components']
        except KeyError:
            self.logger.error('Ha ocurrido un error mientras se extraían datos del mapping con id %s', self.id)
            return components
        except Exception as e:
            raise e
        self.logger.info('Datos extraídos correctamente')

        for component in response_data:
            comp_id = component['IDComp']
            column = component['ColumnName']
            column_mapped = component['CubeComponentCode']
            comp_type = ['CubeComponentType']

            components.append({'id_comp': comp_id, 'column': column, 'column_mapped': column_mapped, 'type': comp_type})
        return components

    def load_cube(self, data,dsd_name = "None", df_name = "None"):
        # if self.cube_is_loaded():
        self.logger.info('Cargando datos en el cubo con id %s', self.cube_id)

        csv = data.to_csv(sep=';', index=False).encode(encoding='utf-8')
        files = {'file': (
            'hehe.csv', csv, 'application/vnd.ms-excel', {})}
        upload_headers = self.session.headers.copy()
        body, content_type = requests.models.RequestEncodingMixin._encode_files(files, {})
        upload_headers['Content-Type'] = content_type
        upload_headers['language'] = 'es'
        # print(f'{self.configuracion["url_base"]}uploadFileOnServer/{self.cube_id}')
        try:
            response = self.session.post(f'{self.configuracion["url_base"]}uploadFileOnServer/{self.cube_id}',
                                         data=body, headers=upload_headers)
            response.raise_for_status()

        except Exception as e:
            raise e

        path = response.text.replace('\\\\','%5C').replace('"','')

        self.logger.info('Archivo con datos subido a la API. Volcando los datos en el cubo')

        try:
            # print( f'{self.configuracion["url_base"]}importCSVData/%3B/true/SeriesAndData/{self.cube_id}" + \
            # f"/{self.id}?filePath='+path+'&checkFiltAttributes=true')
            response = self.session.get(
                f'{self.configuracion["url_base"]}importCSVData/%3B/true/SeriesAndData/'
                f'{self.cube_id}/{self.id}?filePath='+path+'&checkFiltAttributes=true')
            archivo_validation_report = open(os.path.join("entrega", "validation_report", df_name+".txt"),'w')
            # os.makedirs(archivo_validation_report, exist_ok=True)
            response_info = response.json()

            if response_info['WarnDictionary']:
                self.logger.error('Error al cargar los datos en el cubo con id %s', self.cube_id)
                self.logger.error('%s', response_info)
            else:
                self.logger.info('Datos volcados con exito')
            with redirect_stdout(archivo_validation_report):
                print("        ** ** ** ** ** ** ** ** ** * ")
                print("       *                          * ")
                print("      *    Validation Report     *")
                print("     *                          *")
                print("    ** ** ** ** ** ** ** ** ** *")
                print("===========================================================================")
                print("El cubo vinculado con el dataflow", df_name ," relacionado con el DSD",dsd_name,
                      "se ha logrado importar con una cantidad de", len(response_info["WrongLines"]), "lineas erroneas")
                print("-")
                print("Se puede observar la repuesta del servidor:")
                response_errors = {'WarnDictionary':response_info['WarnDictionary'],'WrongLines':response_info['WrongLines']}
                print(response_errors)
                print("===========================================================================")
            archivo_validation_report.close()
            response.raise_for_status()
        except Exception as e:
            raise e

        # else:
        #     self.logger.info('El cubo con id %s ya se encontraba cargado', self.cube_id)

    def cube_is_loaded(self):
        json = {"PageNum": 1, "PageSize": 1, "FilterTable": [], "SortCols": None, "SortByDesc": None}
        try:
            response = self.session.get(f'{self.configuracion["url_base"]}Dataset_{self.cube_id}_ViewCurrentData',
                                        json=json)
            response.raise_for_status()
            response = response.json()['data']
        except Exception as e:
            raise e
        return response

    def init_data(self):
        self.components = self.get()
