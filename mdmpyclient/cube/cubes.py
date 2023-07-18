import logging
import sys

from mdmpyclient.cube.cube import Cube

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Cubes:
    """ Clase que representa el conjunto de cubes del M&D Manager.

       Args:
           session (:class:`requests.session.Session`): Sesión autenticada en la API.
           configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
            parámetros necesarios como la url de la API. Debe ser inicializado a partir del
            fichero de configuración configuracion/configuracion.yaml.
           init_data (:class:`Boolean`): True para traer todos los cubes,
            False para no traerlos. Por defecto toma el valor False.

       Attributes:
           data (:obj:`Diccionario`): Diccionario con todos los cubes

       """

    def __init__(self, session, configuracion, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion

        self.data = self.get(init_data)

    def get(self, init_data=True):
        cubes = {}
        self.logger.info('Solicitando información de los cubos')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}cubesNoFilter')
            response_data = response.json()
        except KeyError:
            self.logger.error('No se han extraído los cubos debido a un error de conexión con el servidor %s',
                              response.text)
            return cubes
        except Exception as e:
            raise e
        self.logger.info('Cubos extraídos correctamente')

        for cube in response_data:
            cube_id = cube['IDCube']
            cube_code = cube['Code']
            cat_id = cube["IDCat"]
            dsd_code = cube['DSDCode']
            names = cube['labels']

            cubes[cube_code] = Cube(self.session, self.configuracion, cube_id, cube_code, cat_id, dsd_code, names,
                                    init_data)

        return cubes

    def put(self, cube_id, cube_cat_id, dsd_id, descripcion, dimensiones, is_alphanumeric=False,has_time_period=True):
        self.logger.info('Se ha solicitado crear el cubo de la consulta con id %s', cube_id)
        if cube_id in self.data:
            self.logger.info('El cubo ya se encuentra en la API. Con  %s', cube_id)
            return self.data[cube_id].id
        self.logger.info('Realizando petición para crear el cubo')

        json = None
        #modificar para casos particulares
        has_time_period = True
        if has_time_period:
            json = {"Code": cube_id, "labels": {"es": descripcion},
                    "IDCat": int(cube_cat_id), "DSDCode": dsd_id + "+ESC01+1.0", "Attributes": [{
                    "IsTid": False,
                    "Code": "OBS_STATUS",
                    "IsMandatory": False,
                    "AttachmentLevel": "Observation",
                    "CodelistCode": "CL_OBS_STATUS+ESTAT+2.2",
                    "refDim": [

                    ]
                }], "Dimensions": [
                    {
                        "IsTimeSeriesDim": False,
                        "Code": "INDICATOR",
                        "CodelistCode": "CL_UNIT+ESC01+1.0"
                    },
                    {
                        "IsTimeSeriesDim": False,
                        "Code": "FREQ",
                        "CodelistCode": None
                    },
                    {
                        "IsTimeSeriesDim": True,
                        "Code": "TIME_PERIOD",
                        "CodelistCode": None
                    }],
                    "Measures": [{
                        "Code": "OBS_VALUE",
                        "IsAlphanumeric": is_alphanumeric
                    }]}
        else:
            json = {"Code": cube_id, "labels": {"es": descripcion},
                    "IDCat": int(cube_cat_id), "DSDCode": dsd_id + "+ESC01+1.0", "Attributes": [{
                    "IsTid": False,
                    "Code": "OBS_STATUS",
                    "IsMandatory": False,
                    "AttachmentLevel": "Observation",
                    "CodelistCode": "CL_OBS_STATUS+ESTAT+2.2",
                    "refDim": [

                    ]
                }], "Dimensions": [
                    {
                        "IsTimeSeriesDim": False,
                        "Code": "INDICATOR",
                        "CodelistCode": "CL_UNIT+ESC01+1.0"
                    },
                    {
                        "IsTimeSeriesDim": False,
                        "Code": "FREQ",
                        "CodelistCode": None
                    }],
                    "Measures": [{
                        "Code": "OBS_VALUE",
                        "IsAlphanumeric": is_alphanumeric
                    }]}
        for dimension in dimensiones:
            codelist = dimensiones[dimension]['codelist']
            json['Dimensions'].append({"Code": dimensiones[dimension]['nombre_dimension'],
                                       "CodelistCode": codelist['id'] + '+' + codelist['agency'] + '+' + codelist[
                                           'version'], "IsTimeSeriesDim": False})

        try:
            response = self.session.post(f'{self.configuracion["url_base"]}cube', json=json)
            response.raise_for_status()
        except Exception as e:
            print(response.text)
            raise e
        cube_id = int(response.text)
        self.logger.info('Cubo creado correctamente con id %s', cube_id)
        return cube_id
