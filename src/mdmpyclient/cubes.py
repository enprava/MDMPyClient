import logging
import sys

from src.mdmpyclient.cube import Cube

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Cubes:
    def __init__(self, session, configuracion, init_data):
        self.session = session
        self.configuracion = configuracion

        self.data = self.get(init_data)

    def get(self, init_data):
        cubes = {}
        try:
            response = self.session.get(f'{self.configuracion["url_base"]}cubesNoFilter')
            response_data = response.json()
        except KeyError:
            self.logger.error('No se han extraído los cubos debido a un error de conexión con el servidor %s',
                              response.text)
            return cubes
        except Exception as e:
            raise e

        for cube in response_data:
            cube_id = cube['IDCube']
            cube_code = cube['Code']
            cat_id = cube["IDCat"]
            dsd_code = cube['DSDCode']
            names = cube['labels']

            cubes[cube_id] = Cube(self.session, self.configuracion, cube_id, cube_code, cat_id, dsd_code, names,
                                  init_data)

        return cubes

    def put(self, code, cat_id, names, dsd):
        pass
