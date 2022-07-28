import logging
import sys

import yaml

from src.mdmpyclient.mdm import MDM

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger('MDM')

if __name__ == '__main__':
    configuracion = open('configuracion/configuracion.yaml', 'r', encoding='utf-8')
    configuracion = yaml.safe_load(configuracion)

    try:
        controller = MDM(configuracion)
        # Esto debería ir en pruebas tal vez.
        # controller.codelists.save('ESC01', 'Prueba', '1.0',
        #                             {language+'1': language for language in configuracion['languages']},
        #                             {language: language for language in configuracion['languages']})
        controller.logout()
    except Exception as e:
        logger.error(e)
