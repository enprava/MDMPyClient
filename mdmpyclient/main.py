import logging
import sys
import deepl
import pandas as pd
import yaml

from mdmpyclient.mdm import MDM

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger('MDM')

if __name__ == '__main__':
    traductor = deepl.Translator('92766a66-fa2a-b1c6-d7dd-ec0750322229:fx')

    with open("configuracion/configuracion.yaml", 'r', encoding='utf-8') as configuracion, \
            open("configuracion/traducciones.yaml", 'r', encoding='utf-8') as traducciones:
        configuracion = yaml.safe_load(configuracion)
        traducciones = yaml.safe_load(traducciones)

        controller = MDM(configuracion, traductor)

        cl = controller.codelists.data['ESC01']['CL_CNAE09']['1.0']
        csv = pd.read_csv('csv/CL_CNAE09.csv', sep=';', dtype='string')
        print(len(csv))
        cl.add_codes(csv)
        print(len(cl.codes_to_upload))
        cl.put()
        controller.logout()
