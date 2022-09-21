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

        # controller.concept_schemes.add_concept_scheme('ESC01', 'ASDADF', '1.0', {'es': 'pepe'}, None)
        # controller.concept_schemes.put_all_concept_schemes()
        controller.concept_schemes.put('ESC01', 'ASDADFAS', '1.0', {'es': 'pepe'}, None)
        controller.logout()
