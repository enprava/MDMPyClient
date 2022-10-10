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

        controller = MDM(configuracion, traductor, True)
        controller.metadataflows.put('ESC01', 'FACILa2', '1.0', {'es': 'ola'}, None)
        controller.metadatasets.put('ESC01','FASILITdfsaO', {'es':'andaslusia'},'FACILa2','1.0','IECA_CAT_EN_ES','DEMO_SOCIAL_STAT.LABOUR.EPA','1.0')
        controller.logout()
