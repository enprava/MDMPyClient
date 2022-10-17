import logging
import sys
import deepl
import pandas as pd
import yaml
from ckanapi import RemoteCKAN

from mdmpyclient.ckan.resource import Resource
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

        ckan = RemoteCKAN('http://localhost:5000/',
                          apikey='97c4de73-ce11-40f9-8da2-0bf7415d0a15')
        res = Resource(ckan)
        datillos = controller.dataflows.data['ESC01']['DF_APARTAMENTOS_TURISTICOS_67915']['1.0'].data
        res.create(datillos)

        controller.logout()
