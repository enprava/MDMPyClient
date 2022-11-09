import logging
import sys
from pprint import pprint

import deepl
import pandas as pd
import yaml
from ckanapi import RemoteCKAN
import requests
from mdmpyclient.ckan.ckan import Ckan
from mdmpyclient.mdm import MDM
from bs4 import BeautifulSoup

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
        # ckan = Ckan(configuracion)
        # ckan.orgs.create_orgs(controller.category_schemes.data['ESC01']['IECA_CAT_EN_ES']['1.0'].categories)

        # datasets.create(datillos.code.lower(), datillos.names['es'], orgs.orgs[0]['id'])
        # res.create(datillos.data, 'DF_APARTAMENTOS_TURISTICOS_67915', 'csv', datasets.datasets[0])
        # orgs.remove_all_orgs()
        # ckan.orgs.create_orgs(controller.category_schemes.data['ESC01']['IECA_CAT_EN_ES']['1.0'].categories)

        # controller.metadatasets.data['MDF_INDICADORES_16861'].download_report('REPORT_ODS_IDA')

        # controller.dsds.get_all_sdmx('sdmx/dsds')
        controller.dataflows.get_all_sdmx('sdmx/dataflows')
        controller.logout()
