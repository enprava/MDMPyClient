import logging
import sys
from pprint import pprint

import deepl
import pandas as pd
import yaml
from ckanapi import RemoteCKAN
import requests
from mdmpyclient.ckan.organizations import Organizations
from mdmpyclient.ckan.resource import Resource
from mdmpyclient.ckan.dataset import Datasets
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

        controller = MDM(configuracion, traductor, True)
        ckan = RemoteCKAN('http://localhost:5000/',
                          apikey=configuracion['api_ckan'])
        # res = Resource(ckan)
        # orgs = Organizations(ckan)
        # orgs.remove_all_orgs()
        # datasets = Datasets(ckan)
        # datasets.remove_all_datasets()
        # datillos = controller.dataflows.data['ESC01']['DF_INDICADORES_16861']['1.0']

        # datasets.create(datillos.code.lower(), datillos.names['es'], orgs.orgs[0]['id'])
        # res.create(datillos.data, 'DF_APARTAMENTOS_TURISTICOS_67915', 'csv', datasets.datasets[0])
        # orgs.remove_all_orgs()
        # orgs.create_orgs(controller.category_schemes.data['ESC01']['IECA_CAT_EN_ES']['1.0'].categories)

        controller.metadatasets.data['MDF_INDICADORES_16861'].download_report('REPORT_ODS_IDA')

        controller.logout()
