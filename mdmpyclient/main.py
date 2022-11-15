import logging
import sys
from pprint import pprint

import deepl
import pandas as pd
import yaml
from ckanapi import RemoteCKAN
import requests

from mdmpyclient.ckan.ckan import Ckan
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

        # controller = MDM(configuracion, traductor, True)
        ckan = Ckan(configuracion)
        print(ckan.groups.test())
        # controller.logout()
