import logging
import sys
# import os
# from pprint import pprint

import deepl
import pandas as pd
import yaml
#from ckanapi import RemoteCKAN
# import requests

# from mdmpyclient.ckan.ckan import Ckan
# from mdmpyclient.ckan.organizations import Organizations
# from mdmpyclient.ckan.resource import Resource
# from mdmpyclient.ckan.dataset import Datasets

from mdmpyclient.mdm import MDM
# from bs4 import BeautifulSoup

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger('MDM')

if __name__ == '__main__':
    traductor = deepl.Translator('92766a66-fa2a-b1c6-d7dd-ec0750322229:fx')

    download_data = False
    delete_data = False
    upload_data = True

    with open("configuracion/configuracion.yaml", 'r', encoding='utf-8') as configuracion, \
            open("configuracion/traducciones.yaml", 'r', encoding='utf-8') as traducciones:
        configuracion = yaml.safe_load(configuracion)
        traducciones = yaml.safe_load(traducciones)

        controller = MDM(configuracion, traductor, False)
        #ckan = Ckan(configuracion)
        #print(ckan.groups.test())

        if delete_data:
            controller.delete_all('ESC01', 'IECA_CAT_EN_ES', '1.0')
        if download_data:
            for dsd in controller.dsds.data:
                controller.dsds.get_all_sdmx("mdm_data/dsds/")
            controller.dataflows.get_all_sdmx("mdm_data/dataflows/")
            controller.codelists.get_all_sdmx("mdm_data/codelists/")
            controller.concept_schemes.get_all_sdmx("mdm_data/conceptschemes/")
            controller.category_schemes.get_all_sdmx("mdm_data/categoryschemes/")


        if upload_data:
            controller.put("mdm_data")
        controller.logout()
