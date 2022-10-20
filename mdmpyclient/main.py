import logging
import sys
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

        # controller = MDM(configuracion, traductor, True)
        #
        ckan = RemoteCKAN('http://localhost:5000/',
                          apikey='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NjYyNTQ2NjQsImp0aSI6Ikt1YkthdVlBVVItQjhDOTI1ODVobThCd28zZnhQcDVJQkVtNkhORkR5SkRldUVtamloTl9ROXUwaG5GRFlOS3ZWb0dNY2ZBRzRUU1FNUXNJIn0.8GgwvTYxOrB1v4GdJ_JbsgYml1BEoNwwizuqW-mlHeg')
        # res = Resource(ckan)
        orgs = Organizations(ckan)
        print(len(orgs.orgs))
        print((orgs.orgs))
        # datasets = Datasets(ckan)
        # datillos = controller.dataflows.data['ESC01']['DF_INDICADORES_16861']['1.0']

        # datasets.create(datillos.code.lower(), datillos.names['es'], orgs.orgs[0]['id'])
        # res.create(datillos.data, 'DF_APARTAMENTOS_TURISTICOS_67915', 'csv', datasets.datasets[0])
        # orgs.remove_all_orgs()
        # orgs.create_orgs(controller.category_schemes.data['ESC01']['IECA_CAT_EN_ES']['1.0'].categories)


        # controller.logout()
