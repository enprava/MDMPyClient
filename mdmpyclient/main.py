import logging
import sys
import deepl
import pandas as pd
import yaml
from ckanapi import RemoteCKAN

from mdmpyclient.ckan.organizations import Organizations
from mdmpyclient.ckan.resource import Resource
from mdmpyclient.ckan.dataset import Dataset
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
                          apikey='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NjYxNzY4MTAsImp0aSI6InhfdDBFQmVOX2VvSEtMV2FQNGJyV29SdDlNcXN1QVp0VTlMWGNYZkxJZWNuSDRPS0VGSnNNUU5YUVNraE11Y09EYnpkeE9lbEZBMkx6SDF0In0.SrhY8b4w_XeeiEmLzgf1oDIVNAu2XWnRtX1gubYzAmA')
        res = Resource(ckan)
        orgs = Organizations(ckan)
        datasets = Dataset(ckan)
        # datasets.create('potato', orgs.orgs[1]['id'])
        datillos = controller.dataflows.data['ESC01']['DF_INDICADORES_16861']['1.0'].data
        res.create(datillos, 'DF_APARTAMENTOS_TURISTICOS_67915', 'csv',datasets.datasets[0])
        # orgs.remove_all_orgs()
        # orgs.create_orgs(controller.category_schemes.data['ESC01']['IECA_CAT_EN_ES']['1.0'].categories)
        controller.logout()
