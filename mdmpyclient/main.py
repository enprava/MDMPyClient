import logging
import sys
import deepl
import pandas
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

        # controller.codelists.put('ESC01', 'TEST', '1.0',
        #                          {'es': 'Esto mensaje lo debe traducir'},
        #                          {'en': 'Hello world!'})

        # controller.codelists.data = controller.codelists.get(True)
        # codelist2 = controller.codelists.data['ESC01']['TEST']['1.0']
        # codelist2.add_code('pepe', None, 'aloha', 'maria')
        # codelist2.put()
        #
        # controller.concept_schemes.put('ESC01', 'HOLAPEPE', '1.0', {'es': 'pepe'}, {'es': 'ramon'})
        # controller.concept_schemes.data = controller.concept_schemes.get(True)
        # conceptscheme = controller.concept_schemes.data['ESC01']['HOLAPEPE']['1.0']
        # conceptscheme.add_concept(concept_id='petardo', parent=None, names='oha', des='patata')
        # conceptscheme.put()
        # conceptscheme.init_concepts()

        # controller.category_schemes.put('ESC01', 'POTATO', '1.0', {'en': 'wow im british'},
        #                                 {'en': 'amazing categories'})
        # controller.category_schemes.data = controller.category_schemes.get(False)
        # cs = controller.category_schemes.data['ESC01']['POTATO']['1.0']
        # cs.put(csv_file_path='csv/POTATO.csv', lang='en')
        # cs.init_data()
        # cs.categories = cs.translate(traductor, traducciones)
        # cs.put()

        # dsd = controller.dsds.data['ESC01']['DSD_APARTAMENTOS_TURISTICOS']['1.0']
        # dsd.init_data()
        dimensions = {"SEXO": {'codelist': {'agency': 'ESC01', 'id': 'CL_SEXO', 'version': '1.0'},
                               'concept_scheme': {'agency': 'ESC01', 'concepto': 'TIPO_ESTABLECIMIENTO', 'id': 'CS_ECONOMIC',
                                                  'version': '1.0'}}}
        controller.dsds.put('ESC01', 'POTATO', '1.0', {'es': 'hola'}, None, dimensions)
        # cube = controller.cubes.data['ESC01_EOC_67672']
        # cube.init_data()

        # mapping = controller.mappings.data['MAPP_175_ESC01_EOC_67667']
        # mapping.init_data()

        # dataflow = controller.dataflows.data['ESC01']['DF_CAMPINGS_67670']['1.0']
        # dataflow.init_data()
        # print(dataflow.data.to_string())
        # controller.logout()

        # msd = controller.msds.data['ESC01']['MSD_IECA']['1.0']
        # msd.init_data()
        # print(msd.data)
        # print(msd.meta)

        # metadataflow = controller.metadataflows.data['ESC01']['MDF_ESC01_ECTA']['1.0']
        # metadataflow.init_data()
        # print(metadataflow.data)
        # print(metadataflow.meta)

        # metadataset = controller.metadatasets.data['MDS_ESC01_EOAT']
        # metadataset.init_data()
        # print(metadataset.reports)
