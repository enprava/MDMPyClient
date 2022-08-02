import logging
import sys

import deepl as deepl
import yaml

from src.mdmpyclient.mdm import MDM

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger('MDM')

if __name__ == '__main__':
    traductor = deepl.Translator('92766a66-fa2a-b1c6-d7dd-ec0750322229:fx')

    with open("configuracion/configuracion.yaml", 'r', encoding='utf-8') as configuracion, \
            open("configuracion/traducciones.yaml", 'r', encoding='utf-8') as traducciones:
        configuracion = yaml.safe_load(configuracion)
        traducciones = yaml.safe_load(traducciones)

        controller = MDM(configuracion)
        # controller.codelists.put('ESC01', 'TEST', '1.0',
        #                          {language: 'dsfsd' if language == 'es' else 'ingles' for language in
        #                           configuracion['languages']},
        #                          {language: 'dsfsd' if language == 'es' else 'ingles' for language in
        #                           configuracion['languages']})
        #
        # controller.codelists.data = controller.codelists.get()
        # codelist2 = controller.codelists.data['ESC01']['CL_UNIT']['1.0']
        # codelist2.init_codes()
        # codelist2.codes = codelist2.translate(traductor, traducciones)

        controller.concept_schemes.data = controller.concept_schemes.get(True)
        conceptscheme = controller.concept_schemes.data['ESC01']['ASDF']['1.0']
        # controller.concept_schemes.put('ESC01','POTATO','1.0', {'es':'pepe'},{'es':'ramon'})
        conceptscheme.put(csv_file_path='tests/test_cs.csv')
        conceptscheme.init_concepts()
        conceptscheme.translate(traductor, traducciones)
        conceptscheme.put()
        #
        # # controller.category_schemes.data = controller.category_schemes.get()
        # # cs = controller.category_schemes.data['ESC01']['POTATO']['1.0']
        # # cs.init_data()
        # # cs.put('tests/test_cat.csv')

        controller.logout()
