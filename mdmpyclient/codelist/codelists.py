import logging
import sys

from mdmpyclient.codelist.codelist import Codelist

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Codelists:
    """ Clase que representa el conjunto de codelists del M&D Manager.

    Args:
        session (:class:`requests.session.Session`): Sesión autenticada en la API.
        configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
         parámetros necesarios como la url de la API. Debe ser inicializado a partir del
         fichero de configuración configuracion/configuracion.yaml.
        init_data (:class:`Boolean`): True para traer todos los códigos de las listas,
         False para no traerlos. Por defecto toma el valor False.

    Attributes:
        data (:obj:`Dicconario`) Diccionario con todas las codelists

    """

    def __init__(self, session, configuracion, translator, translator_cache, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.session = session
        self.configuracion = configuracion
        self.translator = translator
        self.translator_cache = translator_cache

        self.data = self.get(init_data)

    def get(self, init_data=True):
        codelists = {}
        self.logger.info('Solicitando información de las codelists')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}codelist').json()['data']['codelists']
        except KeyError:
            self.logger.error(
                'No se han extraído las codelist debido a un error de conexión con el servidor')
            return {}
        except Exception as e:
            raise e
        self.logger.info('Codelist extraídas correctamente')

        for codelist in response:
            agency = codelist['agencyID']
            codelist_id = codelist['id']
            version = codelist['version']
            names = codelist['names']
            des = codelist['descriptions'] if 'descriptions' in codelists else None
            # names, des = self.translate(names, des)
            if agency not in codelists:
                codelists[agency] = {}
            if codelist_id not in codelists[agency]:
                codelists[agency][codelist_id] = {}
            codelists[agency][codelist_id][version] = Codelist(self.session, self.configuracion, self.translator,
                                                               self.translator_cache, codelist_id, agency,
                                                               version, names, des, init_data=init_data)
        return codelists

    def create(self, agencia, codelist_id, version, nombres, descripciones):
        json = {'data': {'codelists': [
            {'agencyID': agencia, 'id': codelist_id, 'isFinal': 'true', 'names': nombres, 'descriptions': descripciones,
             'version': str(version)}]},
            'meta': {}}

        try:

            response = \
                self.session.post(f'{self.configuracion["url_base"]}createArtefacts',
                                  json=json)

            response.raise_for_status()
        except Exception as e:
            raise e

    def put(self, agencia, codelist_id, version, descripciones, nombres):
        json = {'data': {'codelists': [
            {'agencyID': agencia, 'id': codelist_id, 'isFinal': 'true', 'names': nombres, 'descriptions': descripciones,
             'version': version}]},
            'meta': {}}

        self.logger.info('Creando o actualizando codelist con id: %s', codelist_id)
        try:
            response = self.session.put(f'{self.configuracion["url_base"]}updateArtefacts', json=json)
            response.raise_for_status()
        except Exception as e:
            raise e
        self.logger.info('Codelist creada o actualizada correctamente')
        self.data = self.get(False)  # Provisional.

    def translate(self, translator, translations_cache, names, des):
        for data in [names, des]:
            if data:
                languages = self.configuracion['languages']
                to_translate_langs = list(set(languages) - list(data.keys()))
                value = data.values()[0]
                for target_lang in to_translate_langs:
                    data[target_lang] = self.__get_translate(translator, value, target_lang, translations_cache)
        return names, des

    def __get_translate(self, translator, value, target_language, translations_cache):
        if value in translations_cache:
            translation = translations_cache[value][target_language]
        else:
            translation = str(translator.translate_text(value, target_lang=target_language))
            translations_cache[value] = {}
            translations_cache[value][target_language] = translation
        return translation
