import copy
import logging
import sys
# import os
import yaml
from ftfy import fix_encoding

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
        data (:obj:`Diccionario`): Diccionario con todas las codelists

    """

    def __init__(self, session, configuracion, translator, translator_cache, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.session = session
        self.configuracion = configuracion
        self.translator = translator
        self.translator_cache = translator_cache

        self.data = self.get(init_data)
        self.data_to_upload = {}

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
        self.codelist_list = []
        for codelist in response:
            agency = codelist['agencyID']
            codelist_id = codelist['id']
            version = codelist['version']
            names = codelist['names']
            des = codelist['descriptions'] if 'descriptions' in codelists else None
            if agency not in codelists:
                codelists[agency] = {}
            if codelist_id not in codelists[agency]:
                codelists[agency][codelist_id] = {}

            cl = Codelist(self.session, self.configuracion, self.translator,
                          self.translator_cache, codelist_id, agency,
                          version, names, des, init_data=init_data)
            codelists[agency][codelist_id][version] = cl
            self.codelist_list.append(cl)
        return codelists

    def get_all_sdmx(self, directory):
        """

        Args:
            directory: (:class:`String`) Directorio donde se van a guardar todas las codelist en formato sdmx

        Returns: None

        """
        self.logger.info('Obteniendo todos los dataflows en formato sdmx')
        for cl in self.codelist_list:
            cl.get_sdmx(directory)

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

    def put(self, codelist):
        self.logger.info('Creando o actualizando codelist con id: %s', codelist.id)
        try:
            codelist = self.data[codelist.agency_id][codelist.id][codelist.version]
            self.logger.info('La codelist con id %s ya se encuentra actualizada', codelist.id)
        except KeyError:
            self._put(codelist)

    def _put(self, codelist):
        if self.configuracion['translate']:
            self.logger.info('Traduciendo nombre y descripción de la codelist con id %s', codelist.id)
            codelist.names = self.translate(codelist.names)
            codelist.des = self.translate(codelist.des) if codelist.des else None

        for language in codelist.names.keys():
            codelist.names[language] = fix_encoding((codelist.names[language]))
        json = {'data': {'codelists': [
            {'agencyID': codelist.agency_id, 'id': codelist.id, 'isFinal': 'true', 'names': codelist.names,
             'version': codelist.version}]},
            'meta': {}}
        if codelist.des:
            json['descriptions'] = codelist.des
        try:
            response = self.session.put(f'{self.configuracion["url_base"]}updateArtefacts', json=json)
            response.raise_for_status()
        except Exception as e:
            raise e
        self.logger.info('Codelist creada o actualizada correctamente')
        try:
            self.data[codelist.agency_id]
        except Exception:
            self.data[codelist.agency_id] = {}
        if codelist.id not in self.data[codelist.agency_id]:
            self.data[codelist.agency_id][codelist.id] = {}
        self.data[codelist.agency_id][codelist.id][codelist.version] = codelist

    def translate(self, data):
        """

        Args:
            data: (:class:`Dictionary`) Diccionario de claves idiomas en cadenas de caracteres y de valores cadenas de caracteres.

        Returns:

        """
        result = copy.deepcopy(data)
        languages = copy.deepcopy(self.configuracion['languages'])
        to_translate_langs = list(set(languages) - set(result.keys()))
        value = list(result.values())[0]
        for target_lang in to_translate_langs:
            if 'en' in target_lang:
                target_lang = 'EN-GB'
            translate = self.__get_translate(value, target_lang)
            if 'EN-GB' in target_lang:
                target_lang = 'en'
            result[target_lang] = translate
        with open(f'{self.configuracion["cache"]}', 'w', encoding='utf=8') as file:
            yaml.dump(self.translator_cache, file)
        return result

    def __get_translate(self, value, target_language):
        self.logger.info('Traduciendo el término %s al %s', value, target_language)
        if 'EN-GB' in target_language:
            target_language = 'en'

        try:
            if value in self.translator_cache and target_language in self.translator_cache[value]:
                self.logger.info('Valor encontrado en la caché de traducciones')
                translation = self.translator_cache[value][target_language]
            else:
                if 'en' in target_language:
                    target_language = 'EN-GB'
                self.logger.info('Realizando petición a deepl para traducir el valor %s al %s', value, target_language)
                translation = str(self.translator.translate_text(value, target_lang=target_language))
                if 'EN-GB' in target_language:
                    target_language = 'en'
                self.translator_cache[value] = {}
                self.translator_cache[value][target_language] = translation
            self.logger.info('Traducido el término %s como %s', value, translation)
        except:
            translation = 'Error de traducción automática. Póngase en contacto con el administrador.'

        return translation

    def delete_all(self, agency):
        try:  # Miramos que no este vacio self.data
            for codelist_id, dict_codelist in self.data[agency].items():
                for codelist in dict_codelist.values():
                    if codelist_id in ('OBS_STATUS', 'CL_SEXO'):
                        continue
                    codelist.delete()
        except KeyError:
            pass

    def init_codelists(self, init_data=False):
        self.data = self.get(init_data)

    def put_all_data(self):
        self.logger.info('Realizando un put de todos los códigos de todas las codelist')
        for agency in self.data.values():
            for codelist in agency.values():
                for version in codelist.values():
                    version.put()
        self.init_codelists(True)

    def init_codelist(self, init_data):
        self.data = self.get(init_data)

    def add_codelist(self, agency, cl_id, version, names, des):
        try:
            codelist = self.data[agency][cl_id][version]
        except KeyError:
            try:
                codelist = self.data_to_upload[agency][cl_id][version]
            except KeyError:
                if agency not in self.data_to_upload:
                    self.data_to_upload[agency] = {}
                if cl_id not in self.data_to_upload[agency]:
                    self.data_to_upload[agency][cl_id] = {}
                codelist = Codelist(self.session, self.configuracion, self.translator, self.translator_cache, cl_id,
                                    agency, version, names, des, init_data=False)
                self.data_to_upload[agency][cl_id][version] = codelist
        return codelist

    def put_all_codelists(self):
        self.logger.info('Realizando un put de todas las codelist')
        for agency in self.data_to_upload.values():
            for codelist in agency.values():
                for version in codelist.values():
                    self.put(version)
        self.data_to_upload = {}
        if self.configuracion['translate']:
            self.translate_all_codelists()

    def translate_all_codelists(self):
        self.logger.info('Iniciado proceso de traducción de todas las codelist')
        for agency in self.data.values():
            for codelist in agency.values():
                for version in codelist.values():
                    if version.id == 'CL_ESTRATO_ASALARIADOS':
                        continue
                    version.translate()
