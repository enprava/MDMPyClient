import copy
import logging
import sys
#import os
import yaml

from mdmpyclient.conceptscheme.conceptscheme import ConceptScheme

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class ConceptSchemes:
    """ Clase que representa el conjunto de esquemas de conceptos del M&D Manager.

     Args:
         session (:class:`requests.session.Session`): Sesión autenticada en la API.
         configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
          parámetros necesarios como la url de la API. Debe ser inicializado a partir del
          fichero de configuración configuracion/configuracion.yaml.
         init_data (:class:`Boolean`): True para traer todos los conceptos de los esquemas,
          False para no traerlos. Por defecto toma el valor False.

     Attributes:
         data (:obj:`Diccionario`): Diccionario con todos los esquemas de conceptos
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
        concept_schemes = {}
        self.logger.info('Solicitando información de los esquemas de concepto')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}conceptScheme')
            response_data = response.json()['data']['conceptSchemes']
        except KeyError:
            self.logger.error(
                'No se han extraído los esquemas de concepto debido a un error de conexión con el servidor: %s',
                response.text)
            return concept_schemes
        except Exception as e:
            raise e
        self.logger.info('Esquemas de concepto extraídos correctamente')
        self.conceptscheme_list = []
        for cs in response_data:
            agency = cs['agencyID']
            cs_id = cs['id']
            version = cs['version']
            names = cs['names']
            des = cs['descriptions'] if 'descriptions' in cs.keys() else None

            if agency not in concept_schemes:
                concept_schemes[agency] = {}
            if cs_id not in concept_schemes[agency].keys():
                concept_schemes[agency][cs_id] = {}
            concept_sch = ConceptScheme(self.session, self.configuracion, self.translator,
                                                                    self.translator_cache, cs_id, agency,
                                                                    version, names, des, init_data=init_data)
            concept_schemes[agency][cs_id][version] = concept_sch
            self.conceptscheme_list.append(concept_sch)
        return concept_schemes



    def get_all_sdmx(self, directory):
        self.logger.info('Obteniendo todos los esquemas conceptuales en formato sdmx')
        for cs in self.conceptscheme_list:
            cs.get_sdmx(directory)

    def put(self, concept_scheme):
        self.logger.info('Obteniendo esquema de conceptos con id: %s', concept_scheme.id)
        try:
            concepts_scheme = self.data[concept_scheme.agency_id][concept_scheme.id][concept_scheme.version]
            self.logger.info('El esquema de conceptos con id %s ya se encuentra en la API', concepts_scheme.id)
        except KeyError:
            self._put(concept_scheme)

    def _put(self, concept_scheme):
        if self.configuracion['translate']:
            self.logger.info('Traduciendo nombre y descripción del esquema de concepto con id %s', concept_scheme.id)
            concept_scheme.names = self.translate(concept_scheme.names)
            concept_scheme.des = self.translate(concept_scheme.des) if concept_scheme.des else None
        json = {'data': {'conceptSchemes': [
            {'agencyID': concept_scheme.agency_id, 'id': concept_scheme.id, 'isFinal': 'true',
             'names': concept_scheme.names,
             'version': concept_scheme.version}]},
            'meta': {}}
        if concept_scheme.des:
            json['descriptions'] = concept_scheme.des
        self.logger.info('Creando o actualizando esquema de conceptos con id %s', concept_scheme.id)
        try:
            response = self.session.put(f'{self.configuracion["url_base"]}updateArtefacts', json=json)

            response.raise_for_status()
        except Exception as e:
            raise e
        self.logger.info('Esquema de conceptos creado o actualizado correctamente')
        if concept_scheme.agency_id not in self.data:
            self.data[concept_scheme.agency_id] = {}
        if concept_scheme.id not in self.data[concept_scheme.agency_id].keys():
            self.data[concept_scheme.agency_id][concept_scheme.id] = {}
        self.data[concept_scheme.agency_id][concept_scheme.id][concept_scheme.version] = concept_scheme

    def translate(self, data):
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
        return translation

    def delete_all(self, agency):
        try:  # Miramos que no este vacio self.data
            for cs_id, dict_concept_scheme in self.data[agency].items():
                if cs_id == 'CS_MSD':
                    continue
                for concept_scheme in dict_concept_scheme.values():
                    concept_scheme.delete()
        except KeyError:
            pass

    def init_concept_schemes(self, init_data=False):
        self.data = self.get(init_data)

    def put_all_data(self):
        self.logger.info('Realizando un put de todos los conceptos de cada esquema de conceptos')
        for agency in self.data.values():  # El nombre de la variable representa la clave que manejamos en el
            for scheme in agency.values():  # diccionario
                for version in scheme.values():
                    version.put()
        self.init_concept_schemes(True)

    def add_concept_scheme(self, agency, cs_id, version, names, des):
        try:
            concept_scheme = self.data[agency][cs_id][version]
        except KeyError:
            try:
                concept_scheme = self.data_to_upload[agency][cs_id][version]
            except KeyError:
                if agency not in self.data_to_upload:
                    self.data_to_upload[agency] = {}
                if cs_id not in self.data_to_upload[agency]:
                    self.data_to_upload[agency][cs_id] = {}
                concept_scheme = ConceptScheme(self.session, self.configuracion, self.translator, self.translator_cache,
                                               cs_id, agency, version, names, des, init_data=False)
                self.data_to_upload[agency][cs_id][version] = concept_scheme
        return concept_scheme

    def put_all_concept_schemes(self):
        self.logger.info('Realizando un put de todos los esquemas de concepto')
        for agency in self.data_to_upload.values():  # El nombre de la variable representa la clave que manejamos en el
            for scheme in agency.values():  # diccionario.
                for version in scheme.values():
                    self.put(version)
        self.data_to_upload = {}
        if self.configuracion['translate']:
            self.translate_all_concept_schemes()

    def translate_all_concept_schemes(self):
        self.logger.info('Traduciendo todos los esquemas de concepto')
        for agency in self.data.values():  # El nombre de la variable representa la clave que manejamos en el
            for scheme in agency.values():  # diccionario.
                for version in scheme.values():
                    version.translate()
