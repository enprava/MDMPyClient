import copy
import logging
import sys

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
         data (:obj:`Dicconario`) Diccionario con todos los esquemas de conceptos
     """

    def __init__(self, session, configuracion, translator, translator_cache, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.translator = translator
        self.translator_cache = translator_cache

        self.data = self.get(init_data)

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
            concept_schemes[agency][cs_id][version] = ConceptScheme(self.session, self.configuracion, self.translator,
                                                                    self.translator_cache, cs_id, agency,
                                                                    version, names, des, init_data=init_data)
        return concept_schemes

    def put(self, agency, concepts_scheme_id, version, names, des):
        self.logger.info('Obteniendo esquema de conceptos con id: %s', concepts_scheme_id)
        try:
            concepts_scheme = self.data[agency][concepts_scheme_id][version]
            self.logger.info('El esquema de conceptos con id %s ya se encuentra en la API', concepts_scheme.id)
        except KeyError:
            self._put(agency, concepts_scheme_id, version, names, des)

    def _put(self, agency, concepts_scheme_id, version, names, des):
        if self.configuracion['translate']:
            self.logger.info('Traduciendo el esquema de concepto con id %s', concepts_scheme_id)
            names = self.translate(names)
            des = self.translate(des)
        json = {'data': {'conceptSchemes': [
            {'agencyID': agency, 'id': concepts_scheme_id, 'isFinal': 'true', 'names': names, 'descriptions': des,
             'version': str(version)}]},
            'meta': {}}
        self.logger.info('Creando o actualizando esquema de conceptos con id %s', concepts_scheme_id)
        try:
            response = self.session.put(f'{self.configuracion["url_base"]}updateArtefacts', json=json)

            response.raise_for_status()
        except Exception as e:
            raise e
        self.logger.info('Esquema de conceptos creado o actualizado correctamente')
        if agency not in self.data:
            self.data[agency] = {}
        if concepts_scheme_id not in self.data[agency].keys():
            self.data[agency][concepts_scheme_id] = {}
        self.data[agency][concepts_scheme_id][version] = ConceptScheme(self.session, self.configuracion,
                                                                       self.translator,
                                                                       self.translator_cache, concepts_scheme_id,
                                                                       agency,
                                                                       version, names, des, init_data=False)

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
        if value in self.translator_cache:
            self.logger.info('Valor encontrado en la caché de traducciones')
            if 'EN-GB' in target_language:
                target_language = 'en'
            translation = self.translator_cache[value][target_language]
        else:
            self.logger.info('Realizando petición a deepl')
            translation = str(self.translator.translate_text(value, target_lang=target_language))
            self.translator_cache[value] = {}
            if 'EN-GB' in target_language:
                target_language = 'en'
            self.translator_cache[value][target_language] = translation
        self.logger.info('Se ha traducido el término %s como %s', value, translation)
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

    def put_all_data(self):
        for agency in self.data.values():
            for scheme in agency.values():
                for version in scheme.values():
                    version.put()
