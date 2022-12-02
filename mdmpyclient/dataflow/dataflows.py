import logging
import sys
import copy
import yaml

from mdmpyclient.dataflow.dataflow import Dataflow

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Dataflows:
    """ Clase que representa el conjunto de dataflows del M&D Manager.

               Args:
                   session (:class:`requests.session.Session`): Sesión autenticada en la API.
                   configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                    parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                    fichero de configuración configuracion/configuracion.yaml.
                   init_data (:class:`Boolean`): True para traer todos los datos dataflows, False para no
                    traerlos. Por defecto toma el valor False.

               Attributes:
                   data (:obj:`Dicconario`) Diccionario con todos los dataflows

               """

    def __init__(self, session, configuracion, translator, translator_cache, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.translator = translator
        self.translator_cache = translator_cache
        self.data = self.get(init_data)


    def get_all_sdmx(self, directory):
        self.logger.info('Obteniendo todos los dataflows en formato sdmx')
        for df in self.dataflow_list:
            df.get_sdmx(directory)

    def get(self, init_data=True):
        data = {}
        self.logger.info('Solicitando información de los dataflows')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}ddbDataflow')
            response_data = response.json()
        except Exception as e:
            raise e
        self.logger.info('Dataflows extraídos correctamente')
        self.dataflow_list = []

        for dataflow in response_data:
            code = dataflow['ID']
            dataflow_id = dataflow['IDDataflow']
            cube_id = dataflow['IDCube']
            agency_id = dataflow['Agency']
            version = dataflow['Version']
            # filter = dataflow['Filter']
            names = dataflow['labels']
            des = dataflow['Descriptions'] if 'Descriptions' in dataflow else None

            if agency_id not in data:
                data[agency_id] = {}
            if code not in data[agency_id]:
                data[agency_id][code] = {}

            df = Dataflow(self.session, self.configuracion, code, agency_id, version,
                                                      dataflow_id, cube_id, names, des, init_data)
            data[agency_id][code][version] = df
            self.dataflow_list.append(df)

        return data

    #comentario-tox: partir la funcion en dos
    def put(self, code, agency, version, names, des, columns, cube_id, dsd, category_scheme, category):

        self.logger.info('Creando dataflow con id %s', code)
        try:
            dataflow = self.data[agency][code][version].id
            self.logger.info('El dataflow ya se encuentra en la API')
            return dataflow
        except KeyError:
            self.logger.info('El dataflow no se encuentra en la API. Creando dataflow con id %s', code)

        if self.configuracion['translate']:
            self.logger.info('Traduciendo nombre y descripción del dataflow con id (code) %s', code)
            info_translated = [self.traslate(names),self.translate(des) if des else None]

        else:
            info_translated = [names,des]

        hierarchy = category_scheme.get_category_hierarchy(category)

        json = {
            "ddbDF": {"ID": code, "Agency": agency, "Version": version, "labels": info_translated[0], "IDCube": cube_id,
                      "DataflowColumns": columns,
                      "filter": {"FiltersGroupAnd": {}, "FiltersGroupOr": {}}}, "msdbDF": {"meta": {}, "data": {
                "dataflows": [
                    {"id": code, "version": version, "agencyID": agency, "isFinal": True, "names": info_translated[0],
                     "structure": "urn:sdmx:org.sdmx.infomodel.datastructure.Data" + \
                                  f"Structure={dsd.agency_id}:{dsd.id}({dsd.version})"}]}},
            "msdbCat": {"meta": {}, "data": {"categorisations": [
                {"id": f"CAT_{code}_{cube_id}", "version": version, "agencyID": agency, "names": {"en": f"CAT_{code}"},
                 "source": f"urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow={agency}:{code}({version})",
                 "target": f"urn:sdmx:org.sdmx.infomodel.categoryscheme.Category={category_scheme.agency_id}" + \
                           f":{category_scheme.id}({category_scheme.version}).{hierarchy}"}]}}}


        return self.translate_json(info_translated[1],json)



    def translate_json(self,des_translated,json):

        if des_translated:
            json['msdbDF']['data']['dataflows'][0]['descriptions'] = des_translated
        try:
            response = self.session.post(f'{self.configuracion["url_base"]}createDDBDataflow', json=json)
            response.raise_for_status()
        except Exception as e:
            raise e
        self.logger.info('Dataflow creado correctamente')
        dataflow_id = int(response.text)
        return dataflow_id



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
