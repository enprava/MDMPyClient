import copy
import logging
import sys
import os
import pandas
import requests
import yaml

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class CategoryScheme:
    """ Clase que representa un esquema de conceptos del M&D Manager.

    Args:
        session (:class:`requests.session.Session`): Sesión autenticada en la API.
        configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
         parámetros necesarios como la url de la API. Debe ser inicializado a partir del
         fichero de configuración configuracion/configuracion.yaml.
        category_scheme_id (class: `String`): Identificador del esquema de categorías.
        agency_id (class: `String`): Identificador de la agencia vinculada.
        version (class: `String`): Versión del esquema de categorías.
        names (class: `Diccionario`): Diccionario con los nombres del esquema de categorías
         en varios idiomas.
        des (class: `String`): Diccionario con las descripciones del esquema de categorías
         en varios idiomas.
        init_data (class: `Boolean`): True para traer todos los datos del esquema de
         categorías, False para traer solo id, agencia y versión. Por defecto toma el valor False.

    Attributes:
        categories (:obj:`DataFrame`): DataFrame con todas las categorías del esquema

    """

    def __init__(self, session, configuracion, translator, translator_cache, category_scheme_id, agency_id, version,
                 names, des, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.translator = translator
        self.translator_cache = translator_cache
        self.id = category_scheme_id
        self.agency_id = agency_id
        self.version = version
        self.names = names
        self.des = des
        self.categories = self.get(init_data)
        self.categories_to_upload = pandas.DataFrame(columns=['Id', 'ParentCode', 'Name', 'Description'])

    def get(self, init_data):
        categories = {'id': [], 'parent': [], 'id_cube_cat': []}
        for language in self.configuracion['languages']:
            categories[f'name_{language}'] = []
            categories[f'des_{language}'] = []
        if init_data:
            self.logger.info('Solicitando información del esquema de categorías con id: %s', self.id)
            try:
                response = self.session.get(
                    f'{self.configuracion["url_base"]}categoryScheme/{self.id}/{self.agency_id}/{self.version}')
                response_data = response.json()['data']['categorySchemes'][0]['categories']
                response_dcs = self.session.get(f'{self.configuracion["url_base"]}dcs').json()
                response.raise_for_status()
            except KeyError:
                self.logger.error(
                    'Ha ocurrido un error mientras se cargaban los datos del esquema de categorías con id: %s', self.id)
                self.logger.error(response.text)
                return pandas.DataFrame(data=categories, dtype='string')
            except Exception as e:
                raise e
            if 'errorCode' not in response_dcs:
                self.logger.info('Esquema de categorías extraído correctamente')
            else:
                self.logger.error(
                    'Ha ocurrido un error en la extracción del sistema de categorías, pruebe a establecer un sistema de'
                    ' categorías para los cubos')
            dcs = self.__dcs_to_dict(response_dcs)
            categories = self.__merge_categories(response_data, None, dcs, categories)
        return pandas.DataFrame(data=categories, dtype='string')

    def get_sdmx(self, directory):
        """

        Args:
            directory: Obtiene el esquema de categorías en formato sdmx.

        Returns: None

        """
        self.logger.info('Obteniendo esquema de categoría con id %s en formato sdmx', self.id)
        try:

            response = self.session.get(
                f'{self.configuracion["url_base"]}downloadMetadati/categoryScheme/{self.id}/{self.agency_id}/'
                f'{self.version}/structure/true/false/es')
            response.raise_for_status()
        except Exception as e:
            raise e
        path = os.path.join(directory, self.id + '.xml')
        with open(path, 'w', encoding='utf-8') as file:
            file.write(response.text)
            file.close()

    def add_category(self, category_id, parent, name, des):
        """

        Args:
            category_id: Id de la categoría que se va a añadir.
            parent: Parent de la categoría que se va a añadir.
            name: Nombre de la categoría que se va a añadir.
            des: Descripción de la categoría que se va a añadir.

        Returns: None

        """
        if category_id.upper() not in self.categories_to_upload.Id.values:
            self.categories_to_upload.loc[len(self.categories_to_upload)] = [category_id.upper(), parent, name, des]

    def put(self, lang='es'):
        to_upload = len(self.categories_to_upload)
        if to_upload:
            self.logger.info('Se han detectado %s nuevas categorías para subir al esquema con id %s', to_upload,
                             self.id)
            csv = self.categories_to_upload
            csv = csv.to_csv(sep=';', index=False).encode(encoding='utf-8')
            columns = {"id": 0, "name": 2, "description": 3, "parent": 1, "order": -1, "fullName": -1,
                       "isDefault": -1}
            response = self.__upload_csv(csv, columns)
            self.__export_csv(response)
            self.categories_to_upload.apply(lambda x: self.create_cube_category(x.Id, x.ParentCode, {'es': x.Name}),
                                            axis=1)
            self.init_categories()
            self.categories_to_upload = self.categories_to_upload[0:0]

        else:
            self.logger.info('El esquema de categorías con id %s está actualizado', self.id)

    def create_cube_category(self, cube_id, parent, names):
        """

        Args:
            cube_id: Id de la de categoría que se va a crear
            parent: Parent de la categoría que se va a crear
            names: Nombre de la categoría que se va a crear

        Returns: None

        """
        if cube_id not in self.categories.id.values:
            json = {"catCode": cube_id, "parCode": parent, "ord": None, "labels": names}
            try:
                response = self.session.post(f'{self.configuracion["url_base"]}InsertDCS', json=json)
                response.raise_for_status()
            except Exception as e:
                raise e

    def import_dcs(self):
        """

            Establece el esquema de categoría como predeterminado en el gestor de cubos

        """
        self.logger.info(
            'Se van a importar todas las categorías del esquema con id %s a las categorías de los cubos(DCS)', self.id)
        try:
            response = self.session.get(
                f'{self.configuracion["url_base"]}importDCS/{self.id}/{self.agency_id}/{self.version}')
            response.raise_for_status()
        except Exception as e:
            raise e
        self.logger.info('Se han importado las categorías correctamente')

    def set_permissions(self, agencies=None):
        """

        Args:
            agencies: (:class: `List`): Agencias a las que dar permiso

        Returns:

        """
        if not agencies:
            agencies = ["BE2", "BIS", "ECB", "ESC01", "ESTAT", "FAO", "IAEG-SDGs", "ILO", "IMF", "IT1", "OECD", "TN1",
                        "UNICEF", "UNSD", "WB", "SDMX", "IECA"]
        token = self.session.headers['Authorization'][7:]
        categories = []
        try:
            self.logger.info('Sincronizando la base datos AUTHDB')
            self.session.post(f'{self.configuracion["url_base"]}SynchronizeAuthDB').raise_for_status()
            self.logger.info('Obteniendo los DCs')
            response = self.session.get(f'{self.configuracion["url_base"]}dcs')
            response.raise_for_status()
        except Exception as e:
            raise e
        for dc in response.json():
            categories.append(dc['CatCode'])
        json = {
            "agencies": agencies,
            "category": categories, "cube": [], "cubeOwner": [],
            "functionality": ["agency-schemes", "app", "artefact-browser", "attribute-file", "builder",
                              "categorisations", "category-schemes", "category-schemes-and-dataflows", "codelists",
                              "compare-dsds", "compare-item-schemes", "concept-schemes", "configurations",
                              "content-constraints", "cube-list", "data-consumer-schemes", "dataflow-builder",
                              "dataflows", "data-manager", "data-provider-schemes", "data-structure-definitions",
                              "dcat-ap-it", "ddb-reset", "file-mapping", "hierarchical-codelists", "import-structures",
                              "loader", "manage-series", "merge-item-schemes", "metadataflows", "metadata-set",
                              "meta-manager", "msds", "nodes", "organization-unit-schemes", "permissions",
                              "provision-agreements", "reference-metadata", "registrations", "remove-temp-tables",
                              "structure-sets", "synchronize-codelists", "update-databrowser-cache", "upgrade-dsd",
                              "user-management", "users", "utilities"
                              ],
            "rules": ["AdminRole", "CanDeleteData", "CanDeleteStructuralMetadata", "CanIgnoreProductionFlag",
                      "CanImportData", "CanImportStructures", "CanManageWorkingAnnotation", "CanModifyStoreSettings",
                      "CanPerformInternalMappingConfig", "CanReadData", "CanReadStructuralMetadata", "CanUpdateData",
                      "CanUpdateStructuralMetadata", "DataImporterRole", "DataImporterRole_U", "DataImporterRole_UD",
                      "DomainUserRole", "StructureImporterRole", "StructureImporterRole_U", "StructureImporterRole_UD",
                      "WsUserRole"], "dataflow": [], "metadataFlow": [], "dataflowOwner": [],
            "metadataFlowOwner": [], "username": "admin", "email": None,
            "token": token,
            "isAuthenticated": False}
        try:
            self.logger.info('Estableciendo permisos para las categorías')
            response = self.session.post(f'{self.configuracion["url_base"]}AssignsAll', json=json)
            response.raise_for_status()
        except Exception as e:
            raise e
        self.logger.info('Permisos establecidos correctamnte')

    def get_category_hierarchy(self, category):
        """

        Args:
            category: (:class:`String`): Id de la categoría que se quiere encontrar

        Returns: Jerarquía de categorías

        """
        self.logger.info('Obteniendo jerarquía de la categoría %s', category)
        hierarchy = self._get_category_hierarchy(category, category)
        self.logger.info('La jerarquía obtenida es %s', hierarchy)
        return hierarchy

    def _get_category_hierarchy(self, category, hierarchy):
        parent = self.categories.loc[self.categories['id'] == category].parent.item()
        if pandas.isnull(parent):
            return hierarchy
        return self._get_category_hierarchy(parent, f'{parent}.{hierarchy}')

    def __upload_csv(self, csv, columns, lang='es'):
        upload_headers = self.session.headers.copy()
        custom_data = str(
            {"type": "categoryScheme",
             "identity": {"ID": self.id, "Agency": self.agency_id, "Version": self.version},
             "lang": lang,
             "firstRowHeader": 'true',
             "columns": columns, "textSeparator": ";", "textDelimiter": 'null'})

        files = {'file': (
            'potato.csv', csv, 'application/vnd.ms-excel', {}),
            'CustomData': (None, custom_data)}
        body, content_type = requests.models.RequestEncodingMixin._encode_files(files, {})
        body = body.decode('utf-8')
        upload_headers['Content-Type'] = content_type
        upload_headers['language'] = lang
        try:
            self.logger.info('Subiendo categorías a la API')
            response = self.session.post(
                f'{self.configuracion["url_base"]}CheckImportedFileCsvItem', headers=upload_headers,
                data=body)

            response.raise_for_status()

        except Exception as e:
            raise e
        response = response.json()
        response['identity']['ID'] = self.id
        response['identity']['Agency'] = self.agency_id
        response['identity']['Version'] = self.version
        return response

    def __export_csv(self, json):
        try:
            self.logger.info('Importando categorías al esquema')
            self.session.post(
                f'{self.configuracion["url_base"]}importFileCsvItem',
                json=json)

        except Exception as e:
            raise e
        self.logger.info('Categorías importados correctamente')

    def __merge_categories(self, response, parent, dcs, categories):
        for categorie in response:
            category_id = categorie['id']
            for key, column in categories.items():
                try:
                    if 'cube' in key:
                        column.append(dcs[category_id])
                    elif 'id' in key:
                        column.append(category_id)
                    elif 'parent' in key:
                        column.append(parent)
                    else:
                        lang = key[-2:]
                        if 'name' in key:
                            column.append(categorie['names'][lang])
                        if 'des' in key:
                            column.append(categorie['descriptions'][lang])
                except Exception:
                    column.append(None)
            if 'categories' in categorie:
                self.__merge_categories(categorie['categories'], category_id, dcs, categories)
        return categories

    # TODO
    def __dcs_to_dict(self, response_dcs):
        dcs = {}
        for dc in response_dcs:
            dcs[dc['CatCode']] = dc['IDCat']
        return dcs

    def translate(self):
        """

            Traduce el esquema de categorías.

        """
        languages = copy.deepcopy(self.configuracion['languages'])
        self.logger.info('Iniciando proceso de traducción para el esquema de categorías con id %s', self.id)
        categories = self.__translate(self.categories)

        columns = {"id": 0, "name": 2, "description": 3, "parent": 1, "order": -1, "fullName": -1,
                   "isDefault": -1}
        n_translations = len(categories)
        self.logger.info('Se han traducido %s categorías')
        if n_translations:
            for language in languages:
                categories_to_upload = categories.copy(deep=True)
                categories_to_upload = categories_to_upload[['id', 'parent', f'name_{language}', f'des_{language}']]
                categories_to_upload.columns = ['Id', 'Parent', 'Name', 'Description']
                csv = categories_to_upload.to_csv(sep=';', index=False).encode(encoding='utf-8')

                response = self.__upload_csv(csv, columns, lang=language)
                self.__export_csv(response)

    def __translate(self, data):
        columns = data.columns[3:]
        categories = data.copy()
        codes_translated = pandas.DataFrame(columns=categories.columns)
        for column in columns:
            target_language = column[-2:]
            source_languages = self.configuracion['languages'].copy()
            source_languages.remove(target_language)
            if target_language == 'en':
                target_language = 'EN-GB'
            source_language = source_languages[-1]
            source_column = column[:-2] + source_language
            to_be_translated_indexes = categories[categories[column].isnull()][column].index
            fake_indexes = categories[categories[source_column].isnull()][source_column].index
            to_be_translated_indexes = to_be_translated_indexes.difference(fake_indexes, sort=False)
            indexes_size = len(to_be_translated_indexes)
            if not indexes_size:
                continue
            categories[column][to_be_translated_indexes] = categories[source_column][to_be_translated_indexes].map(
                lambda value, tl=target_language: self.__get_translate(value, tl))

            with open(f'{self.configuracion["cache"]}', 'w', encoding='utf=8') as file:
                yaml.dump(self.translator_cache, file)
            codes_translated = pandas.concat(
                [codes_translated, categories.iloc[to_be_translated_indexes]])  # Se guardan los codigos traducidos
        self.logger.info('Proceso de traducción finalizado')
        return codes_translated

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

    def init_categories(self):
        self.categories = self.get(True)

    def __repr__(self):
        return f'{self.id} {self.version}'
