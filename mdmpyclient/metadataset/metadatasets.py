import logging
import sys

from metadataset import Metadataset

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Metadatasets:
    """ Clase que representa el conjunto de metadatasets del M&D Manager.

                   Args:
                       session (:class:`requests.session.Session`): Sesión autenticada en la API.
                       configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                        parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                        fichero de configuración configuracion/configuracion.yaml.
                       init_data (:class:`Boolean`): True para traer todos los datos metadatasets, False para no
                        traerlos. Por defecto toma el valor False.

                   Attributes:
                       data (:obj:`Dicconario`) Diccionario con todos los metadatasets"""

    def __init__(self, session, configuracion, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion

        self.data = self.get(init_data)

    def get(self, init_data=True):
        data = {}
        self.logger.info('Solicitando información de los metadatasets')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}api/RM/getJsonMetadatasetList')
            response_data = response.json()['data']['metadataSets']
        except Exception as e:
            raise e
        if not response_data:
            self.logger.warning('No se han encontrado metadatasets en la API')
            return data
        self.logger.info('Metadatasets extraídos correctamente')
        for metadata in response_data:
            meta_id = metadata['id']
            names = metadata['names']

            data[meta_id] = Metadataset(self.session, self.configuracion, meta_id, names, init_data)
        return data

    def put(self, agency, md_set_id, names, md_flow_id, md_version, category_scheme_id, hierarchy, cat_version):
        try:
            self.logger.info('Obteniendo metadataset con id %s', md_set_id)

            if (self.data[md_set_id]):
                self.logger.info('El metadataset ya se encuentra en la API')
        except KeyError:
            self.logger.info('El metadataset no se encuentra en la API, creando metadataset')
            json = {"meta": {}, "data": {"metadataSets": [{"id": md_set_id, "names": names,
                        "annotations": [
                            {"id": "MetadataflowId", "texts": {"en": md_flow_id}},
                            {"id": "MetadataflowAgency", "texts": {"en": agency}},
                            {"id": "MetadataflowVersion",
                            "texts": {"en": md_version}},
                            {"id": "MSDId",
                            "texts": {"en": self.configuracion['msd']['id']}},
                            {"id": "MSDAgency",
                            "texts": {"en": self.configuracion['msd']['agency']}},
                            {"id": "MSDVersion",
                            "texts": {"en": self.configuracion['msd']['version']}},
                            {"id": f"categorisation_[CAT_{md_set_id}]", "texts": {
                            "en": f"CAT_{md_set_id}+{agency}+{md_version}+urn:sdmx:org.sdmx."
                                "infomodel.metadatastructure.Metadataflow="
                                f"{agency}:{md_flow_id}({md_version})+urn:sdmx:org.sdmx.infomodel"
                                f".categoryscheme.Category={agency}:"
                                f"{category_scheme_id}({cat_version}).{hierarchy}"},
                                'text': f"CAT_{md_set_id}+{agency}+{md_version}+urn:sdmx:org.sdmx."
                                "infomodel.metadatastructure.Metadataflow="
                                f"{agency}:{md_flow_id}({md_version})+urn:sdmx:org.sdmx.infomodel"
                                f".categoryscheme.Category={agency}:"
                                f"{category_scheme_id}({cat_version}).{hierarchy}"}],
                                "links": [{"rel": "msd",
                                    "urn": "urn:sdmx:org.sdmx.infomodel.metadatastructure"
                                    f".MetadataStructure={self.configuracion['msd']['agency']}"
                                    f":{self.configuracion['msd']['id']}({self.configuracion['msd']['version']})"}]}]}}
            try:
                response = self.session.post(f'{self.configuracion["url_base"]}api/RM/upsertJsonMetadataSet', json=json)
                response.raise_for_status()
            except Exception as e:
                raise e
            self.logger.info('Metadataset creado correctamente')
            self.data[md_set_id] = Metadataset(self.session, self.configuracion, md_set_id, names, False)

    def delete_all(self):
        self.logger.info('Borrando todos los metadatasets y sus reportes')
        for metadataset in self.data.values():
            if metadataset.id == 'ASF':
                continue
            metadataset.delete()
