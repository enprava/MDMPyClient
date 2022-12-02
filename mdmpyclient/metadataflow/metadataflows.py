import logging
import sys

from mdmpyclient.metadataflow.metadataflow import Metadataflow

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Metadataflows:
    """ Clase que representa el conjunto de metadataflows del M&D Manager.

                Args:
                    session (:class:`requests.session.Session`): Sesión autenticada en la API.
                    configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                     parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                     fichero de configuración configuracion/configuracion.yaml.
                    init_data (:class:`Boolean`): True para traer todos los datos metadataflows, False para no
                     traerlos. Por defecto toma el valor False.

                Attributes:
                    data (:obj:`Dicconario`) Diccionario con todos los metadataflows

                """

    def __init__(self, session, configuracion, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion

        self.data = self.get(init_data)

    def get(self, init_data=True):
        data = {}
        self.logger.info('Solicitando información de los metadataflows')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}metadataflow')
            response_data = response.json()['data']['metadataflows']
        except KeyError:
            self.logger.warning('No se han encontrado metadataflows en la API')
            return data
        except Exception as e:
            raise e
        self.logger.info('Metadataflows extraídos correctamente')

        for metadata in response_data:
            meta_id = metadata['id']
            agency = metadata['agencyID']
            version = metadata['version']
            names = metadata['names']
            des = metadata['descriptions']

            if agency not in data:
                data[agency] = {}
            if meta_id not in data[agency]:
                data[agency][meta_id] = {}
            data[agency][meta_id][version] = Metadataflow(self.session, self.configuracion, meta_id, agency, version,
                                                          names, des, init_data)
        return data

    def put(self, agency, id_msd, version, names, des):
        self.logger.info('Obteniendo el MSD con id %s', id_msd)
        try:
            if(self.data[agency][id_msd][version]):
                self.logger.info('El MSD ya se encuentra en la API')
        except KeyError:
            self.logger.info('El MSD no se encuentra en la API, realizando peticion para crearlo')
            json = {"meta": {}, "data": {"metadataflows": [
            {"id": id_msd, "version": version, "agencyID": agency, "isFinal": True, "names": names, 'descriptions': des,
                 "structure": "urn:sdmx:org.sdmx.infomodel.metadatastructure.MetadataStructure=" + \
                    f"{self.configuracion['msd']['agency']}:{self.configuracion['msd']['id']}" + \
                    f"({self.configuracion['msd']['version']})"}]}}
            try:
                response = self.session.post(f'{self.configuracion["url_base"]}createArtefacts', json=json)
                response.raise_for_status()
            except Exception as e:
                raise e
            if response.text == 'true':
                self.logger.info('El MSD se ha creado correctamente')
                if agency not in self.data:
                    self.data[agency] = {}
                if id_msd not in self.data[agency]:
                    self.data[agency][id_msd] = {}
                self.data[agency][id_msd][version] = Metadataflow(self.session, self.configuracion, id_msd, agency,
                                                              version,
                                                              names, des, False)
            else:
                self.logger.error('Ha ocurrido un error y no se ha creado el MSD')

    def delete_all(self):
        self.logger.info('Borrando todos los metadataflows')
        for agency in self.data.values():
            for id_ag in agency.values():
                for version in id_ag.values():
                    if version.id == 'ASDF':
                        continue
                    version.delete()
