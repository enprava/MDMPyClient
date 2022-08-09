import logging
import sys

from src.mdmpyclient.dsd import DSD

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class DSDs:
    """ Clase que representa el conjunto de DSDs del M&D Manager.

         Args:
             session (:class:`requests.session.Session`): Sesión autenticada en la API.
             configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
              parámetros necesarios como la url de la API. Debe ser inicializado a partir del
              fichero de configuración configuracion/configuracion.yaml.
             init_data (:class:`Boolean`): True para traer todos los DSDs, False para no traerlos.
              Por defecto toma el valor False.

         Attributes:
             data (:obj:`Dicconario`) Diccionario con todos los DSDs
         """

    def __init__(self, session, configuracion, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion

        self.data = self.get(init_data)

    def get(self, init_data):
        dsd = {}
        self.logger.info('Solicitando información de los DSDs')
        try:
            response = self.session.get(f'{self.configuracion["url_base"]}dsd')
            response_data = response.json()['data']['dataStructures']
        except KeyError:
            self.logger.info('No se han extraído los DSDs debido a un error de conexión con el servidor')
            return dsd
        except Exception as e:
            raise e
        self.logger.info('DSDs estraídos correctamente')

        for data_structure in response_data:
            dsd_id = data_structure['id']
            agency = data_structure['agencyID']
            version = data_structure['version']
            names = data_structure['names']
            des = data_structure['descriptions'] if 'descriptions' in data_structure.keys() else None

            if agency not in dsd:
                dsd[agency] = {}
            if dsd_id not in dsd[agency]:
                dsd[agency][dsd_id] = {}
            dsd[agency][dsd_id][version] = DSD(self.session, self.configuracion, dsd_id, agency, version, names, des,
                                               init_data)
        return dsd

    def put(self, agency, dsd_id, version, names, des, primary_meassure, dimensions, measure_dimensions,
            time_dimensions, attributes):
        # {"meta": {}, "data": {"dataStructures": [ {"id": "ASDF", "version": "1.0", "agencyID": "ESC01", "names": {
        # "es": "ASDFASDF"}, "dataStructureComponents": {"measureList": {"id": "MeasureDescriptor", "primaryMeasure":
        # {"id": "OBS_VALUE", "conceptIdentity":
        # "urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=ESC01:CS_ECONOMIC(1.0).TIPO_ESTABLECIMIENTO",
        # "localRepresentation": { "enumeration":
        # "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ESC01:CL_PROCEDENCIA_ECTA(1.0)"}}}, "dimensionList": {"id":
        # "DimensionDescriptor", "dimensions": [ {"id": "ASDF", "position": 0, "conceptIdentity":
        # "urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=ESC01:ASDF(1.0).R1", "localRepresentation": {
        # "enumeration": "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=SDMX:CL_FREQ(2.0)"}, "type": "Dimension"},
        # {"id": "TEMP", "position": 1, "conceptIdentity":
        # "urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=SDMX:CROSS_DOMAIN_CONCEPTS(2.0).AGE",
        # "localRepresentation": { "enumeration": "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=SDMX:CL_FREQ(2.0)"},
        # "type": "Dimension"}], "measureDimensions": [], "timeDimensions": []}, "attributeList": {"id":
        # "AttributeDescriptor", "attributes": [{"id": "OBS", "conceptIdentity":
        # "urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=SDMX:CROSS_DOMAIN_CONCEPTS(2.0).ACCURACY",
        # "localRepresentation": { "enumeration": "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ESC01:CL_OBS_STATUS(
        # 1.0)"}, "attributeRelationship": { "primaryMeasure": "OBS_VALUE"}, "assignmentStatus": "Conditional"}]}}}]}}
        pass
