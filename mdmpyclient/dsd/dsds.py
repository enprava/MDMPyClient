import logging
import sys

from mdmpyclient.dsd.dsd import DSD

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

    def put(self, agency, dsd_id, version, names, des, dimensions):
        try:
            dsd = self.data[agency][dsd_id][version]
            self.logger.info('El DSD con id %s ya se encuentra en la API', dsd.id)
        except Exception:
            self.logger.info('Creando DSD con ID %s', dsd_id)
            self._put(agency, dsd_id, version, names, des, dimensions)

    def _put(self, agency, dsd_id, version, names, des, dimensions):
        format_dimensions = self.format_dimensions(dimensions)
        json = {"meta": {},
                "data": {"dataStructures": [{"id": dsd_id, "version": version, "agencyID": agency, "names":
                    names, "description": des, "isFinal": "true", "dataStructureComponents": {
                    "measureList": {"id": "MeasureDescriptor", "primaryMeasure":
                        {"id": "OBS_VALUE", "conceptIdentity":
                            "urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=SDMX:CROSS_DOMAIN_CONCEPTS(2.0).OBS_VALUE"}}
                    , "dimensionList": {"id": "DimensionDescriptor",
                                        "dimensions": format_dimensions + [
                                            {"id": "FREQ", "position": len(format_dimensions) + 1,
                                             "conceptIdentity":
                                                 "urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=SDMX:CROSS_DOMAIN_CONCEPTS(2.0).FREQ",
                                             "type": "Dimension"}, {"id": "INDICATOR",
                                                                    "position": len(format_dimensions),
                                                                    "type": "Dimension",
                                                                    "conceptIdentity": "urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=SDMX:CROSS_DOMAIN_CONCEPTS(2.0).INDICATOR",
                                                                    "localRepresentation": {
                                                                        "enumeration": "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ESC01:CL_UNIT(1.0)"}}]
                        , "measureDimensions": [], "timeDimensions": [{"id": "TIME_PERIOD",
                                                                       "position": len(format_dimensions) + 2,
                                                                       "type": "TimeDimension",
                                                                       "conceptIdentity": "urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=SDMX:CROSS_DOMAIN_CONCEPTS(2.0).TIME_PERIOD",
                                                                       "localRepresentation": {"textFormat": {
                                                                           "textType": "ObservationalTimePeriod",
                                                                           "isSequence": False,
                                                                           "isMultiLingual": False}}}]},
                    "attributeList": {"id": "AttributeDescriptor",
                                      "attributes": [{"id": "OBS_STATUS",
                                                      "conceptIdentity": "urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=SDMX:CROSS_DOMAIN_CONCEPTS(2.0).OBS_STATUS",
                                                      "localRepresentation": {
                                                          "enumeration": "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=estat:CL_OBS_STATUS(2.2)"},
                                                      "assignmentStatus": "Conditional",
                                                      "attributeRelationship": {"primaryMeasure": "OBS_VALUE"},
                                                      "assignmentStatus": "Conditional"}]}}}]}}
        try:
            response = self.session.post(f'{self.configuracion["url_base"]}createArtefacts', json=json)
            response.raise_for_status()
        except Exception as e:
            raise e
        self.logger.info('DSD creado correctamente')

    def format_dimensions(self, dimensions):
        result = []
        i = 0
        for id, values in dimensions.items():
            dimension = {}
            concept_scheme = values['concept_scheme']
            codelist = values['codelist']
            dimension['id'] = id
            dimension['position'] = i
            i += 1
            dimension['type'] = 'Dimension'
            dimension[
                'conceptIdentity'] = f'urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept={concept_scheme["agency"]}' \
                                     f':{concept_scheme["id"]}({concept_scheme["version"]})' \
                                     f'.{concept_scheme["concepto"]}'
            dimension['localRepresentation'] = {
                'enumeration': f'urn:sdmx:org.sdmx.infomodel.codelist.Codelist={codelist["agency"]}'
                               f':{codelist["id"]}({codelist["version"]})'}
            result.append(dimension)
        return result

    def delete_all(self, agency):
        if len(self.data): #Miramos que no este vacio self.data
            for dict_dsd in self.data[agency].values():
                for dsd in dict_dsd.values():
                    dsd.delete()
