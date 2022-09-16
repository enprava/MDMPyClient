import logging
import sys

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

    def __init__(self, session, configuracion, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.data = self.get(init_data)

    def get(self, init_data=True):
        data = {}
        self.logger.info('Solicitando información de los dataflows')

        try:
            response = self.session.get(f'{self.configuracion["url_base"]}ddbDataflow')
            response_data = response.json()
        except Exception as e:
            raise e
        self.logger.info('Dataflows extraídos correctamente')

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
            data[agency_id][code][version] = Dataflow(self.session, self.configuracion, code, agency_id, version,
                                                      dataflow_id, cube_id, names, des, init_data)

        return data

    def put(self, id, agency, version, names, des, cube_id, dsd, category_scheme, category):
        self.logger.info('Obteniendo dataflow con id %s', id)
        # try:
        #     dataflow = self.data[agency][id][version]
        # except KeyError:
        #     self.logger.info('El dataflow no se encuentra en la API. Creando dataflow con id %s', id)
        hierarchy = category_scheme.get_category_hierarchy(category)
        json = {
            "ddbDF": {"ID": id, "Agency": agency, "Version": version, "labels": names, "IDCube": cube_id,
                      "DataflowColumns": ["ID_INDICATOR", "ID_FREQ", "ID_SEXO", "ID_EDAD", "ID_EPA_RELACTIVIDAD",
                                          "ID_EPA_NIVEL", "ID_TERRITORIO", "ID_CNAE09", "ID_CNO11",
                                          "ID_EPA_NACIONALIDAD", "ID_EPA_CLASEINACTIV", "ID_CNED2014",
                                          "ID_EPA_ESTRUCHOGAR", "ID_EPA_TIPACTIVIDADHOGAR", "ID_TIME_PERIOD",
                                          "ID_OBS_STATUS", "OBS_VALUE"],
                      "filter": {"FiltersGroupAnd": {}, "FiltersGroupOr": {}}}, "msdbDF": {"meta": {}, "data": {
                "dataflows": [
                    {"id": id, "version": version, "agencyID": agency, "isFinal": True, "names": names,
                     "structure": f"urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure={dsd.agency_id}:{dsd.id}({dsd.version})"}]}},
            "msdbCat": {"meta": {}, "data": {"categorisations": [
                {"id": f"CAT_{id}", "version": "1.0", "agencyID": agency, "names": {"en": f"CAT_{id}"},
                 "source": f"urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow={agency}:{id}({version})",
                 "target": f"urn:sdmx:org.sdmx.infomodel.categoryscheme.Category={category_scheme.agency_id}:{category_scheme.id}({category_scheme.version}).{hierarchy}"}]}}}
        if des:
            json['msdbDF']['data']['dataflows'][0]['descriptions'] = des
        try:
            response = self.session.post(f'{self.configuracion["url_base"]}createDDBDataflow', json=json)
            response.raise_for_status()
        except Exception as e:
            raise e
        self.logger.info('Dataflow creado correctamente')
        dataflow_id = int(response.text)
        return dataflow_id
