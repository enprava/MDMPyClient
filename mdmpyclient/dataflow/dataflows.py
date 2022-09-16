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

    def put(self, code, agency, version, names, des, columns, cube_id, dsd, category_scheme, category):
        self.logger.info('Creando dataflow con id %s', code)
        try:
            self.logger.info('El dataflow ya se encuentra en la API')
            return self.data[agency][code][version].id
        except KeyError:
            self.logger.info('El dataflow no se encuentra en la API. Creando dataflow con id %s', code)
        hierarchy = category_scheme.get_category_hierarchy(category)
        json = {
            "ddbDF": {"ID": code, "Agency": agency, "Version": version, "labels": names, "IDCube": cube_id,
                      "DataflowColumns": columns,
                      "filter": {"FiltersGroupAnd": {}, "FiltersGroupOr": {}}}, "msdbDF": {"meta": {}, "data": {
                "dataflows": [
                    {"id": code, "version": version, "agencyID": agency, "isFinal": True, "names": names,
                     "structure": f"urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure={dsd.agency_id}:{dsd.id}({dsd.version})"}]}},
            "msdbCat": {"meta": {}, "data": {"categorisations": [
                {"id": f"CAT_{code}", "version": "1.0", "agencyID": agency, "names": {"en": f"CAT_{code}"},
                 "source": f"urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow={agency}:{code}({version})",
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
