import pandas
import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Codelist:
    """ Clase que representa una codelist del M&D Manager.

    Args:
        session (:class:`requests.session.Session`): Sesión autenticada en la API.
        configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
         parámetros necesarios como la url de la API. Debe ser inicializado a partir del
         fichero de configuración configuracion/configuracion.yaml.
        codelist_id (:class:`String`): Identificador de la codelist.
        version (:class:`String`): Versión de la codelist.
        agency_id (:class:`String`): Agencia vinculada a la codelist.
        init_data (:class:`Boolean`): True para traer todos los datos de la codelist,
         False para traer solo id, agencia y versión. Por defecto toma el valor False.

    Attributes:



    """

    def __init__(self, session, configuracion, codelist_id, agency_id, version, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = codelist_id
        self.version = version
        self.agency_id = agency_id
        self.data = self.get_data() if init_data else None

    def get_data(self):
        codes = {'id': [], 'name_es': [], 'name_en': [], 'parent': [], 'des_es': [], 'des_en': []}
        try:
            response = self.session.post(f'{self.configuracion["url_base"]}NOSQL/codelist/',
                                         json={"id": self.id, "agencyId": self.agency_id,
                                               "version": self.version,
                                               "lang": "es",
                                               "pageNum": 1, "pageSize": 5000000, "rebuildDb": False}).json()['data'][
                'codelists'][0]['codes']
        except KeyError:
            self.logger.warning(
                f'Ha ocurrido un error inesperado mientras se cargaban los datos de la codelist con id: {self.id}')
            return codes

        for code in response:
            code_id = code['id']
            name_es = code['names']['es'] if 'es' in code['names'].keys() else None
            name_en = code['names']['en'] if 'en' in code['names'].keys() else None
            code_parent = code['parent'] if 'parent' in code.keys() else None

            codes['id'].append(code_id)
            codes['name_es'].append(name_es)
            codes['name_en'].append(name_en)
            codes['parent'].append(code_parent)

            if 'descriptions' in code.keys():
                des_es = code['descriptions']['es'] if 'es' in code['descriptions'].keys() else None
                des_en = code['descriptions']['en'] if 'en' in code['descriptions'].keys() else None
            else:
                des_es = None
                des_en = None

            codes['des_es'].append(des_es)
            codes['des_en'].append(des_en)
        return pandas.DataFrame(data=codes)

    def __repr__(self):
        return f'{self.id} {self.version}'
