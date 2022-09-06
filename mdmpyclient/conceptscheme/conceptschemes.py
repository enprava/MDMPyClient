import logging
import sys

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

    def put(self, agency, conceptscheme_id, version, names, des):
        json = {'data': {'conceptSchemes': [
            {'agencyID': agency, 'id': conceptscheme_id, 'isFinal': 'true', 'names': names, 'descriptions': des,
             'version': str(version)}]},
            'meta': {}}

        self.logger.info('Creando o actualizando esquema de conceptos con id: %s', conceptscheme_id)
        try:
            response = self.session.put(f'{self.configuracion["url_base"]}updateArtefacts', json=json)

            response.raise_for_status()
        except Exception as e:
            raise e
        self.logger.info('Esquema de conceptos creado o actualizado correctamente')
