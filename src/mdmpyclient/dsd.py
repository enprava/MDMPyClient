import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class DSD:
    """ Clase que representa un DSD del M&D Manager.

        Args:
            session (:class:`requests.session.Session`): Sesión autenticada en la API.
            configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
             parámetros necesarios como la url de la API. Debe ser inicializado a partir del
             fichero de configuración configuracion/configuracion.yaml.
            dsd_id (class: 'String'): Identificador del DSD.
            agency_id (class: `String`): Identificador de la agencia vinculada.
            version (class: `String`): Version del DSD.
            names (class: `Diccionario`): Diccionario con los nombres del DSD en varios idiomas.
            des (class: `String`): Diccionario con las descripciones del DSD en varios idiomas.
            init_data (class: `Boolean`): True para traer todos los datos del DSD,
             False para traer solo id, agencia y versión. Por defecto toma el valor False.

        Attributes:
            data (:obj:`DataFrame`) DataFrame con todos los datos del DSD
        """

    def __init__(self, session, configuracion, dsd_id, agency_id, version, names, des, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = dsd_id
        self.agency_id = agency_id
        self.version = version
        self.names = names
        self.des = des
        self.data = self.get() if init_data else None

    def get(self):
        data = {}
        try:
            response = \
                self.session.get(
                    f'{self.configuracion["url_base"]}dsd/{self.id}/{self.agency_id}/{self.version}')
            response_data = response.json()['data'][
                'dataStructures'][0]['dataStructureComponents']
        except KeyError:
            self.logger.warning('Ha ocurrido un error mientras se cargaban los datos del DSD con id: %s',
                                self.id)
            self.logger.error(response.text)
            return data
        except Exception as e:
            raise e

        data['attributes'] = self.__get_attributes(response_data['attributeList']['attributes'])
        data['dimensions'] = self.__get_dimensions(response_data['dimensionList'])
        data['primary_meassure'] = self.__get_meassures(response_data['measureList']['primaryMeasure'])

        return data

    def __get_attributes(self, attribute_list):
        attributes = []
        for attribute in attribute_list:
            id = attribute['id']
            concept = attribute['conceptIdentity']
            codelist = self.__decode_codelist_info(attribute['localRepresentation']['enumeration'])
            assignment_status = attribute['assignmentStatus']
            # EL NIVEL DE APEGO SE CONSIGUE DEPENDIENDO DE LO QUE HAYA AQUI. SI ES VACIO ES DATASET, SI TIENE LA
            # PRIMARY MEASSURE ES OBSERVATION, SI ES DIMENSIONGROUP TIENE UNA LISTA DE DIMENSIONES Y SI TIENE GRUPOS
            # ES GROUP(ESTE ULTIMO LO SUPONGO, NO LO SE 100%
            attribute_relationship = attribute['attributeRelationship']
            attributes.append(
                {'id': id, 'concept': concept, 'codelist': codelist, 'assignment_status': assignment_status,
                 'relationship': attribute_relationship})
        return attributes

    def __decode_codelist_info(self, info):
        index = info.find('=') + 1
        return info[index:]

    def __get_dimensions(self, dimension_list):
        dimensions = []
        measures = dimension_list['measureDimensions'] if 'measureDimensions' in dimension_list else []
        dimension_iterator = dimension_list['dimensions'] + measures + dimension_list[
            'timeDimensions']
        for dimension in dimension_iterator:
            id = dimension['id']
            pos = dimension['position']
            type = dimension['type']
            concept = dimension['conceptIdentity']
            codelist = dimension['localRepresentation']['enumeration'] if 'enumeration' in dimension[
                'localRepresentation'] else None
            dimensions.append({'id': id, 'pos': pos, 'type': type, 'concept': concept, 'codelist': codelist})
        return dimensions

    def __get_meassures(self, meassure_list):
        id = meassure_list['id']
        concept = meassure_list['conceptIdentity']
        codelist = meassure_list['localRepresentation'][
            'enumeration'] if 'localRepresentation' in meassure_list else None
        return {'id': id, 'concept': concept, 'codelist': codelist}

    def init_data(self):
        self.data = self.get()

    def __repr__(self):
        return f'{self.id} {self.version}'
