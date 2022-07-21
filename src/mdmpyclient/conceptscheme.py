import pandas


class ConceptScheme:

    def __init__(self, session, configuracion, cs_id, agency_id, version, init_data=False):
        self.session = session
        self.configuracion = configuracion
        self.cs_id = cs_id
        self.agency_id = agency_id
        self.version = version
        self.concepts = self.get_data() if init_data else None

    def get_data(self):
        response = self.session.get(
            f'{self.configuracion["url_base"]}conceptScheme/{self.cs_id}/{self.agency_id}/{self.version}').json()[
            'data']['conceptSchemes'][0]['concepts']

        concepts = {'concept_id': [], 'concept_name': [], 'concept_order': []}

        for concept in response:
            concept_id = concept['id']
            concept_name = concept['name']
            concept_order = concept['annotations'][0]['text']

            concepts['concept_id'].append(concept_id)
            concepts['concept_name'].append(concept_name)
            concepts['concept_order'].append(concept_order)

        return pandas.DataFrame(data=concepts)

    def __repr__(self):
        return f'{self.cs_id} {self.version}'
