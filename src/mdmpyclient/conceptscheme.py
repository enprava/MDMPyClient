import pandas


class ConceptScheme:

    def __init__(self, session, configuracion, cs_id, agency_id, version, init_data=False):
        self.session = session
        self.configuracion = configuracion
        self.cs_id = cs_id
        self.agency_id = agency_id
        self.version = version
        self.data = self.get_data() if init_data else None

    def get_data(self):
        response = self.session.get(
            f'{self.configuracion["url_base"]}conceptScheme/{self.cs_id}/{self.agency_id}/{self.version}').json()[
            'data']['conceptSchemes'][0]['concepts']

        concepts = {'id': [], 'name_es': [], 'name_en': [], 'des_es': [], 'des_en': []}

        for concept in response:
            concept_id = concept['id']
            name_es = concept['names']['es'] if 'es' in concept['names'].keys() else None
            name_en = concept['names']['en'] if 'en' in concept['names'].keys() else None

            concepts['id'].append(concept_id)
            concepts['name_es'].append(name_es)
            concepts['name_en'].append(name_en)
            if 'descriptions' in concept.keys():
                des_es = concept['descriptions']['es'] if 'es' in concept['descriptions'].keys() else None
                des_en = concept['descriptions']['en'] if 'en' in concept['descriptions'].keys() else None

            else:
                des_es = None
                des_en = None
            concepts['des_es'].append(des_es)
            concepts['des_en'].append(des_en)
        return pandas.DataFrame(data=concepts)

    def __repr__(self):
        return f'{self.cs_id} {self.version}'
