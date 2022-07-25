import pandas


class CategoryScheme:
    def __init__(self, session, configuracion, category_scheme_id, agency_id, version, init_data=False):
        self.session = session
        self.configuracion = configuracion
        self.category_scheme_id = category_scheme_id
        self.agency_id = agency_id
        self.version = version
        self.data = self.get_data() if init_data else None

    def get_data(self):
        response = self.session.get(
            f'{self.configuracion["url_base"]}categoryScheme/{self.category_scheme_id}/{self.agency_id}/{self.version}').json()[
            'data']['categorySchemes'][0]['categories']
        categories = {'id': [], 'name_es': [], 'name_en': [], 'des_es': [], 'des_en': []
            , 'parent': []}
        self.extract_categories(response, None, categories)
        res = pandas.DataFrame(categories)
        return res

    def extract_categories(self, response, parent, categories):
        for categorie in response:
            category_id = categorie['id']
            category_name_es = categorie['names']['es'] if 'es' in categorie['names'].keys() else None
            category_name_en = categorie['names']['en'] if 'en' in categorie['names'].keys() else None

            categories['id'].append(category_id)
            categories['name_es'].append(category_name_es)
            categories['name_en'].append(category_name_en)
            categories['parent'].append(parent)
            if 'descriptions' in categorie.keys():
                category_des_es = categorie['descriptions']['es'] if 'es' in categorie['descriptions'].keys() else None
                category_des_en = categorie['descriptions']['en'] if 'en' in categorie['descriptions'].keys() else None
            else:
                category_des_es = None
                category_des_en = None

            categories['des_es'].append(category_des_es)
            categories['des_en'].append(category_des_en)

            if 'categories' in categorie.keys():
                self.extract_categories(categorie['categories'], category_id, categories)

    def __repr__(self):
        return f'{self.category_scheme_id} {self.version}'
