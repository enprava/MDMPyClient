class Dataset:
    def __init__(self, ckan):
        self.ckan = ckan
        self.datasets = ckan.call_action('package_list', {'all_fields':True})

    def create(self, name, org_id):
        self.ckan.call_action('package_create', {'name': name, 'private': False, 'owner_org': org_id})

