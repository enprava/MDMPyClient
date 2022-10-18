class Organizations:
    def __init__(self, ckan):
        self.ckan = ckan
        self.orgs = ckan.call_action('organization_list', {'all_fields': True, 'include_groups': True})

    def remove_all_orgs(self):
        for org in self.orgs:
            self.ckan.call_action('organization_delete', {'id': org['id']})

    def create_org(self, id, name, parent=None):
        if parent:
            self.ckan.call_action('organization_create', {'name': id, 'title': name,
                                                          "groups":
                                                              [{
                                                                  "name": parent
                                                              }]
                                                          })
        else:
            self.ckan.call_action('organization_create', {'name': id, 'title': name,
                                                          "groups":
                                                              [{
                                                                  "name": "child-org"
                                                              }]
                                                          })

    def create_orgs(self, orgs):
        orgs.apply(lambda x: self.create_org(x.id, x.name, x.parent), axis=1)
