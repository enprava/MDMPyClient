import pandas as pd
import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Organizations:
    def __init__(self, ckan):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.ckan = ckan
        self.orgs = self.get_orgs()

    def remove_all_orgs(self):
        for org in self.orgs:
            self.ckan.call_action('organization_purge', {'id': org['id']})

    def create_org(self, id, name, parent=None):
        if not pd.isna(parent):
            self.ckan.call_action('organization_create', {'name': id.lower(), 'title': name.lower(),
                                                          "groups":
                                                              [{
                                                                  "name": parent.lower()
                                                              }]
                                                          })
        else:
            self.ckan.call_action('organization_create', {'name': id.lower(), 'title': name.lower(),
                                                          "groups":
                                                              [{
                                                                  "name": "child-org"
                                                              }]
                                                          })

    def create_orgs(self, orgs):
        self.logger.info('Creando esquemas de categoría como organizaciones')
        orgs.apply(lambda x: self.create_org(x.id, x.name_es, x.parent), axis=1)

    def get_orgs(self): #TODO Implementarlo para un caso general de n organizaciones
        orgs = {}
        orgs_list = self.ckan.call_action('organization_list', {'all_fields': True, 'offset': 0})
        orgs_list = orgs_list + self.ckan.call_action('organization_list', {'all_fields': True, 'offset': 25})
        orgs_list = orgs_list + self.ckan.call_action('organization_list', {'all_fields': True, 'offset': 50})
        for org in orgs_list:
            orgs[org['name']] = org['id']
        return orgs
