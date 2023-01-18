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
        self.logger.info('Se van a eliminar todas las organizaciones')
        for org in self.orgs.items():
            self.ckan.call_action('organization_purge', {'id': org[1]})
        self.orgs = {}
        self.logger.info('Se han eliminado todas las organizaciones')

    def create_org(self, org_id, name, parent=None):
        self.logger.info('Creando organizacion con id %s', org_id)
        if not pd.isna(parent):
            self.ckan.call_action('organization_create', {'name': org_id.lower(), 'title': name.lower(),
                                                          "groups":
                                                              [{
                                                                  "name": parent.lower()
                                                              }]
                                                          })
        else:
            self.ckan.call_action('organization_create', {'name': org_id.lower(), 'title': name.lower(),
                                                          "groups":
                                                              [{
                                                                  "name": "child-org"
                                                              }]
                                                          })
        self.logger.info('Organizacion creada correctamente')

    def create_orgs(self, orgs):
        self.logger.info('Creando esquemas de categoría como organizaciones')
        orgs.apply(lambda x: self.create_org(x.id, x.name_es, x.parent), axis=1)

    def get_orgs(self):
        self.logger.info('Obteniendo todas las organizaciones')
        orgs = {}
        orgs_list = self.ckan.call_action('organization_list', {'all_fields': True, 'offset': 0})
        org_cont = self.ckan.call_action('organization_list', {'all_fields': True, 'offset': 25})
        i = 50
        while org_cont:
            orgs_list = orgs_list + org_cont
            org_cont = self.ckan.call_action('organization_list', {'all_fields': True, 'offset': i})
            i += 25

        for org in orgs_list:
            orgs[org['name']] = org['id']
        self.logger.info('Organizaciones extraídas correctamente')
        return orgs
