import logging
import sys
import pandas as pd

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Groups:
    def __init__(self, ckan):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.ckan = ckan
        self.groups = self.get_groups()

    def get_groups(self):
        self.logger.info('Obteniendo todas los grupos')
        groups = {}
        groups_list = self.ckan.call_action('group_list', {'all_fields': True, 'offset': 0})
        group_cont = self.ckan.call_action('group_list', {'all_fields': True, 'offset': 25})
        i = 50
        while group_cont:
            groups_list = groups_list + group_cont
            group_cont = self.ckan.call_action('group_list', {'all_fields': True, 'offset': i})
            i += 25

        for group in groups_list:
            groups[group['name']] = group['id']
        self.logger.info('grupos extraídos correctamente')
        return groups

    def create_group(self, group_id, name, parent):
        self.logger.info('Creando grupo con id %s', group_id)
        if pd.isna(name):
            name = group_id

        self.ckan.call_action('group_create', {'id': group_id.lower(), 'name': name, 'title': name.lower(),
                                               "groups":
                                                   [{
                                                       "name": parent.lower() if not pd.isna(parent) else "child-group"
                                                   }]
                                               })

        self.logger.info('grupo creado correctamente')

    def create_groups(self, groups):
        self.logger.info('Creando esquemas de categoría como grupos')
        groups.apply(lambda x: self.create_group(x.id, x.name_es, x.parent), axis=1)
