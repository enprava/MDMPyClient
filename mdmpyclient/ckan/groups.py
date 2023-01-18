import logging
import sys

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Groups:
    def __init__(self, ckan):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.ckan = ckan
        self.groups = self.get_groups()

    def get_groups(self):
        return self.ckan.call_action('group_list', {})

    def create_group(self, name, parent):
        self.ckan.call_action('group_create', {'name': name, 'groups': [{'name': parent}]})

    def test(self):
        return self.ckan.call_action('group_list', {'all_fields': True, 'include_groups': True})
