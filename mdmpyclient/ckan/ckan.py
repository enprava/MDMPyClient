from ckanapi import RemoteCKAN

from dataset import Datasets
from groups import Groups
from organizations import Organizations
from resource import Resource
from tags import Tags


class Ckan:
    def __init__(self, configuracion):
        self.configuracion = configuracion
        self.ckan = RemoteCKAN(configuracion['url_ckan'],
                               apikey=configuracion['api_ckan'])
        self.orgs = Organizations(self.ckan)
        self.groups = Groups(self.ckan)
        self.datasets = Datasets(self.ckan)
        self.resources = Resource(self.ckan)
        self.tags = Tags(self.ckan)
