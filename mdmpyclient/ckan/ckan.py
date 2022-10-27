from ckanapi import RemoteCKAN

from mdmpyclient.ckan.dataset import Datasets
from mdmpyclient.ckan.organizations import Organizations
from mdmpyclient.ckan.resource import Resource
from mdmpyclient.ckan.tags import Tags


class Ckan:
    def __init__(self, configuracion):
        self.configuracion = configuracion
        self.ckan = RemoteCKAN(configuracion['url_ckan'],
                               apikey=configuracion['api_ckan'])
        self.orgs = Organizations(self.ckan)
        self.datasets = Datasets(self.ckan)
        self.resources = Resource(self.ckan)
        self.tags = Tags(self.ckan)