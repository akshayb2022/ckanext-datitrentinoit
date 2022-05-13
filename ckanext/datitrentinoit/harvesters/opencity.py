import logging

from ckan.plugins.core import SingletonPlugin
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)

class OpenCityHarvester(HarvesterBase, SingletonPlugin):
    '''
        Open City Harvester
    '''

    def gather_stage(self, harvest_job):
        pass        

    def fetch_stage(self, harvest_object):
        pass

    def import_stage(self, harvest_object):
        pass