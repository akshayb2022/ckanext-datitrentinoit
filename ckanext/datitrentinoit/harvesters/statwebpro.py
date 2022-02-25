
import logging
import json
import requests
import urllib.request as r
from urllib.parse import urlparse, urlunparse

from ckan.plugins.core import SingletonPlugin

from ckanext.datitrentinoit.model.statweb_metadata import StatWebProIndex, StatWebProEntry, StatWebMetadataPro, \
    _safe_decode
import ckanext.datitrentinoit.model.mapping as mapping
from ckanext.datitrentinoit.harvesters.statwebbase import StatWebBaseHarvester
from ckanext.dcatapit.model import License

log = logging.getLogger(__name__)


class StatWebProHarvester(StatWebBaseHarvester, SingletonPlugin):
    '''
    Harvester per StatWeb Pro

    GATHER: fa richiesta al servizio indice e salva ogni entry in un HarvestObject
    FETCH:  legge l'HarvestObject, fa il retrieve dei metadati, aggiorna il contenuto dell'HarvestObject 
            aggiungendo i metadati appena caricati
    IMPORT: effettua il parsing dell'HarvestObject e crea/aggiorna il dataset corrispondente
    '''

    def info(self):
        return {
            'name': 'tn_statweb_pro',
            'title': 'StatWebPro',
            'description': 'Harvester per servizio StatWebPro',
            'form_config_interface': 'Text'
        }

    def harvester_name(self):
        return 'StatWebPro'

    ## IHarvester

    def validate_config(self, source_config):
        if not source_config:
            return source_config

        try:
            source_config_obj = json.loads(source_config)

            if 'groups' in source_config_obj:
                if not isinstance(source_config_obj['groups'], list):
                    raise ValueError('"groups" should be a list')

        except ValueError as e:
            raise e

        return source_config

    def create_index(self, url):
        log.info('%s: connecting to %s', self.harvester_name(), url)
        content = r.urlopen(url).read().decode()
        return StatWebProIndex(content)

    def create_package_dict(self, guid, content):
        swpentry = StatWebProEntry(txt=content)
        metadata = StatWebMetadataPro(obj=swpentry.get_metadata())
        package_dict = mapping.create_pro_package_dict(guid, swpentry, metadata, self.source_config)
        return package_dict, metadata

    def fetch_stage(self, harvest_object):

        # Check harvest object status
        status = self._get_object_extra(harvest_object, 'status')

        if status == 'delete':
            # No need to fetch anything, just pass to the import stage
            return True

        log = logging.getLogger(__name__ + '.fetch')
        log.debug('StatWebPro fetch_stage for object: %s', harvest_object.id)

        entry = StatWebProEntry(txt=harvest_object.content)
        url = entry.get_url()
        # rebuild item url, replacing scheme and netloc (workaround for bad data)
        url = reroute_url(url, harvest_object.job.source.url)

        identifier = harvest_object.guid

        log.info('Retrieving StatWebPro metadata from %s', url)

        try:
            content = r.urlopen(url).read().decode()
        except Exception as e:
            self._save_object_error('Error getting the StatWebPro record with GUID %s' % identifier, harvest_object)
            return False

        if content is None:
            self._save_object_error('Empty record for GUID %s' % identifier, harvest_object)
            return False

        metadata = StatWebMetadataPro(txt=content)
        entry.set_metadata(metadata.get_obj())

        # Update the harvest_object content, adding the metadata
        try:
            harvest_object.content = entry.tostring()
            harvest_object.save()
        except Exception as e:
            self._save_object_error(f'Error saving the harvest object for GUID {identifier} [{e}]',
                                    harvest_object)
            return False

        return True

    def attach_resources(self, metadata, package_dict, harvest_object):

        for resource_key in ["Indicatore", "TabNumeratore", "TabDenominatore"]:
            json_resource_url = metadata.get(resource_key)
            if not json_resource_url:
                continue

            log.debug('StatWebPro: loading resource %s', resource_key)

            # rebuild item url, replacing scheme and netloc (workaround for bad data)
            json_resource_url = reroute_url(json_resource_url, harvest_object.job.source.url)

            try:
                rdata = r.urlopen(json_resource_url).read().decode()
                robj = _safe_decode(rdata)
                log.debug('StatWebPro: loaded resource %s', resource_key)
            except Exception as e:
                log.error(f'StatWebPro error in GUID {harvest_object.guid} while loading resource {resource_key} at {json_resource_url}')
                continue

            res_title = list(robj.keys())[0]

            res_dict_json = {
                'name': res_title,
                'description': res_title,
                'url': json_resource_url,
                'format': 'json',
                'mimetype': 'application/json',
                'resource_type': 'api',
#                'last_modified': modified,
                'distribution_format': 'JSON',  # dcatapit
                'license_type': package_dict['license_url'],  # dcatapit
            }
            package_dict['resources'].append(res_dict_json)

            # Get also the twin CSV resource
            csv_resource_url = metadata.get(resource_key + "CSV")
            if not csv_resource_url:
                continue

            res_dict_csv = {
                'name': res_title,
                'description': res_title,
                'url': csv_resource_url,
                'format': 'csv',
                'mimetype': 'text/csv',
                'resource_type': 'file',
#                'last_modified': modified,
                'distribution_format': 'CSV',  # dcatapit
                'license_type': package_dict['license_url'],  # dcatapit
            }
            package_dict['resources'].append(res_dict_csv)


def reroute_url(original_url, destination_url):
    # rebuild item url, replacing scheme and netloc (workaround for bad data)
    destination_parsed = urlparse(destination_url)
    original_parsed = urlparse(original_url)
    original_parsed = original_parsed._replace(scheme=destination_parsed.scheme)
    original_parsed = original_parsed._replace(netloc=destination_parsed.netloc)
    return urlunparse(original_parsed)

