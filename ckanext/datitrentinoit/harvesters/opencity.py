import json
import logging

from ckan import model
from ckan.plugins.core import SingletonPlugin

from ckanext.datitrentinoit.harvesters.client import OpenCityClient
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra as HOExtra
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class OpenCityHarvester(HarvesterBase, SingletonPlugin):
    """
    Open City Harvester
    """

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + ".gather")

        # # Get source URL
        url = harvest_job.source.url

        self._set_source_config(harvest_job.source.config)

        try:
            log.info("Connecting to Open City  at %s", url)

            query = (
                model.Session.query(HarvestObject.guid, HarvestObject.package_id)
                .filter(HarvestObject.current == True)
                .filter(HarvestObject.harvest_source_id == harvest_job.source.id)
            )

            guid_to_package_id = {}
            for guid, package_id in query:
                guid_to_package_id[guid] = package_id

            guids_in_db = list(guid_to_package_id.keys())

            ho_ids = []

            client = OpenCityClient(url)

            # dict guid: layer
            harvested = []

            cnt_upd = 0
            cnt_add = 0

            for obj in client.get_resources():
                id = obj["id"]
                doc = json.dumps(obj)
                if id in guids_in_db:
                    ho = HarvestObject(
                        guid=id,
                        job=harvest_job,
                        content=doc,
                        package_id=guid_to_package_id[id],
                        extras=[HOExtra(key="status", value="change")],
                    )
                    action = "UPDATE"
                    cnt_upd = cnt_upd + 1
                else:
                    ho = HarvestObject(
                        guid=id,
                        job=harvest_job,
                        content=doc,
                        extras=[HOExtra(key="status", value="new")],
                    )
                    action = "ADD"
                    cnt_add = cnt_add + 1

                ho.save()
                ho_ids.append(ho.id)
                harvested.append(id)
                log.info(f"Queued open_city uuid {id} for {action}")

        except Exception as e:
            self._save_gather_error("Error harvesting Opencity: %s" % e, harvest_job)
            return None

        delete = set(guids_in_db) - set(harvested)

        log.info(
            f"Found {len(harvested)} objects,  {cnt_add} new, {cnt_upd} to update, {len(delete)} to remove"
        )

        for guid in delete:
            ho = HarvestObject(
                guid=guid,
                job=harvest_job,
                package_id=guid_to_package_id[guid],
                extras=[HOExtra(key="status", value="delete")],
            )
            model.Session.query(HarvestObject).filter_by(guid=guid).update(
                {"current": False}, False
            )
            ho.save()
            ho_ids.append(ho.id)

        if len(harvested) == 0 and len(delete) == 0:
            self._save_gather_error("No records received from GeoNode", harvest_job)
            return None

        return ho_ids

    def fetch_stage(self, harvest_object):
        pass

    def import_stage(self, harvest_object):
        pass

    def _set_source_config(self, config_str):
        """
        Loads the source configuration JSON object into a dict for
        convenient access
        """
        if config_str:
            self.source_config = json.loads(config_str)
            log.debug("Using config: %r", self.source_config)
        else:
            self.source_config = {}
