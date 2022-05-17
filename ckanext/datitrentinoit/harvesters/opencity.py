import json
import logging
import uuid
from ckan import model
from ckan.model import Session
from ckan import plugins as p
from datetime import datetime

from ckan import logic
from ckan.lib.navl.validators import not_empty

from ckanext.datitrentinoit.harvesters.client import OpenCityClient
from ckanext.datitrentinoit.model.mapping import (
    create_opencity_package_dict,
    _extras_as_dict,
)
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra as HOExtra
from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class OpenCityHarvester(HarvesterBase, SingletonPlugin):
    """
    Open City Harvester
    """

    implements(IHarvester)

    _user_name = None

    source_config = {}

    def info(self):
        return {
            "name": "openCity",
            "title": "openCity harvester",
            "description": "Harvests openCity instances",
            "form_config_interface": "Text",
        }

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
        return True

    def import_stage(self, harvest_object):
        log = logging.getLogger(__name__ + ".import")
        log.debug("Import stage for harvest object: %s" % harvest_object.id)

        if not harvest_object:
            log.error("No harvest object received")
            return False

        self._set_source_config(harvest_object.source.config)

        status = self._get_object_extra(harvest_object, "status")

        # Get the last harvested object (if any)
        previous_object = (
            Session.query(HarvestObject)
            .filter(HarvestObject.guid == harvest_object.guid)
            .first()
        )

        if status == "delete":
            # Delete package
            context = {
                "model": model,
                "session": model.Session,
                "user": self._get_user_name(),
            }

            p.toolkit.get_action("package_delete")(
                context, {"id": harvest_object.package_id}
            )
            log.info(
                "Deleted package {0} with guid {1}".format(
                    harvest_object.package_id, harvest_object.guid
                )
            )

            return True

        if previous_object:
            # Flag previous object as not current anymore
            previous_object.current = False
            previous_object.add()

            content_old = previous_object.content
            content_new = harvest_object.content

            is_modified = content_old != content_new
            prev_job_id = previous_object.job.id
        else:
            is_modified = True
            prev_job_id = None

        # Error if GUID not present
        if not harvest_object.guid:
            self._save_object_error(
                "Missing GUID for object {0}".format(harvest_object.id),
                harvest_object,
                "Import",
            )
            return False

        log.error("Object GUID:%s is modified: %s" % (harvest_object.guid, is_modified))

        # Let's set the metadata date according to the import time. Not the best choice, since
        # we'd like to set the original metadata date.
        # If geonode provided this info, we could rely on this to find out if a dataset needs to be updated.
        harvest_object.metadata_modified_date = datetime.now()
        harvest_object.add()

        # Build the package dict
        package_dict = self.get_package_dict(harvest_object)
        if not package_dict:
            log.error(
                "No package dict returned, aborting import for object {0}".format(
                    harvest_object.id
                )
            )
            return False

        # Create / update the package

        context = {
            "model": model,
            "session": model.Session,
            "user": self._get_user_name(),
            "extras_as_string": True,
            "api_version": "2",
            "return_id_only": True,
        }
        if context["user"] == self._site_user["name"]:
            context["ignore_auth"] = True

        # The default package schema does not like Upper case tags
        tag_schema = logic.schema.default_tags_schema()
        tag_schema["name"] = [not_empty, str]

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        if status == "new":
            package_schema = logic.schema.default_create_package_schema()
            package_schema["tags"] = tag_schema
            context["schema"] = package_schema

            # We need to explicitly provide a package ID, otherwise ckanext-spatial
            # won't be be able to link the extent to the package.
            package_dict["id"] = str(uuid.uuid4())
            package_schema["id"] = [str]

            # Save reference to the package on the object
            harvest_object.package_id = package_dict["id"]
            harvest_object.add()
            # Defer constraints and flush so the dataset can be indexed with
            # the harvest object id (on the after_show hook from the harvester
            # plugin)
            Session.execute("SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED")
            model.Session.flush()

            try:
                package_id = self._create_package(context, package_dict, harvest_object)
                log.info(
                    "Created new package %s with guid %s"
                    % (package_id, harvest_object.guid)
                )
                self._post_package_create(package_id, harvest_object)
            except p.toolkit.ValidationError as e:
                self._save_object_error(
                    "Validation Error: %s" % str(e.error_summary),
                    harvest_object,
                    "Import",
                )
                return False

        elif status == "change":

            # Check if the document has changed

            if not is_modified:

                # Assign the previous job id to the new object to
                # avoid losing history
                harvest_object.harvest_job_id = prev_job_id
                harvest_object.add()

                harvest_object.metadata_modified_date = (
                    previous_object.metadata_modified_date
                )

                # Delete the previous object to avoid cluttering the object table
                previous_object.delete()

                log.info(
                    "Document with GUID %s unchanged, skipping...", harvest_object.guid
                )
                model.Session.commit()
                return "unchanged"
            else:
                package_schema = logic.schema.default_update_package_schema()
                package_schema["tags"] = tag_schema
                context["schema"] = package_schema

                package_dict["id"] = harvest_object.package_id
                try:
                    package_id = self._update_package(
                        context, package_dict, harvest_object
                    )
                    log.info(
                        "Updated package %s with guid %s",
                        package_id,
                        harvest_object.guid,
                    )
                    self._post_package_update(package_id, harvest_object)
                except p.toolkit.ValidationError as e:
                    self._save_object_error(
                        "Validation Error: %s" % str(e.error_summary),
                        harvest_object,
                        "Import",
                    )
                    return False

        model.Session.commit()

        return True

    def _post_package_create(self, package_id, harvest_object):
        pass

    def _create_package(self, context, package_dict, harvest_object):
        package_dict["name"] = self._gen_new_name(package_dict["title"])

        # We need to get the owner organization (if any) from the harvest source dataset
        source_dataset = model.Package.get(harvest_object.source.id)

        if source_dataset.owner_org:
            package_dict["owner_org"] = source_dataset.owner_org

        package_id = p.toolkit.get_action("package_create")(context, package_dict)
        return package_id

    def get_package_dict(self, harvest_object):
        package_dict, extras = create_opencity_package_dict(
            harvest_object, self.source_config
        )
        package_dict = _extras_as_dict(package_dict, extras)
        return package_dict

    def _get_object_extra(self, harvest_object, key):
        """
        Helper function for retrieving the value from a harvest object extra,
        given the key
        """
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return None

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
