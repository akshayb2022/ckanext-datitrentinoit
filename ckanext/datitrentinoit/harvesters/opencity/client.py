# -*- coding: utf-8 -*-
import json
import logging

from urllib.request import urlopen

from ckanext.datitrentinoit.harvesters.opencity import DATASET_LIMIT

log = logging.getLogger(__name__)


class OpenCityClient(object):
    def __init__(self, baseurl):
        self.baseurl = baseurl.rstrip("/")

    def get_resources(self):
        """return Opencity resource json"""
        url = f"{self.baseurl}" + "?limit=" + DATASET_LIMIT

        while True:
            log.debug("Retrieving at GeoNode URL %s", url)
            response = urlopen(url).read()
            json_content = json.loads(response)

            url = json_content["next"]

            objects = json_content["items"]
            for res in objects:
                lid = res["id"]
                ltitle = res["title"]
                log.info(f'Found id:{lid} "{ltitle}"')
                yield res

            if url is None:
                break
