import json
from datetime import datetime


def create_opencity_package_dict(harvest_object, config):
    json_dict = json.loads(harvest_object.content)


    def dateformat(d):
        return datetime.fromisoformat(d).strftime('%d-%m-%Y')

    start_date = json_dict["start_date"]
    end_date = json_dict["end_date"]

    package_dict = {
        "title": json_dict["title"],
        "notes": json_dict['short_description'],
        "geographical_geonames_url": json_dict["spatial_coverage"],
        "url": json_dict["uri"],
    }

    extras = {
        "identifier": json_dict['identifier'],
        "temporal_coverage": [
            {
                "temporal_start": dateformat(start_date),
                "temporal_end": dateformat(end_date),
            }
        ]
    }

    return package_dict, extras


def addExtras(package_dict, extras):

    extras_as_dict = []
    for key, value in extras.items():
        if isinstance(value, (list, dict)):
            extras_as_dict.append({'key': key, 'value': json.dumps(value)})
        else:
            extras_as_dict.append({'key': key, 'value': value})

    package_dict['extras'] = extras_as_dict
    return package_dict