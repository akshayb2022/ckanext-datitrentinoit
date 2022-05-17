import json


def create_opencity_package_dict(harvest_object, config):
    json_dict = json.loads(harvest_object.content)

    package_dict = {
        "title": json_dict["title"],
        "short_description": json_dict["short_description"],
        "identifier": json_dict["identifier"],
        "geographical_geonames_url": json_dict["spatial_coverage"],
        "url": json_dict["uri"],
    }

    extras = {
        "temporal_coverage": [
            {
                "temporal_start": json_dict["start_date"],
                "temporal_end": json_dict["end_date"],
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