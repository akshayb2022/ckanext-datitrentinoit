  
import logging

from rdflib.namespace import Namespace, RDF, SKOS
from rdflib import URIRef, BNode, Literal

import ckan.logic as logic

from ckanext.dcat.profiles import RDFProfile, DCAT, LOCN, VCARD, DCT, FOAF, ADMS, OWL, SCHEMA, TIME
from ckanext.dcatapit.dcat.profiles import remove_unused_object

DCATAPIT = Namespace('http://dati.gov.it/onto/dcatapit#')

log = logging.getLogger(__name__)


class DatitrentinoitProfile(RDFProfile):
    """
    Customization of DCATAP-IT for Regione Umbria
    """

    def parse_dataset(self, dataset_dict, dataset_ref):
        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        g = self.g
        org_dict = _get_org(dataset_dict.get('owner_org'))

        # Remove previous poc
        for s, p, o in g.triples((dataset_ref, DCAT.contactPoint, None)):
            log.info(f"UmbriaProfile: Removing contactPoint {o}")
            g.remove((s, p, o))
            remove_unused_object(g, o, "contactPoint (UMBRIA)")

        # Add new poc
        create_point = dataset_dict.get('contact_point')

        poc = BNode()
        g.add((dataset_ref, DCAT.contactPoint, poc))
        g.add((poc, RDF.type, DCATAPIT.Organization))
        g.add((poc, RDF.type, VCARD.Kind))
        g.add((poc, RDF.type, VCARD.Organization))

        g.add((poc, VCARD.fn, Literal(create_point if create_point else org_dict.get('create_point'))))
        

def _get_org(org_id):
    org_dict = {}
    if org_id:
        try:
            org_dict = logic.get_action('organization_show')(
                {'ignore_auth': True},
                {'id': org_id,
                 'include_datasets': False,
                 'include_tags': False,
                 'include_users': False,
                 'include_groups': False,
                 'include_extras': True,
                 'include_followers': False}
                )
        except Exception as err:
            log.warning("Cannot get org for %s: %s", org_id, err, exc_info=err)

    return org_dict