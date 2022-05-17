import json
import logging

from rdflib.namespace import Namespace, RDF, SKOS
from rdflib import BNode, Literal, URIRef

from ckanext.datitrentinoit.model.mapping import ISPAT_BASE_URL
from ckanext.dcat.profiles import RDFProfile, DCAT, VCARD, DCT
from ckanext.dcatapit.dcat.profiles import remove_unused_object

DCATAPIT = Namespace('http://dati.gov.it/onto/dcatapit#')

log = logging.getLogger(__name__)


class DatitrentinoitProfile(RDFProfile):
    """
    Customization of DCATAP-IT for DatiTrentinoIt
    """

    def parse_dataset(self, dataset_dict, dataset_ref):
        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        g = self.g

        # Replace PoC
        contact_point_raw = dataset_dict.get('contact_point')
        if contact_point_raw:
            try:
                contact_points = json.loads(contact_point_raw)
            except:
                log.error(f"Can't decode contact_point [{contact_points}]")
                return

            if not isinstance(contact_points, list):
                log.warning(f"Can't handle contact_point [{contact_points}]")
                return

            # Remove previous poc
            for s, p, o in g.triples((dataset_ref, DCAT.contactPoint, None)):
                log.info(f"Datitrentinoit Profile: Removing contactPoint {o}")
                g.remove((s, p, o))
                remove_unused_object(g, o, "contactPoint (DatiTrentinoIt)")

            idx = 0
            for contact_point in contact_points:
                # Add new poc
                poc = URIRef(f"{ISPAT_BASE_URL}#contactPoint{idx if idx else ''}")
                idx += 1
                g.add((dataset_ref, DCAT.contactPoint, poc))
                g.add((poc, RDF.type, DCATAPIT.Organization))
                g.add((poc, RDF.type, VCARD.Kind))
                g.add((poc, RDF.type, VCARD.Organization))

                for key, predicate, node_type, logger in (
                        ('contact_point_name', VCARD.fn, Literal, log.warning),
                        ('contact_point_identifier', DCT.identifier, Literal, log.debug),
                        ('contact_point_email', VCARD.hasEmail, URIRef, log.debug),
                ):
                    if key in contact_point:
                        g.add((poc, predicate, node_type(contact_point.get(key))))
                    else:
                        logger(f'Contact point field not found:"{key}"')

                g.add((poc, VCARD.hasURL, URIRef(ISPAT_BASE_URL)))
