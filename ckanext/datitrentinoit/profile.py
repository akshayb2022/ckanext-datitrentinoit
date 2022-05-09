  
import logging

from rdflib.namespace import Namespace, RDF, SKOS
from rdflib import BNode, Literal

from ckanext.dcat.profiles import RDFProfile, DCAT, VCARD

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
        
        # Add new poc
        contact_point = dataset_dict.get('contact_point')

        poc = BNode()
        g.add((dataset_ref, DCAT.contactPoint, poc))
        g.add((poc, RDF.type, DCATAPIT.Organization))
        g.add((poc, RDF.type, VCARD.Kind))
        g.add((poc, RDF.type, VCARD.Organization))

        g.add((poc, VCARD.fn, Literal(contact_point)))
