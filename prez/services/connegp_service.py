import logging
import re
from enum import Enum
from textwrap import dedent
from typing import List, Dict
from urllib.parse import urlencode

from pydantic import BaseModel
from rdflib import Graph, Namespace, URIRef

from prez.config import settings
from prez.exceptions.model_exceptions import NoProfilesException
from prez.models.ogc_features import Link
from prez.repositories.base import Repo
from prez.services.curie_functions import get_curie_id_for_uri, get_uri_for_curie_id

log = logging.getLogger(__name__)

RDF_MEDIATYPES = [
    "text/turtle",
    "application/rdf+xml",
    "application/ld+json",
    "application/n-triples",
]

RDF_SERIALIZER_TYPES_MAP = {
    "text/turtle": "turtle",
    "text/n3": "n3",
    "application/n-triples": "nt",
    "application/ld+json": "json-ld",
    "application/rdf+xml": "xml",
    # Some common but incorrect mimetypes
    "application/rdf": "xml",
    "application/rdf xml": "xml",
    "application/json": "json-ld",
    "application/ld json": "json-ld",
    "text/ttl": "turtle",
    "text/ntriples": "nt",
    "text/n-triples": "nt",
    "text/plain": "nt",  # text/plain is the old/deprecated mimetype for n-triples
}


class MediaType(str, Enum):
    turtle = "text/turtle"
    n3 = "text/n3"
    nt = "application/n-triples"
    json_ld = "application/ld+json"
    xml = "application/rdf+xml"
    anot_turtle = "text/anot+turtle"
    anot_n3 = "text/anot+n3"
    anot_nt = "application/anot+n-triples"
    anot_json_ld = "application/anot+ld+json"
    anot_xml = "application/anot+rdf+xml"
    # Some common but incorrect mimetypes
    application_rdf = "application/rdf"
    application_rdf_xml = "application/rdf xml"
    application_json = "application/json"
    application_ld_json = "application/ld json"
    text_ttl = "text/ttl"
    text_ntriples = "text/ntriples"
    text_plain = "text/plain"


class TokenError(Exception):
    def __init__(self, *args):
        super().__init__(*args)


class NegotiatedPMTs(BaseModel):
    """The Requested Profiles and Media Types as negotiated by the ConnegP standard.
    See: https://w3c.github.io/dx-connegp/connegp/#introduction

    Exposes the selected profile / media type as self.selected: dict
    with keys:
        - profile: URIRef
        - title: str
        - mediatype: str
        - class: str

    Response headers with alternate profiles / mediatypes can be generated by calling
    the .generate_response_headers() method.
    """

    headers: dict
    params: dict
    classes: list[URIRef]
    system_repo: Repo
    listing: bool = False
    default_weighting: float = 1.0
    requested_profiles: list[tuple[str, float]] | None = None
    requested_mediatypes: list[tuple[str, float]] | None = None
    available: list[dict] | None = None
    selected: dict | None = None
    current_path: str | None = None

    class Config:
        arbitrary_types_allowed = True

    async def setup(self):
        self.requested_profiles = await self._get_requested_profiles()
        self.requested_mediatypes = await self._get_requested_mediatypes()
        self.available = await self._get_available()
        self.selected = self.available[0]

    async def _resolve_token(self, token: str) -> str:
        query_str: str = dedent(
            """
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX prof: <http://www.w3.org/ns/dx/prof/>
    
        SELECT ?profile
        WHERE {
            ?profile a prof:Profile .
            ?profile dcterms:identifier ?o .
            FILTER(?o="<token>"^^xsd:token)
        }
        """.replace(
                "<token>", token
            )
        )
        try:
            _, results = await self.system_repo.send_queries([], [(None, query_str)])
            result: str = results[0][1][0]["profile"]["value"]
        except (KeyError, IndexError, ValueError):
            raise TokenError(f"Token: '{token}' could not be resolved to URI")
        uri = "<" + result + ">"
        return uri

    async def _tupilize(
            self, string: str, is_profile: bool = False
    ) -> tuple[str, float]:
        parts: list[str | float] = string.split("q=")  # split out the weighting
        parts[0] = parts[0].strip(
            " ;"
        )  # remove the seperator character, and any whitespace characters
        if is_profile and not re.search(
                r"^<.*>$", parts[0]
        ):  # If it doesn't look like a URI ...
            try:
                parts[0] = await self._resolve_token(
                    parts[0]
                )  # then try to resolve the token to a URI
            except TokenError as e:
                log.error(e.args[0])
                try:  # if token resolution fails, try to resolve as a curie
                    result = await get_uri_for_curie_id(parts[0])
                    result = str(result)
                    parts[0] = "<" + result + ">"
                except ValueError as e:
                    parts[0] = (
                        ""  # if curie resolution failed, then the profile is invalid
                    )
                    log.error(e.args[0])
        if len(parts) == 1:
            parts.append(self.default_weighting)  # If no weight given, set the default
        else:
            try:
                parts[1] = float(parts[1])  # Type-check the seperated weighting
            except ValueError as e:
                log.debug(
                    f"Could not cast q={parts[1]} as float. Defaulting to {self.default_weighting}. {e.args[0]}"
                )
        return parts[0], parts[1]

    @staticmethod
    def _prioritize(types: list[tuple[str, float]]) -> list[tuple[str, float]]:
        return sorted(types, key=lambda x: x[1], reverse=True)

    async def _get_requested_profiles(self) -> list[tuple[str, float]] | None:
        raw_profiles: str = self.params.get(
            "_profile", ""
        )  # Prefer profiles declared in the QSA, as per the spec.
        if not raw_profiles:
            raw_profiles: str = self.headers.get("accept-profile", "")
        if raw_profiles:
            profiles: list = [
                await self._tupilize(profile, is_profile=True)
                for profile in raw_profiles.split(",")
            ]
            return self._prioritize(profiles)
        return None

    async def _get_requested_mediatypes(self) -> list[tuple[str, float]] | None:
        raw_mediatypes: str = self.params.get(
            "_mediatype", ""
        )  # Prefer mediatypes declared in the QSA, as per the spec.
        if not raw_mediatypes:
            raw_mediatypes: str = self.headers.get("accept", "")
        if raw_mediatypes:
            mediatypes: list = [
                await self._tupilize(mediatype)
                for mediatype in raw_mediatypes.split(",")
            ]
            return self._prioritize(mediatypes)
        return None

    async def _get_available(self) -> list[dict]:
        query = self._compose_select_query()
        repo_response = await self._do_query(query)
        available = [
            {
                "profile": URIRef(result["profile"]["value"]),
                "title": result["title"]["value"],
                "mediatype": result["format"]["value"],
                "class": result["class"]["value"],
            }
            for result in repo_response[1][0][1]
        ]
        if not available:
            raise NoProfilesException(self.classes)
        return available

    def generate_response_headers(self) -> dict:
        profile_uri = "<http://www.w3.org/ns/dx/prof/Profile>"
        distinct_profiles = {(pmt["profile"], pmt["title"]) for pmt in self.available}
        profile_header_links = ", ".join(
            [f'<{self.selected["profile"]}>; rel="profile"']
            + [
                f'{profile_uri}; rel="type"; title="{pmt[1]}"; token="{get_curie_id_for_uri(pmt[0])}"; anchor="{pmt[0]}"'
                for pmt in distinct_profiles
            ]
        )
        mediatype_header_links = ", ".join(
            [
                f'<{settings.system_uri}{self.current_path}?_profile={get_curie_id_for_uri(pmt["profile"])}&_mediatype={pmt["mediatype"]}>; rel="{"self" if pmt == self.selected else "alternate"}"; type="{pmt["mediatype"]}"; format="{pmt["profile"]}"'
                for pmt in self.available
            ]
        )
        headers = {
            "Content-Type": self.selected["mediatype"],
            "link": profile_header_links + ", " + mediatype_header_links,
        }
        return headers

    def _compose_select_query(self) -> str:
        prez = Namespace("https://prez.dev/")
        profile_class = prez.ListingProfile if self.listing else prez.ObjectProfile
        if self.requested_profiles:
            requested_profile = self.requested_profiles[0][0]
        else:
            requested_profile = None

        query = dedent(
            f"""
            PREFIX altr-ext: <http://www.w3.org/ns/dx/connegp/altr-ext#>
            PREFIX dcat: <http://www.w3.org/ns/dcat#>
            PREFIX dcterms: <http://purl.org/dc/terms/>
            PREFIX geo: <http://www.opengis.net/ont/geosparql#>
            PREFIX prez: <https://prez.dev/>
            PREFIX prof: <http://www.w3.org/ns/dx/prof/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX sh: <http://www.w3.org/ns/shacl#>
        
            SELECT ?profile ?title ?class (count(?mid) as ?distance) ?req_profile ?def_profile ?format ?req_format ?def_format
        
            WHERE {{
              VALUES ?class {{{" ".join('<' + str(klass) + '>' for klass in self.classes)}}}
              ?class rdfs:subClassOf* ?mid .
              ?mid rdfs:subClassOf* ?base_class .
              VALUES ?base_class {{ dcat:Dataset geo:FeatureCollection geo:Feature
              skos:ConceptScheme skos:Concept skos:Collection 
              dcat:Catalog rdf:Resource dcat:Resource prof:Profile prez:SPARQLQuery 
              prez:SearchResult prez:CQLObjectList prez:QueryablesList prez:Object rdfs:Resource }}
              ?profile altr-ext:constrainsClass ?class ;
                       altr-ext:hasResourceFormat ?format ;
                       dcterms:title ?title .\
              {f'?profile a {profile_class.n3()} .'}
              {f'BIND(?profile={requested_profile} as ?req_profile)' if requested_profile else ''}
              BIND(EXISTS {{ ?shape sh:targetClass ?class ;
                                   altr-ext:hasDefaultProfile ?profile }} AS ?def_profile)
              {self._generate_mediatype_if_statements()}
              BIND(EXISTS {{ ?profile altr-ext:hasDefaultResourceFormat ?format }} AS ?def_format)
            }}
            GROUP BY ?class ?profile ?req_profile ?def_profile ?format ?req_format ?def_format ?title
            ORDER BY DESC(?req_profile) DESC(?distance) DESC(?def_profile) DESC(?req_format) DESC(?def_format)
            """
        )
        return query

    def _generate_mediatype_if_statements(self) -> str:
        """
        Generates a list of if statements used to determine the response mediatype based on user requests,
        and the availability of these in profiles.
        These are of the form:
          BIND(
            IF(?format="application/ld+json", "0.9",
              IF(?format="text/html", "0.8",
                IF(?format="image/apng", "0.7", ""))) AS ?req_format)
        """
        if not self.requested_mediatypes:
            return ""
        line_join = "," + "\n"
        ifs = (
            f"BIND(\n"
            f"""{line_join.join(
                {chr(9) + 'IF(?format="' + tup[0] + '", "' + str(tup[1]) + '"' for tup in self.requested_mediatypes}
            )}"""
            f""", ""{')' * len(self.requested_mediatypes)}\n"""
            f"\tAS ?req_format)"
        )
        return ifs

    async def _do_query(self, query: str) -> tuple[Graph, list]:
        response = await self.system_repo.send_queries([], [(None, query)])
        if not response[1][0][1]:
            raise NoProfilesException(self.classes)

        if settings.log_level == "DEBUG":
            from tabulate import tabulate
            table_data = [
                [
                    item['profile']['value'],
                    item['title']['value'],
                    item['class']['value'],
                    item['distance']['value'],
                    item['def_profile']['value'],
                    item['format']['value'],
                    item['req_format']['value'],
                    item['def_format']['value'],
                ]
                for item in response[1][0][1]
            ]

            # Define headers
            headers = ["Profile", "Title", "Class", "Distance", "Default Profile", "Format", "Requested Format",
                       "Default Format"]

            # Render as a table
            log.debug(tabulate(table_data, headers=headers, tablefmt="grid"))

        return response


def generate_ogc_features_links(url_path: str, selected_mediatype: str) -> List[Link]:
    components_after_collections = url_path.split('collections')[1:]
    components_len = len(components_after_collections)

    if components_len == 1:  # collections or a specific collection - links are the same
        self_link = Link(
            rel="self",
            href=f"{settings.system_uri}{url_path}?{urlencode({'_mediatype': selected_mediatype})}",
            type="application/json"
        )

        alt_links = [
            Link(
                rel="alternate",
                href=f"{settings.system_uri}{url_path}?{urlencode({'_mediatype': mediatype})}",
                type=mediatype
            )
            for mediatype in RDF_MEDIATYPES
            if mediatype != selected_mediatype
        ]
        return [self_link] + alt_links
    return []


def generate_link_headers(links) -> Dict[str, str]:
    link_header = ", ".join([f'<{link.href}>; rel="{link.rel}"; type="{link.type}"' for link in links])
    return {"Link": link_header}
