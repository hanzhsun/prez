import asyncio
import time
from prez.cache import tbox_cache
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import GEO, DCAT
from typing import List, Optional

from prez.services.sparql_utils import sparql_construct


def generate_listing_construct(
    item_class: URIRef,
    parent_uri: URIRef,
    page: Optional[int] = None,
    per_page: Optional[int] = None,
    profile: dict = {},  # unused - we don't currently filter or otherwise change listings based on profiles
):
    """
    Generates a SPARQL construct query for a listing of items, including labels
    """
    construct_query = f"""PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dcterms: <http://purl.org/dc/terms/>

CONSTRUCT {{ ?item dcterms:identifier ?id ;
                   rdfs:label ?label ; }}
WHERE {{ \
{chr(10) + chr(9) + f'<{parent_uri}> rdfs:member ?item .' if parent_uri else ""}
    ?item a <{item_class}> ;
          dcterms:identifier ?id ;
          rdfs:label|dcterms:title|skos:prefLabel ?label .
  	FILTER(DATATYPE(?id) = xsd:token)
    }} {f"LIMIT {per_page} OFFSET {(page - 1) * per_page}" if page is not None and per_page is not None else ""}
    """
    return construct_query


def generate_item_construct(object_uri: URIRef, profile: dict):

    include_predicates = []  # TODO from profile
    exclude_predicates = []  # TODO from profile
    bnode_depth = profile.get("bnode_depth", 2)  # TODO from profile
    construct_query = f"""PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n
CONSTRUCT {{
    <{object_uri}> ?p ?o1 .
{generate_bnode_construct(bnode_depth)}
  }}
WHERE {{
    <{object_uri}> !rdfs:member ?o1 ;
        ?p ?o1 . \n
{generate_include_predicates(include_predicates)} \
{generate_bnode_select(bnode_depth)}
}}
"""
    return construct_query


def generate_include_predicates(include_predicates):
    """
    Generates a SPARQL VALUES clause for a list of predicates, of the form:
    VALUES ?p { <http://example1.com> <http://example2.com> }
    """
    if include_predicates:
        return f"""VALUES ?p{{\n{chr(10).join([f"<{p}>" for p in include_predicates])}\n}}"""
    else:
        return ""


def generate_bnode_construct(depth):
    """
    Generate the construct query for the bnodes, this is of the form:
    ?o1 ?p2 ?o2 .
        ?o2 ?p3 ?o3 .
        ...
    """
    return "\n".join([f"\t?o{i + 1} ?p{i + 2} ?o{i + 2} ." for i in range(depth)])


def generate_bnode_select(depth):
    """
    Generates a SPARQL select string for bnodes to a given depth, of the form:
    OPTIONAL {
        FILTER(ISBLANK(?o1))
        ?o1 ?p2 ?o2 ;
        OPTIONAL {
            FILTER(ISBLANK(?o2))
            ?o2 ?p3 ?o3 ;
            OPTIONAL { ...
                }
            }
        }
    """
    part_one = "\n".join(
        [
            f"""{chr(9) * (i + 1)}OPTIONAL {{
{chr(9) * (i + 2)}FILTER(ISBLANK(?o{i + 1}))
{chr(9) * (i + 2)}?o{i + 1} ?p{i + 2} ?o{i + 2} ."""
            for i in range(depth)
        ]
    )
    part_two = "".join(
        [f"{chr(10)}{chr(9) * (i + 1)}}}" for i in reversed(range(depth))]
    )
    return part_one + part_two


async def get_labels(
    object_graph: Graph,
    label_property: URIRef = URIRef("http://www.w3.org/2000/01/rdf-schema#label"),
):
    terms = set(i for i in object_graph.predicates() if isinstance(i, URIRef)) | set(
        i for i in object_graph.objects() if isinstance(i, URIRef)
    )
    # read labels from the tbox cache, this should be the majority of labels
    uncached_terms, labels_g = get_labels_from_tbox_cache(terms)
    # read remaining labels from the SPARQL endpoint
    queries = [
        f"CONSTRUCT {{ <{term}> <{label_property}> ?label }} WHERE {{ <{term}> <{label_property}> ?label }}"
        for term in uncached_terms
    ]
    # untested assumption is running multiple queries in parallel is faster than running one query for all labels
    results = await asyncio.gather(
        *[sparql_construct(query, "SpacePrez") for query in queries]
    )
    for r in results:
        if r[0]:
            labels_g += r[1]
    return labels_g


def get_labels_from_tbox_cache(terms: List[URIRef]):
    """
    Gets labels from the TBox cache, returns a list of terms that were not found in the cache, and a graph of labels
    """
    uncached_terms = []
    labels_from_cache = Graph()
    cache_keys = tbox_cache.keys()
    for term in terms:
        if term in cache_keys:
            labels_from_cache.add(
                (
                    term,
                    URIRef("http://www.w3.org/2000/01/rdf-schema#label"),
                    tbox_cache[term],
                )
            )
        else:
            uncached_terms.append(term)
    return uncached_terms, labels_from_cache
