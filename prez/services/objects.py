import io
import json
import logging
import time
from urllib.parse import urlencode

from fastapi.responses import PlainTextResponse
from rdf2geojson import convert
from rdflib import RDF, URIRef
from sparql_grammar_pydantic import TriplesSameSubject, IRI, Var, TriplesSameSubjectPath

from prez.config import settings
from prez.models.ogc_features import Link, Collection
from prez.models.query_params import QueryParams
from prez.reference_data.prez_ns import ALTREXT, ONT, PREZ
from prez.renderers.renderer import return_from_graph, return_annotated_rdf
from prez.services.connegp_service import RDF_MEDIATYPES
from prez.services.curie_functions import get_uri_for_curie_id, get_curie_id_for_uri
from prez.services.link_generation import add_prez_links
from prez.services.listings import listing_function, generate_link_headers
from prez.services.query_generation.umbrella import (
    PrezQueryConstructor,
)

log = logging.getLogger(__name__)


async def object_function(
    data_repo,
    system_repo,
    endpoint_structure,
    pmts,
    profile_nodeshape,
):
    if pmts.selected["profile"] == ALTREXT["alt-profile"]:
        none_keys = [
            "endpoint_nodeshape",
            "concept_hierarchy_query",
            "cql_parser",
            "search_query",
        ]
        none_kwargs = {key: None for key in none_keys}
        query_params = QueryParams(
            mediatype=pmts.selected["mediatype"],
            filter=None,
            q=None,
            page=1,
            limit=100,
            datetime=None,
            bbox=[],
            filter_crs=None,
            filter_lang=None,
            order_by=None,
            order_by_direction=None,
        )
        return await listing_function(
            data_repo=data_repo,
            system_repo=system_repo,
            endpoint_structure=endpoint_structure,
            pmts=pmts,
            profile_nodeshape=profile_nodeshape,
            query_params=query_params,
            original_endpoint_type=ONT["ObjectEndpoint"],
            **none_kwargs,
        )
    if "anot+" in pmts.selected["mediatype"]:
        profile_nodeshape.tss_list.append(
            TriplesSameSubject.from_spo(
                subject=profile_nodeshape.focus_node,
                predicate=IRI(value="https://prez.dev/type"),
                object=IRI(value="https://prez.dev/FocusNode"),
            )
        )
    query = PrezQueryConstructor(
        profile_triples=profile_nodeshape.tssp_list,
        profile_gpnt=profile_nodeshape.gpnt_list,
        construct_tss_list=profile_nodeshape.tss_list,
    ).to_string()

    if pmts.requested_mediatypes[0][0] == "application/sparql-query":
        return PlainTextResponse(query, media_type="application/sparql-query")
    query_start_time = time.time()
    item_graph, _ = await data_repo.send_queries([query], [])
    log.debug(f"Query time: {time.time() - query_start_time}")
    if "anot+" in pmts.selected["mediatype"]:
        await add_prez_links(item_graph, data_repo, endpoint_structure)
    return await return_from_graph(
        item_graph,
        pmts.selected["mediatype"],
        pmts.selected["profile"],
        pmts.generate_response_headers(),
        pmts.selected["class"],
        data_repo,
        system_repo,
    )


async def ogc_features_object_function(
    template_query,
    selected_mediatype,
    url,
    data_repo,
    system_repo,
    **path_params,
):
    collectionId = path_params.get("collectionId")
    featureId = path_params.get("featureId")
    collection_uri = await get_uri_for_curie_id(collectionId)
    if template_query:
        if featureId:
            focus_uri = await get_uri_for_curie_id(featureId)
        else:
            focus_uri = collection_uri
        query = template_query.replace(
            "VALUES ?focusNode { UNDEF }", f"VALUES ?focusNode {{ {focus_uri.n3()} }}"
        )
    else:
        if featureId is None:  # feature collection
            collection_iri = IRI(value=collection_uri)
            tssp_list = [
                TriplesSameSubjectPath.from_spo(
                    collection_iri, IRI(value=RDF.type), Var(value="type")
                )
            ]
        else:  # feature
            feature_uri = await get_uri_for_curie_id(featureId)
            feature_iri = IRI(value=feature_uri)
            tssp_list = [
                TriplesSameSubjectPath.from_spo(
                    feature_iri, IRI(value=RDF.type), Var(value="type")
                )
            ]
        query = PrezQueryConstructor(
            profile_triples=tssp_list,
        ).to_string()

    query_start_time = time.time()
    item_graph, _ = await data_repo.send_queries([query], [])
    annotations_graph = await return_annotated_rdf(item_graph, data_repo, system_repo)
    log.debug(f"Query time: {time.time() - query_start_time}")

    link_headers = None
    if selected_mediatype == "application/sparql-query":
        content = io.BytesIO(query.encode("utf-8"))
    elif selected_mediatype == "application/json":
        collection = create_collection_json(
            collectionId, collection_uri, annotations_graph, url
        )
        link_headers = generate_link_headers(collection.links)
        content = io.BytesIO(
            collection.model_dump_json(exclude_none=True).encode("utf-8")
        )
    elif selected_mediatype == "application/geo+json":
        geojson = convert(g=item_graph, do_validate=False, iri2id=get_curie_id_for_uri)
        content = io.BytesIO(json.dumps(geojson).encode("utf-8"))
    else:
        content = io.BytesIO(
            item_graph.serialize(format=selected_mediatype, encoding="utf-8")
        )
    return content, link_headers


def create_collection_json(collection_curie, collection_uri, annotations_graph, url):
    return Collection(
        id=collection_curie,
        title=annotations_graph.value(
            subject=collection_uri, predicate=PREZ.label, default=None
        ),
        description=annotations_graph.value(
            subject=collection_uri, predicate=PREZ.description, default=None
        ),
        links=[
            Link(
                href=URIRef(
                    f"{settings.system_uri}{url.path}/items?{urlencode({'_mediatype': mt})}"
                ),
                rel="items",
                type=mt,
            )
            for mt in ["application/geo+json", *RDF_MEDIATYPES]
        ],
    )
