import json
from pathlib import Path

import httpx
from fastapi import Depends, Request, HTTPException
from pyoxigraph import Store
from rdflib import Dataset

from prez.cache import (
    store,
    oxrdflib_store,
    system_store,
    profiles_graph_cache,
    endpoints_graph_cache,
    annotations_store,
    annotations_repo
)
from prez.config import settings
from prez.repositories import PyoxigraphRepo, RemoteSparqlRepo, OxrdflibRepo
from prez.services.query_generation.node_selection.cql import CQLParser


async def get_async_http_client():
    return httpx.AsyncClient(
        auth=(settings.sparql_username, settings.sparql_password)
        if settings.sparql_username
        else None,
        timeout=settings.sparql_timeout,
    )


def get_pyoxi_store():
    return store


def get_system_store():
    return system_store


def get_annotations_store():
    return annotations_store


def get_oxrdflib_store():
    return oxrdflib_store


async def get_repo(
    http_async_client: httpx.AsyncClient = Depends(get_async_http_client),
    pyoxi_store: Store = Depends(get_pyoxi_store),
):
    if settings.sparql_repo_type == "pyoxigraph":
        return PyoxigraphRepo(pyoxi_store)
    elif settings.sparql_repo_type == "oxrdflib":
        return OxrdflibRepo(oxrdflib_store)
    elif settings.sparql_repo_type == "remote":
        return RemoteSparqlRepo(http_async_client)


async def get_system_repo(
    pyoxi_store: Store = Depends(get_system_store),
):
    """
    A pyoxigraph Store with Prez system data including:
    - Profiles
    # TODO add and test other system data (endpoints etc.)
    """
    return PyoxigraphRepo(pyoxi_store)


async def get_annotations_repo():
    """
    A pyoxigraph Store with labels, descriptions etc. from Context Ontologies
    """
    return annotations_repo



async def load_local_data_to_oxigraph(store: Store):
    """
    Loads all the data from the local data directory into the local SPARQL endpoint
    """
    for file in (Path(__file__).parent.parent / settings.local_rdf_dir).glob("*.ttl"):
        store.load(file.read_bytes(), "text/turtle")


async def load_system_data_to_oxigraph(store: Store):
    """
    Loads all the data from the local data directory into the local SPARQL endpoint
    """
    # TODO refactor to use the local files directly
    profiles_bytes = profiles_graph_cache.serialize(format="nt", encoding="utf-8")
    store.load(profiles_bytes, "application/n-triples")

    endpoints_bytes = endpoints_graph_cache.serialize(format="nt", encoding="utf-8")
    store.load(endpoints_bytes, "application/n-triples")


async def load_annotations_data_to_oxigraph(store: Store):
    """
    Loads all the data from the local data directory into the local SPARQL endpoint
    """
    relevant_predicates = settings.label_predicates + settings.description_predicates + settings.provenance_predicates
    raw_g = Dataset(default_union=True)
    for file in (Path(__file__).parent / "reference_data/context_ontologies").glob("*"):
        raw_g.parse(file)
    relevant_g = Dataset(default_union=True)
    relevant_triples = raw_g.triples_choices((None, relevant_predicates, None))
    for triple in relevant_triples:
        relevant_g.add(triple)
    file_bytes = relevant_g.serialize(format="nt", encoding="utf-8")
    store.load(file_bytes, "application/n-triples")


async def cql_post_parser_dependency(request: Request):
    try:
        body = await request.json()
        context = json.load(
            (Path(__file__).parent.parent / "temp" / "default_cql_context.json").open()
        )
        cql_parser = CQLParser(cql=body, context=context)
        cql_parser.generate_jsonld()
        return cql_parser
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format.")
    except Exception as e:  # Replace with your specific parsing exception
        raise HTTPException(
            status_code=400, detail="Invalid CQL format: Parsing failed."
        )


async def cql_get_parser_dependency(request: Request):
    if request.query_params.get("filter"):
        try:
            query = json.loads(request.query_params["filter"])
            context = json.load(
                (
                    Path(__file__).parent / "reference_data/cql/default_context.json"
                ).open()
            )
            cql_parser = CQLParser(cql=query, context=context)
            cql_parser.generate_jsonld()
            return cql_parser
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON format.")
        except Exception as e:  # Replace with your specific parsing exception
            raise HTTPException(
                status_code=400, detail="Invalid CQL format: Parsing failed."
            )
