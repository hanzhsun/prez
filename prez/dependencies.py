import json
import urllib
from pathlib import Path
from typing import Optional

from pydantic import BaseModel
import httpx
from fastapi import Depends, Request, HTTPException
from pyoxigraph import Store

from prez.cache import store, oxrdflib_store
from prez.config import settings
from prez.sparql.methods import PyoxigraphRepo, RemoteSparqlRepo, OxrdflibRepo
from temp.cql2sparql import CQLParser


async def get_async_http_client():
    return httpx.AsyncClient(
        auth=(settings.sparql_username, settings.sparql_password)
        if settings.sparql_username
        else None,
        timeout=settings.sparql_timeout,
    )


def get_pyoxi_store():
    return store


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


async def load_local_data_to_oxigraph(store: Store):
    """
    Loads all the data from the local data directory into the local SPARQL endpoint
    """
    for file in (Path(__file__).parent.parent / "rdf").glob("*.ttl"):
        store.load(file.read_bytes(), "text/turtle")


class CQLRequest(BaseModel):
    cql: Optional[dict]


async def cql_parser_dependency(request: Request):
    try:
        body = await request.json()
        context = json.load(
            (Path(__file__).parent.parent / "temp" / "default_cql_context.json").open()
        )
        cql_parser = CQLParser(cql=body, context=context)
        cql_parser.generate_jsonld()
        return cql_parser.cql_json
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format.")
    except Exception as e:  # Replace with your specific parsing exception
        raise HTTPException(
            status_code=400, detail="Invalid CQL format: Parsing failed."
        )
