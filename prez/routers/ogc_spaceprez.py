from typing import Optional

from fastapi import APIRouter, Request, Depends
from rdflib import Namespace
from starlette.responses import PlainTextResponse

from prez.dependencies import get_repo, get_system_repo
from prez.services.curie_functions import get_uri_for_curie_id
from prez.services.listings import listing_function_new
from prez.services.objects import object_function_new
from prez.sparql.methods import Repo

router = APIRouter(tags=["SpacePrez"])

SP_EP = Namespace("https://prez.dev/endpoint/spaceprez/")


@router.get("/s", summary="SpacePrez Home")
async def spaceprez_profiles():
    return PlainTextResponse("SpacePrez Home")


@router.get(
    "/s/catalogs",
    summary="List Datasets",
    name=SP_EP["dataset-listing"],
)
async def list_datasets(
    request: Request,
    repo: Repo = Depends(get_repo),
    system_repo: Repo = Depends(get_system_repo),
    page: Optional[int] = 1,
    per_page: Optional[int] = 20,
):
    endpoint_uri = SP_EP["dataset-listing"]
    return await listing_function_new(
        request=request,
        repo=repo,
        system_repo=system_repo,
        endpoint_uri=endpoint_uri,
        page=page,
        per_page=per_page,
    )


@router.get(
    "/s/catalogs/{dataset_curie}/collections",
    summary="List Feature Collections",
    name=SP_EP["feature-collection-listing"],
)
async def list_feature_collections(
    request: Request,
    dataset_curie: str,
    repo: Repo = Depends(get_repo),
    system_repo: Repo = Depends(get_system_repo),
    page: Optional[int] = 1,
    per_page: Optional[int] = 20,
):
    endpoint_uri = SP_EP["feature-collection-listing"]
    dataset_uri = get_uri_for_curie_id(dataset_curie)
    return await listing_function_new(
        request=request,
        repo=repo,
        system_repo=system_repo,
        endpoint_uri=endpoint_uri,
        page=page,
        per_page=per_page,
        parent_uri=dataset_uri,
    )


@router.get(
    "/s/catalogs/{dataset_curie}/collections/{collection_curie}/items",
    summary="List Features",
    name=SP_EP["feature-listing"],
)
async def list_features(
    request: Request,
    dataset_curie: str,
    collection_curie: str,
    repo: Repo = Depends(get_repo),
    system_repo: Repo = Depends(get_system_repo),
    page: Optional[int] = 1,
    per_page: Optional[int] = 20,
):
    collection_uri = get_uri_for_curie_id(collection_curie)
    endpoint_uri = SP_EP["feature-listing"]
    return await listing_function_new(
        request=request,
        repo=repo,
        system_repo=system_repo,
        endpoint_uri=endpoint_uri,
        page=page,
        per_page=per_page,
        parent_uri=collection_uri,
    )


@router.get(
    "/s/catalogs/{dataset_curie}", summary="Get Dataset", name=SP_EP["dataset-object"]
)
async def dataset_item(
    request: Request,
    dataset_curie: str,
    repo: Repo = Depends(get_repo),
    system_repo: Repo = Depends(get_system_repo),
):
    request_url = request.scope["path"]
    endpoint_uri = SP_EP["dataset-object"]
    dataset_uri = get_uri_for_curie_id(dataset_curie)
    return await object_function_new(
        request=request,
        endpoint_uri=endpoint_uri,
        uri=dataset_uri,
        request_url=request_url,
        repo=repo,
        system_repo=system_repo,
    )


@router.get(
    "/s/catalogs/{dataset_curie}/collections/{collection_curie}",
    summary="Get Feature Collection",
    name=SP_EP["feature-collection-object"],
)
async def feature_collection_item(
    request: Request,
    dataset_curie: str,
    collection_curie: str,
    repo: Repo = Depends(get_repo),
    system_repo: Repo = Depends(get_system_repo),
):
    request_url = request.scope["path"]
    endpoint_uri = SP_EP["feature-collection-object"]
    collection_uri = get_uri_for_curie_id(collection_curie)
    return await object_function_new(
        request=request,
        endpoint_uri=endpoint_uri,
        uri=collection_uri,
        request_url=request_url,
        repo=repo,
        system_repo=system_repo,
    )


@router.get(
    "/s/catalogs/{dataset_curie}/collections/{collection_curie}/items/{feature_curie}",
    summary="Get Feature",
    name="https://prez.dev/endpoint/spaceprez/feature",
)
async def feature_item(
    request: Request,
    dataset_curie: str,
    collection_curie: str,
    feature_curie: str,
    repo: Repo = Depends(get_repo),
    system_repo: Repo = Depends(get_system_repo),
):
    request_url = request.scope["path"]
    endpoint_uri = SP_EP["feature-object"]
    feature_uri = get_uri_for_curie_id(feature_curie)
    return await object_function_new(
        request=request,
        endpoint_uri=endpoint_uri,
        uri=feature_uri,
        request_url=request_url,
        repo=repo,
        system_repo=system_repo,
    )
