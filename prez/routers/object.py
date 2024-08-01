from fastapi import APIRouter, Request, HTTPException, status, Query
from fastapi import Depends
from starlette.responses import PlainTextResponse

from prez.dependencies import get_repo, get_system_repo
from prez.queries.object import object_inbound_query, object_outbound_query
from prez.routers.identifier import get_iri_route
from prez.services.objects import object_function

router = APIRouter(tags=["Object"])


@router.get(
    "/count", summary="Get object's statement count", response_class=PlainTextResponse
)
async def count_route(
    curie: str,
    inbound: str = Query(
        None,
        examples={
            "skos:inScheme": {
                "summary": "skos:inScheme",
                "value": "http://www.w3.org/2004/02/skos/core#inScheme",
            },
            "skos:topConceptOf": {
                "summary": "skos:topConceptOf",
                "value": "http://www.w3.org/2004/02/skos/core#topConceptOf",
            },
            "empty": {"summary": "Empty", "value": None},
        },
    ),
    outbound: str = Query(
        None,
        examples={
            "empty": {"summary": "Empty", "value": None},
            "skos:hasTopConcept": {
                "summary": "skos:hasTopConcept",
                "value": "http://www.w3.org/2004/02/skos/core#hasTopConcept",
            },
        },
    ),
    repo=Depends(get_repo),
):
    """Get an Object's statements count based on the inbound or outbound predicate"""
    iri = get_iri_route(curie)

    if inbound is None and outbound is None:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "At least 'inbound' or 'outbound' is supplied a valid IRI.",
        )

    if inbound and outbound:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "Only provide one value for either 'inbound' or 'outbound', not both.",
        )

    if inbound:
        query = object_inbound_query(iri, inbound)
        _, rows = await repo.send_queries([], [(None, query)])
        for row in rows[0][1]:
            return row["count"]["value"]

    query = object_outbound_query(iri, outbound)
    _, rows = await repo.send_queries([], [(None, query)])
    for row in rows[0][1]:
        return row["count"]["value"]


@router.get("/object", summary="Object", name="https://prez.dev/endpoint/object")
async def object_route(
    request: Request, repo=Depends(get_repo), system_repo=Depends(get_system_repo)
):
    return await object_function(request, repo=repo, system_repo=system_repo)
