import logging
from pathlib import Path as PLPath

from fastapi import APIRouter, Depends, Body
from fastapi import HTTPException
from fastapi import Path as FAPath
from pyoxigraph import NamedNode
from rdflib import Graph
from rdflib.exceptions import ParserError

from prez.cache import profiles_graph_cache
from prez.dependencies import get_system_repo
from prez.repositories import Repo
from prez.services.curie_functions import get_uri_for_curie_id

router = APIRouter(tags=["Configuration"])
log = logging.getLogger(__name__)

# Read the example RDF data from a file
example_profile = (PLPath(__file__).parent / "example_profile.ttl").read_text()


@router.put(
    "/update-profile/{profile_name}", summary="Update Profile", tags=["Configuration"]
)
async def update_profile(
    profile_name: str = FAPath(
        ...,
        title="Profile Name",
        description="The name of the profile to update",
        example="prez:ExProf",
    ),
    profile_update: str = Body(
        ...,
        example=example_profile,
        media_type="text/turtle",
    ),
    system_repo: Repo = Depends(get_system_repo),
):
    profile_uri = await get_uri_for_curie_id(profile_name)
    try:
        new_profile_g = Graph().parse(data=profile_update, format="turtle")
    except ParserError as e:
        raise HTTPException(status_code=400, detail=f"Error parsing profile: {e}")
    try:
        old_profile = profiles_graph_cache.cbd(profile_uri)
    except KeyError:
        raise HTTPException(
            status_code=404, detail=f"Profile {profile_name} not found."
        )
    for t in old_profile:
        profiles_graph_cache.remove(t)
    try:
        # system_repo.pyoxi_store.update(f"DELETE DATA {{ {" ".join([i.n3() for i in t])} }}")
        system_repo.pyoxi_store.remove_graph(NamedNode(str(profile_uri)))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating profile: {e}")
    for t in new_profile_g:
        profiles_graph_cache.add(t)
    new_prof_bytes = new_profile_g.serialize(format="nt", encoding="utf-8")
    system_repo.pyoxi_store.load(
        new_prof_bytes, "application/n-triples", to_graph=NamedNode(str(profile_uri))
    )
    log.info(f"Profile {profile_name} updated.")
    return {"message": f"Profile {profile_name} updated."}
