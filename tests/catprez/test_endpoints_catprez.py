import os
import shutil
import subprocess
import sys
from pathlib import Path
import pytest
from time import sleep

from rdflib import Graph, URIRef, RDFS, DCTERMS, SKOS

PREZ_DIR = os.getenv("PREZ_DIR")
LOCAL_SPARQL_STORE = os.getenv("LOCAL_SPARQL_STORE")
from fastapi.testclient import TestClient

# https://www.python-httpx.org/advanced/#calling-into-python-web-apps


@pytest.fixture(scope="module")
def cp_test_client(request):
    print("Run Local SPARQL Store")
    p1 = subprocess.Popen(["python", str(LOCAL_SPARQL_STORE), "-p", "3033"])
    sleep(1)

    def teardown():
        print("\nDoing teardown")
        p1.kill()

    request.addfinalizer(teardown)

    # must only import app after config.py has been altered above so config is retained
    from prez.app import app

    return TestClient(app)


@pytest.fixture(scope="module")
def a_catalog_link(cp_test_client):
    with cp_test_client as client:
        # get link for first catalog
        r = client.get("/c/catalogs")
        g = Graph().parse(data=r.text)
        member_uri = g.value(
            URIRef("https://kurrawong.net/prez/memberList"), RDFS.member, None
        )
        link = g.value(member_uri, URIRef(f"https://kurrawong.net/prez/link", None))
        return link


@pytest.fixture(scope="module")
def a_resource_link(cp_test_client, a_catalog_link):
    with cp_test_client as client:
        # get link for a dataset's collections
        r = client.get(f"{a_catalog_link}/collections")
        g = Graph().parse(data=r.text)
        member_uri = g.value(
            URIRef("https://kurrawong.net/prez/memberList"), RDFS.member, None
        )
        link = g.value(member_uri, URIRef(f"https://kurrawong.net/prez/link", None))
        return link
