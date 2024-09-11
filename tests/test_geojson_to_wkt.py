import pytest

from prez.services.query_generation.cql import get_wkt_from_coords


@pytest.mark.parametrize("geom_type, coordinates, expected_wkt, expected_wkt_alternative", [
    ("Point", [0.123, 0.456], "POINT (0.123 0.456)", None),
    ("Point", [10.123456, -85.123456], "POINT (10.123456 -85.123456)", None),
    ("MultiPoint", [[0.1, 0.1], [1.1, 1.1]], "MULTIPOINT ((0.1 0.1), (1.1 1.1))", "MULTIPOINT (0.1 0.1, 1.1 1.1)"),
    ("MultiPoint", [[10.5, 40.5], [40.25, 30.75], [20.123, 20.456], [30.0001, 10.0001]],
     "MULTIPOINT ((10.5 40.5), (40.25 30.75), (20.123 20.456), (30.0001 10.0001))",
     "MULTIPOINT (10.5 40.5, 40.25 30.75, 20.123 20.456, 30.0001 10.0001)"),
    ("LineString", [[0.0, 0.0], [1.5, 1.5], [2.25, 2.25]], "LINESTRING (0.0 0.0, 1.5 1.5, 2.25 2.25)", "LINESTRING (0.00 0.00, 1.50 1.50, 2.25 2.25)"),
    ("LineString", [[100.123, 0.123], [101.456, 1.456], [102.789, 2.789]],
     "LINESTRING (100.123 0.123, 101.456 1.456, 102.789 2.789)", None),
    ("MultiLineString", [[[0.1, 0.1], [1.1, 1.1]], [[2.2, 2.2], [3.3, 3.3]]],
     "MULTILINESTRING ((0.1 0.1, 1.1 1.1), (2.2 2.2, 3.3 3.3))", None),
    ("MultiLineString", [[[100.001, 0.001], [101.001, 1.001]], [[102.002, 2.002], [103.002, 3.002]]],
     "MULTILINESTRING ((100.001 0.001, 101.001 1.001), (102.002 2.002, 103.002 3.002))", None),
    ("Polygon", [[[100.01, 0.01], [101.02, 0.01], [101.02, 1.02], [100.01, 1.02], [100.01, 0.01]]],
     "POLYGON ((100.01 0.01, 101.02 0.01, 101.02 1.02, 100.01 1.02, 100.01 0.01))", None),
    ("Polygon", [[[35.001, 10.001], [45.002, 45.002], [15.003, 40.003], [10.004, 20.004], [35.001, 10.001]],
                 [[20.005, 30.005], [35.006, 35.006], [30.007, 20.007], [20.005, 30.005]]],
     "POLYGON ((35.001 10.001, 45.002 45.002, 15.003 40.003, 10.004 20.004, 35.001 10.001), (20.005 30.005, 35.006 35.006, 30.007 20.007, 20.005 30.005))", None),
    ("MultiPolygon", [[[[0.1, 0.1], [1.1, 1.1], [1.1, 0.1], [0.1, 0.1]]], [[[2.2, 2.2], [3.3, 3.3], [3.3, 2.2], [2.2, 2.2]]]],
     "MULTIPOLYGON (((0.1 0.1, 1.1 1.1, 1.1 0.1, 0.1 0.1)), ((2.2 2.2, 3.3 3.3, 3.3 2.2, 2.2 2.2)))", None),
    ("MultiPolygon", [[[[102.001, 2.001], [103.001, 2.001], [103.001, 3.001], [102.001, 3.001], [102.001, 2.001]]],
                      [[[100.002, 0.002], [101.002, 0.002], [101.002, 1.002], [100.002, 1.002], [100.002, 0.002]],
                       [[100.503, 0.503], [100.753, 0.503], [100.753, 0.753], [100.503, 0.753], [100.503, 0.503]]]],
     "MULTIPOLYGON (((102.001 2.001, 103.001 2.001, 103.001 3.001, 102.001 3.001, 102.001 2.001)), ((100.002 0.002, 101.002 0.002, 101.002 1.002, 100.002 1.002, 100.002 0.002), (100.503 0.503, 100.753 0.503, 100.753 0.753, 100.503 0.753, 100.503 0.503)))", None),
])
def test_get_wkt_from_coords_valid(geom_type, coordinates, expected_wkt, expected_wkt_alternative):
    assert get_wkt_from_coords(coordinates, geom_type) == expected_wkt or expected_wkt_alternative

# Shapely appears to have a bug with input Polygon formats. The above tests fails ONLY for Polygon for Shapely.
# Geomet works with:
# [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]
# which appears to be as per spec.