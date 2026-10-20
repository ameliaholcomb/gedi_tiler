"""Unit tests for converting between DuckDB tables and GeoDataFrames
(ducky.gdf_to_duck and ducky.duck_to_gdf), and the CRS they carry.

Run with:
    conda run -n pyduck python -m pytest gtiler/tests/test_ducky_conversion.py -v
"""

import duckdb
import geopandas as gpd
import pytest
from shapely.geometry import Point, box

from gtiler.database import ducky


@pytest.fixture
def con():
    con = duckdb.connect()
    con.load_extension("spatial")
    return con


def two_geometry_gdf():
    gdf = gpd.GeoDataFrame(
        {"id": [1]}, geometry=[box(-92, 34, -91, 35)], crs="EPSG:4326"
    )
    gdf["centre"] = gpd.GeoSeries([Point(500000, 3800000)], crs="EPSG:32615")
    return gdf


class TestGdfToDuck:
    def test_column_types_carry_the_crs(self, con):
        rel = ducky.gdf_to_duck(con, two_geometry_gdf(), ["geometry", "centre"])
        types = dict(zip(rel.columns, map(str, rel.types)))
        # Lon/lat, which DuckDB calls OGC:CRS84 rather than EPSG:4326.
        assert types["geometry"] == "GEOMETRY('OGC:CRS84')"
        assert types["centre"] == "GEOMETRY('EPSG:32615')"

    def test_no_crs_gives_a_plain_geometry(self, con):
        gdf = gpd.GeoDataFrame(geometry=[Point(1, 2)])
        rel = ducky.gdf_to_duck(con, gdf)
        assert str(rel.types[0]) == "GEOMETRY"


class TestDuckToGdf:
    def test_round_trip_keeps_each_column_crs(self, con):
        gdf = two_geometry_gdf()
        rel = ducky.gdf_to_duck(con, gdf, ["geometry", "centre"])
        back = ducky.duck_to_gdf(rel, ["geometry", "centre"])
        assert back.crs == "EPSG:4326"
        assert back["centre"].crs == "EPSG:32615"
        assert back.geometry[0].equals(gdf.geometry[0])

    def test_plain_geometry_takes_crs_argument(self, con):
        rel = con.sql("SELECT ST_Point(500000, 3800000) AS geometry")
        assert ducky.duck_to_gdf(rel, crs="EPSG:32615").crs == "EPSG:32615"

    def test_crs_argument_by_column(self, con):
        rel = con.sql(
            "SELECT ST_Point(9.4, 0.8) AS geometry, ST_Point(906000, 101000) AS geometry_6933"
        )
        gdf = ducky.duck_to_gdf(
            rel,
            ["geometry", "geometry_6933"],
            crs={"geometry": "EPSG:4326", "geometry_6933": "EPSG:6933"},
        )
        assert gdf.crs == "EPSG:4326"
        assert gdf["geometry_6933"].crs == "EPSG:6933"

    def test_plain_geometry_without_crs_is_rejected(self, con):
        rel = con.sql("SELECT ST_Point(1, 2) AS geometry")
        with pytest.raises(ValueError, match="no CRS"):
            ducky.duck_to_gdf(rel)

    def test_column_crs_wins_over_crs_argument(self, con):
        rel = con.sql("SELECT ST_SetCRS(ST_Point(500000, 3800000), 'EPSG:32615') AS geometry")
        assert ducky.duck_to_gdf(rel, crs="EPSG:4326").crs == "EPSG:32615"
