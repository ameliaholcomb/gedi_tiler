"""Unit tests for the tiles covering a region: tiles.get_covering_tile_ids
and the ducky helpers built on it.

Run with:
    conda run -n pyduck python -m pytest gtiler/tests/test_region_tiles.py -v
"""

import math

import duckdb
import geopandas as gpd
import numpy as np
import pytest
import shapely
from shapely.geometry import LineString, Point, box

from gtiler.database import ducky, tiles

PLOTS_TILES = ["N35_W092", "N35_W093"]
FIJI_TILES = sorted(
    f"S{lat}_{lon}" for lat in (16, 17, 18) for lon in ("E177", "E178", "E179", "W180")
)


def region(geoms, crs, from_crs=None):
    """A GeoDataFrame of `geoms` in `crs`, given in `from_crs` if set."""
    series = gpd.GeoSeries(geoms, crs=from_crs or crs)
    return gpd.GeoDataFrame(geometry=series.to_crs(crs) if from_crs else series)


def lonlat_box(*bounds):
    """A lon/lat box densified so it keeps its shape when reprojected."""
    return shapely.segmentize(box(*bounds), 0.01)


def brute_force_tiles(gdf, step=0.02, margin=3):
    """Tiles holding a point of a lon/lat grid that falls in the region."""
    shape = gdf.union_all()
    lon0, lat0, lon1, lat1 = gdf.to_crs("EPSG:4326").total_bounds
    lons = np.arange(math.floor(lon0) - margin, math.ceil(lon1) + margin, step) + step / 2
    lats = np.arange(math.floor(lat0) - margin, math.ceil(lat1) + margin, step) + step / 2
    lons, lats = [a.ravel() for a in np.meshgrid(lons, lats)]
    points = gpd.GeoSeries(gpd.points_from_xy(lons, lats), crs="EPSG:4326").to_crs(gdf.crs)
    inside = shapely.intersects(shape, points.values)
    return sorted(
        {tiles._to_text(math.floor(x), math.ceil(y)) for x, y in zip(lons[inside], lats[inside])}
    )


class TestGetCoveringTileIds:
    @pytest.mark.parametrize("crs", ["EPSG:4326", "EPSG:3857", "EPSG:32615"])
    def test_plots_in_any_crs(self, crs):
        gdf = region([lonlat_box(-92.6, 34.2, -91.6, 34.6)], crs, "EPSG:4326")
        assert sorted(tiles.get_covering_tile_ids(gdf)) == PLOTS_TILES

    def test_point(self):
        gdf = region([Point(-92.5, 34.3)], "EPSG:32615", "EPSG:4326")
        assert tiles.get_covering_tile_ids(gdf) == ["N35_W093"]

    def test_line_crossing_tiles(self):
        # The tiles of points every 5 m along the line.
        gdf = region([LineString([(300000, 3.7e6), (700000, 4.1e6)])], "EPSG:32615")
        assert sorted(tiles.get_covering_tile_ids(gdf)) == [
            "N34_W095", "N34_W096", "N35_W094", "N35_W095", "N36_W093",
            "N36_W094", "N37_W091", "N37_W092", "N37_W093", "N38_W091",
        ]

    def test_strip_just_inside_a_tile_edge(self):
        # Lat 35 is the edge between N35 (34..35) and N36 (35..36); the strip
        # is 10-30 m south of it.
        gdf = region([box(-91.9, 34.9997, -91.1, 34.9999)], "EPSG:4326")
        assert tiles.get_covering_tile_ids(gdf) == ["N35_W092"]

    def test_tile_aligned_box_includes_the_touching_neighbours(self):
        # Shots on a tile's edge can belong to the neighbour, so every tile
        # the box touches is kept.
        gdf = region([box(-92, 34, -91, 35)], "EPSG:4326")
        assert len(tiles.get_covering_tile_ids(gdf)) == 9

    def test_includes_the_last_column_of_tiles(self):
        gdf = region([box(179.2, -17.5, 179.8, -16.2)], "EPSG:4326")
        assert sorted(tiles.get_covering_tile_ids(gdf)) == ["S16_E179", "S17_E179"]

    @pytest.mark.parametrize(
        "gdf",
        [
            region([box(177.5, -18.5, 180, -16.3), box(-180, -18.5, -179.2, -16.3)], "EPSG:4326"),
            region(
                [shapely.union(lonlat_box(177.5, -18.5, 180, -16.3), lonlat_box(-180, -18.5, -179.2, -16.3))],
                "EPSG:32760",
                "EPSG:4326",
            ),
        ],
        ids=["lonlat_split", "utm60s"],
    )
    def test_across_the_antimeridian(self, gdf):
        assert sorted(tiles.get_covering_tile_ids(gdf)) == FIJI_TILES

    @pytest.mark.parametrize(
        "gdf",
        [
            # Straight edges in UTM curve in lon/lat.
            region([box(0, 3.5e6, 1e6, 4.5e6)], "EPSG:32615"),
            region([lonlat_box(-99.7, 50.3, -90.4, 51.8)], "EPSG:3413", "EPSG:4326"),
            region([lonlat_box(-72.6, -51.7, -68.2, -48.3)], "EPSG:3031", "EPSG:4326"),
        ],
        ids=["utm15_1000km_square", "polar_stereo_north", "polar_stereo_south"],
    )
    def test_large_projected_regions_match_brute_force(self, gdf):
        assert sorted(tiles.get_covering_tile_ids(gdf)) == brute_force_tiles(gdf)


class TestCoveringTileIds:
    def test_list_in_mixed_crs_is_the_union(self):
        plots = region([lonlat_box(-92.6, 34.2, -91.6, 34.6)], "EPSG:32615", "EPSG:4326")
        fiji = region([box(177.5, -18.5, 180, -16.3), box(-180, -18.5, -179.2, -16.3)], "EPSG:4326")
        assert ducky.covering_tile_ids([plots, fiji]) == sorted(PLOTS_TILES + FIJI_TILES)

    def test_relation_uses_its_geometry_crs(self):
        con = duckdb.connect()
        con.load_extension("spatial")
        plots = region([lonlat_box(-92.6, 34.2, -91.6, 34.6)], "EPSG:32615", "EPSG:4326")
        rel = con.sql(
            "SELECT ST_SetCRS(ST_GeomFromText($1), 'EPSG:32615') AS geom",
            params=[plots.geometry[0].wkt],
        )
        assert ducky.covering_tile_ids(rel) == PLOTS_TILES

    def test_relation_without_crs_is_rejected(self):
        con = duckdb.connect()
        con.load_extension("spatial")
        rel = con.sql("SELECT ST_Point(-92, 34.5) AS geom")
        with pytest.raises(ValueError, match="no CRS"):
            ducky.covering_tile_ids(rel)

    def test_spatial_filter_clause(self):
        plots = region([lonlat_box(-92.6, 34.2, -91.6, 34.6)], "EPSG:4326")
        assert ducky.spatial_filter_clause(plots) == "tile_id IN ('N35_W092', 'N35_W093')"
