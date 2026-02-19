import geopandas as gpd
import h3
import math
import pandas as pd
from shapely.geometry import Polygon
import warnings

from typing import List, Optional

from gtiler.database import ducky

H3_RESOLUTION = 11
K_RING = 2
EDGE_LENGTH_M = 28.66
# SQRT(67)/4 * edge_length is the smallest distance between a point in the
# central hexagon and a point outside of the h3_disk of radius 2.
# This is therefore the largest theoretical distance supported by the library 
# without changing the H3_resolution and k-ring diameter. 
# (with a 5 m buffer to account for hexagon size variability.)
# Changing the H3 resolution can have high query performance impact.
# Without exploring the optimal tradeoff for various distances, for now we
# hard-code this one, which works well for diameters up to ~50 m.
MAX_DISTANCE_M = math.sqrt(67)/4 * EDGE_LENGTH_M - 5


def find_repeat_footprints(
    con,
    data_spec: str,
    geom: Polygon,
    distance_threshold_m: float = 40.0,
    filters: Optional[str] = None,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Find pairs of GEDI footprints that are within a specified distance of each other.

    Uses H3 spatial indexing to efficiently identify candidate pairs, then computes exact distances using the Haversine formula.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection, optional
        Existing DuckDB connection.
    data_spec : str
        Data spec (glob string) for the GEDI table parquet files.
    geom : Polygon
        Shapely Polygon defining the area of interest in (LON, LAT) format.
    distance_threshold_m : float, default 40.0
        Maximum distance in meters between footprint centers for a pair to be included. 
    filters : str, optional
        SQL WHERE clause conditions to filter footprints (e.g., "l4_quality_flag_l4a = 1").
        Do not include "WHERE" keyword.
    columns : list of str, optional
        Additional columns to extract for each footprint. These will be included
        with suffixes "_1" and "_2" for the two shots in each pair.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
        - shot_number_1, shot_number_2: Shot numbers of the pair
        - lat_1, lon_1, lat_2, lon_2: Coordinates of the two shots
        - distance_m: Distance between footprint centers in meters
        - Additional requested columns with _1 and _2 suffixes

    Examples
    --------
    >>> from shapely.geometry import box
    >>> from gedih3.crossovers import find_repeat_footprints
    >>> bbox = box(-9, -68.5, -8.9, -68.25)
    >>> pairs = find_repeat_footprints(
    ...     con,
    ...     data_spec="s3://bucket/prefix/data/*/*/*.parquet",
    ...     bbox,
    ...     distance_threshold_m=40,
    ...     filters="l4_quality_flag_l4a = 1",
    ...     columns=["agbd_l4a", "rh_98_l2a"]
    ... )
    """

    # Ensure that this is a suitably small region
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        region = gpd.GeoDataFrame(geometry=[geom], crs="EPSG:4326")
        if region.area.sum() > 9:
            raise ValueError(
                "This function is not yet designed to handle large regions. "
                "Please contact Amelia and tell her you need this feature ASAP."
            )
    if not distance_threshold_m < MAX_DISTANCE_M:
        raise ValueError(
            f"Distance threshold {distance_threshold_m} m is too large. "
            f"Maximum supported distance {MAX_DISTANCE_M} m."
        )

    con.execute("INSTALL h3 FROM community;")
    con.execute("LOAD h3;")

    # Build column selection
    base_columns = ["shot_number", "lat_lowestmode", "lon_lowestmode"]
    if columns:
        select_columns = base_columns + [c for c in columns if c not in base_columns]
    else:
        select_columns = base_columns

    select_clause = ", ".join(select_columns)

    # Build filter clause
    filter_clause = f"ST_Contains(ST_GeomFromText('{geom.wkt}'), geometry)"
    spatial_filter = ducky.spatial_filter_clause(
        gpd.GeoDataFrame(geometry=[geom], crs="EPSG:4326"))
    filter_clause += f" AND {spatial_filter}"
    if filters:
        filter_clause += f" AND {filters}"

    # 1. Load points in the region of interest
    h3_resolution = 11
    k_ring = 2
    con.execute(f"""--sql
        CREATE OR REPLACE TEMP TABLE pts AS
        SELECT {select_clause},
               h3_latlng_to_cell(lat_lowestmode, lon_lowestmode, {h3_resolution}) AS h3_cell
        FROM read_parquet('{data_spec}')
        WHERE {filter_clause};
    """)

    # 2. Candidate expansion: for each point, enumerate nearby H3 cells (k-ring)
    con.execute(f"""--sql
        CREATE OR REPLACE TEMP TABLE pts_expanded AS
        SELECT
            {select_clause},
            UNNEST(h3_grid_disk(h3_cell, {k_ring})) AS cand_cell
        FROM pts;
    """)

    # 3. Candidate pairs via join on candidate cell

    # Build select clause for joined output
    join_cols_1 = ""
    join_cols_2 = ""
    if columns:
        join_cols_1 = ", " + ", ".join([f"t1.{c} AS t1_{c}" for c in columns])
        join_cols_2 = ", " + ", ".join([f"t2.{c} AS t2_{c}" for c in columns])
    
    joined_cols = ", ".join([f"t1_{c}" for c in columns] + [f"t2_{c}" for c in columns])


    # Note: ST_Point takes (lat, lon) order for our data
    query = f"""--sql
        CREATE OR REPLACE TEMP TABLE pairs AS
        WITH joined AS (
            SELECT
                t1.shot_number AS t1_shot_number,
                t2.shot_number AS t2_shot_number,
                t1.lat_lowestmode AS t1_lat_lowestmode,
                t1.lon_lowestmode AS t1_lon_lowestmode,
                t2.lat_lowestmode AS t2_lat_lowestmode,
                t2.lon_lowestmode AS t2_lon_lowestmode
                {join_cols_1}
                {join_cols_2}
            FROM pts_expanded t1
            JOIN pts t2
                ON t1.shot_number < t2.shot_number
                AND t2.h3_cell = t1.cand_cell
            WHERE t1.shot_number < t2.shot_number
        ),
        distances AS (
            SELECT
                t1_shot_number,
                t2_shot_number,
                t1_lat_lowestmode,
                t1_lon_lowestmode,
                t2_lat_lowestmode,
                t2_lon_lowestmode,
                ST_Distance_Spheroid(
                    ST_Point(t1_lat_lowestmode, t1_lon_lowestmode),
                    ST_Point(t2_lat_lowestmode, t2_lon_lowestmode)
                ) AS distance_m,
                {joined_cols}
            FROM joined
        )
        SELECT * FROM distances
        WHERE distance_m <= {distance_threshold_m}
    """
    con.execute(query)
    result = con.sql("SELECT * FROM pairs").df()

    # Clean up temp table
    con.execute("DROP TABLE IF EXISTS filtered_shots")

    return result
