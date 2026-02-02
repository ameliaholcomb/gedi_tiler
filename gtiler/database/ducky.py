from typing import List
import duckdb
import geopandas as gpd
import warnings

from . import tiles

TILE_ID = "tile_id"
YEAR = "year"


def init_duckdb():
    con = duckdb.connect()
    # con.execute("SET access_mode = 'READ_ONLY';")
    con.install_extension("spatial")
    con.load_extension("spatial")
    con.install_extension("aws")
    con.load_extension("aws")
    con.install_extension("httpfs")
    con.load_extension("httpfs")
    con.execute("CREATE SECRET ( TYPE s3, PROVIDER credential_chain);")
    con.execute("SET enable_progress_bar = true;")
    con.execute("SET preserve_insertion_order = false;")
    con.execute("SET memory_limit = '8GB';")
    # con.sql("SET temp_directory='/projects/my-private-bucket/tmp/duckdb_swap'")
    # con.sql("SET max_temp_directory_size = '100GB'")
    return con


def brazil_data_spec():
    BUCKET = "maap-ops-workspace"
    PREFIX = "shared/ameliah/gedi-test/brazil_tiles"
    return data_spec(BUCKET, PREFIX)


def data_prefix(bucket, prefix):
    return f"s3://{bucket}/{prefix}/data/"


def metadata_prefix(bucket, prefix):
    return f"s3://{bucket}/{prefix}/metadata/"


def data_spec(bucket, prefix, tile=None, year=None):
    tile_part = "*"
    year_part = "*"
    if tile is not None:
        tile_part = f"{TILE_ID}={tile}"
    if year is not None:
        year_part = f"{YEAR}={year}"
    return f"s3://{bucket}/{prefix}/data/{tile_part}/{year_part}/*.parquet"


def metadata_spec(bucket, prefix, tile=None):
    tile_part = "*"
    if tile is not None:
        tile_part = f"{TILE_ID}={tile}"
    return f"s3://{bucket}/{prefix}/metadata/{tile_part}/*.parquet"


def spatial_filter_clause(gdf: gpd.GeoDataFrame) -> str:
    """Create a filter clause to help DuckDB look at only relevant tiles."""
    covering_tiles, _ = tiles.get_covering_tiles_for_region(gdf)
    clause = " OR ".join(
        [f"tile_id = '{t}'" for t in covering_tiles.tile_id.values]
    )
    return f"({clause})"


def duck_to_gdf(
    table, geometry_columns=["geometry"], crs="EPSG:4326"
) -> gpd.GeoDataFrame:
    """Convert a DuckDB table to a GeoDataFrame.
    If multiple geometry columns are specified,
    the first will be set as the active geometry.
    """
    for geom_col in geometry_columns:
        if geom_col not in table.columns:
            raise ValueError(f"Column '{geom_col}' not found in table.")
    replace_cols = ", ".join(
        [f"ST_AsHEXWKB({col}) AS {col}" for col in geometry_columns]
    )
    df = table.select(f"* REPLACE ({replace_cols})").to_df()
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.GeoSeries.from_wkb(df[geometry_columns[0]]),
        crs=crs,
    )
    if len(geometry_columns) > 1:
        for geom_col in geometry_columns[1:]:
            gdf[geom_col] = gpd.GeoSeries.from_wkb(df[geom_col])
    return gdf


def gdf_to_duck(
    con,
    gdf: gpd.GeoDataFrame,
    table_name: str,
    geometry_columns: List[str] = ["geometry"],
):
    """Load a GeoDataFrame into a DuckDB table."""
    # Convert geometries to WKT
    gdf_tmp = gdf.copy()
    with warnings.catch_warnings():
        # ignore that the df now has a geometry column of strings
        warnings.simplefilter("ignore")
        for col in geometry_columns:
            gdf_tmp[col] = gdf_tmp[col].to_wkt()

    replace_cols = ", ".join(
        [f"ST_GeomFromText({col}) AS {col}" for col in geometry_columns]
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * REPLACE ({replace_cols})
        FROM gdf_tmp
    """)
