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


def duck_to_gdf(table, geometry_column="geometry") -> gpd.GeoDataFrame:
    """Convert a DuckDB table to a GeoDataFrame."""
    if geometry_column not in table.columns:
        raise ValueError(f"Column '{geometry_column}' not found in table.")
    df = table.select(
        f"* REPLACE ST_AsHEXWKB({geometry_column}) AS geometry"
    ).to_df()
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.GeoSeries.from_wkb(df.geometry),
        crs="EPSG:4326",
    )
    return gdf


def gdf_to_duck(con, gdf: gpd.GeoDataFrame, table_name: str):
    """Load a GeoDataFrame into a DuckDB table."""
    # Convert geometries to WKT
    gdf_tmp = gdf.copy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        # ignore that the df now has a geometry column of strings
        gdf_tmp["geometry"] = gdf_tmp["geometry"].to_wkt()
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * REPLACE ST_GeomFromText(geometry) AS geometry
        FROM gdf_tmp
    """)
