import boto3
import os
import tempfile
from typing import List
import duckdb
import geopandas as gpd
import warnings
import shapely.ops

from gtiler.database import tiles

TILE_ID = "tile_id"
YEAR = "year"
# Written by jobs that complete without producing any footprints, so that
# they are not planned again. Not a parquet file, so data globs skip it.
EMPTY_MARKER = "_EMPTY"
ESA_TESTDB_PATH = "s3://nasa-maap-data-store/file-staging/nasa-map/gedi-tiled-v2"
ESA_TESTDB_MANIFEST_PATH = f"{ESA_TESTDB_PATH}/manifest.txt"
ESA_TESTDB_ICEBERG_PATH = f"{ESA_TESTDB_PATH}/iceberg/gedi_tiled_v2/metadata/latest.metadata.json"

def refresh_credentials(con):
    """Give DuckDB the session's current AWS credentials.

    The credentials on the MAAP Hub expire after about an hour, after which
    reads fail with ExpiredToken; run this again to renew them.
    """
    creds = boto3.Session().get_credentials().get_frozen_credentials()
    # Set AWS credentials explicitly; using the credential chain sometimes
    # fetches the incorrect credentials on the MAAP Hub.
    con.execute(f"""
        CREATE OR REPLACE SECRET (
            TYPE s3,
            KEY_ID '{creds.access_key}',
            SECRET '{creds.secret_key}',
            SESSION_TOKEN '{creds.token}',
            REGION 'us-west-2'
        )
    """)


def init_duckdb(temp_dir: str = None):
    con = duckdb.connect()
    # con.execute("SET access_mode = 'READ_ONLY';")
    con.install_extension("spatial")
    con.load_extension("spatial")
    con.install_extension("aws")
    con.load_extension("aws")
    con.install_extension("httpfs")
    con.load_extension("httpfs")
    refresh_credentials(con)
    con.execute("SET enable_progress_bar = true;")
    con.execute("SET preserve_insertion_order = false;")
    con.execute("SET memory_limit = '8GB';")
    # date_part on a timestamptz follows the session zone, and the year
    # it returns decides which partition a footprint lands in.
    con.execute("SET TimeZone = 'UTC';")
    if temp_dir:
        con.sql(f"SET temp_directory='{temp_dir}'")
    con.sql("SET max_temp_directory_size = '100GB'")
    return con


def init_duckdb_esa(temp_dir: str = None, memory_limit = '8GB'):
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    con.install_extension("httpfs")
    con.load_extension("httpfs")
    con.install_extension("iceberg")
    con.load_extension("iceberg")
    con.execute("SET enable_progress_bar = true;")
    con.execute("SET preserve_insertion_order = false;")
    con.execute(f"SET memory_limit = '{memory_limit}';")
    if temp_dir:
        con.sql(f"SET temp_directory='{temp_dir}'")
    con.sql("SET max_temp_directory_size = '100GB'")
    return con
    
    
def load_database_from_manifest(con, manifest_path, name = 'gedi_data'):
    con.sql(f"""
        SET VARIABLE gedi_v2_files = (
            SELECT list(href)
            FROM read_csv(
                '{manifest_path}',
                header = false,
                columns = {{'href': 'VARCHAR'}}
            )
        );
        CREATE OR REPLACE VIEW {name} AS
        SELECT *
        FROM read_parquet(
            getvariable('gedi_v2_files'),
            hive_partitioning = true
        );
    """)


def load_database_from_iceberg(con, iceberg_path, name = 'gedi_iceberg'):
    return con.sql(f"""
        CREATE OR REPLACE VIEW {name} AS
        SELECT *
        FROM iceberg_scan('{iceberg_path}')
    """)
    
def brazil_data_spec():
    BUCKET = "maap-ops-workspace"
    PREFIX = "shared/ameliah/gedi-test/brazil_tiles"
    return data_spec(BUCKET, PREFIX)


def attach_ducklake(con, bucket, prefix, name="gedi_dl"):
    """Attach the DuckLake catalog read-only and define the spatial macros.

    The catalog is copied to local disk first: DuckDB reading it in place
    on S3 adds ~20 s to the first query of every session, while the copy
    takes a couple of seconds.
    """
    local_dir = os.path.join(tempfile.gettempdir(), "gtiler", bucket, prefix)
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, "gedi.ducklake")
    boto3.client("s3").download_file(
        bucket, f"{prefix}/ducklake/gedi.ducklake", local_path
    )
    con.sql(f"""--sql
            ATTACH 'ducklake:{local_path}' AS {name} (READ_ONLY);
            USE {name};
    """)
    create_spatial_macros(con, f"{name}.data")


def create_spatial_macros(con, table):
    """Define a table macro selecting the footprints in a lon/lat box of
    `table`:

        SELECT * FROM gedi_bbox(xmin, ymin, xmax, ymax)

    It filters on lat/lon ranges, which the DuckLake catalog uses to skip
    files; a bare ST_Intersects would open every file in the database.
    For other regions, use set_region.
    """
    con.sql(f"""--sql
            CREATE OR REPLACE TEMP MACRO gedi_bbox(xmin, ymin, xmax, ymax) AS TABLE
            SELECT * FROM {table}
            WHERE lon_lowestmode BETWEEN xmin AND xmax
                AND lat_lowestmode BETWEEN ymin AND ymax;
    """)


def set_region(con, gdf: gpd.GeoDataFrame, name: str = "region"):
    """Store the union of a GeoDataFrame's shapes as a SQL variable,
    for use as e.g. `gedi_within(getvariable('region'))`."""
    con.execute(
        f"SET VARIABLE {name} = ST_GeomFromWKB($1)",
        [gdf.to_crs("EPSG:4326").union_all().wkb],
    )


def data_prefix(bucket, prefix):
    return f"s3://{bucket}/{prefix}/data/"


def metadata_prefix(bucket, prefix):
    return f"s3://{bucket}/{prefix}/metadata/"


def empty_marker_path(bucket, prefix, tile, year):
    return (
        f"{data_prefix(bucket, prefix)}{TILE_ID}={tile}/{YEAR}={year}/"
        f"{EMPTY_MARKER}"
    )


def empty_marker_spec(bucket, prefix):
    """Glob matching every empty marker in the database."""
    return f"{data_prefix(bucket, prefix)}{TILE_ID}=*/{YEAR}=*/{EMPTY_MARKER}"


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
    geometry_columns: List[str] = ["geometry"],
) -> duckdb.DuckDBPyRelation:
    """Load a GeoDataFrame into a DuckDB table."""
    # Convert geometries to WKT
    gdf_tmp = gdf.copy()
    with warnings.catch_warnings():
        # ignore that the df now has a geometry column of strings
        warnings.simplefilter("ignore")
        for col in geometry_columns:
            gdf_tmp[col] = gpd.GeoSeries(gdf_tmp[col]).to_wkt()
    replace_cols = ", ".join(
        [f"ST_GeomFromText({col}) AS {col}" for col in geometry_columns]
    )
    # Execute immediately to use local context table (gdf_tmp)
    rel = con.sql(f"""
        SELECT * REPLACE ({replace_cols})
        FROM gdf_tmp
    """).execute()
    return rel
