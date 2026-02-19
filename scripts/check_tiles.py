import argparse
import geopandas as gpd
import os
import pandas as pd

from gtiler.common.granule_metadata import get_granule_metadata

from gtiler.common import shape_parser, s3_utils
from gtiler.database import ducky, tiles
from gtiler.database.schema import GediProduct


def main(args):
    con = ducky.init_duckdb()
    # 1. Get relevant tiles for region
    covering_tiles, covering = tiles.get_covering_tiles_for_region(args.shapefile)

    # 2. Get current up-to-date CMR granule metadata for each tile
    products = [
        GediProduct.L2A,
        GediProduct.L2B,
        GediProduct.L4A,
        GediProduct.L4C,
    ]
    # Get CMR metadata for all granules covering the region
    cmr_md = get_granule_metadata(
        shape_parser.check_and_format_shape(covering), products
    )
    tile_granule_gdf = covering_tiles.sjoin(
        cmr_md, how="inner", predicate="intersects"
    )
    tile_granule_gdf.drop(columns=["index_right"], inplace=True)
    tile_granule_gdf["cmr_access_time"] = pd.Timestamp.now(tz="UTC")
    ducky.gdf_to_duck(con, tile_granule_gdf, "cmr_md")

    # 2. Get the existing metadata from the database
    print("Scanning existing metadata ...")
    path = ducky.metadata_prefix(args.bucket, args.prefix)
    if s3_utils.s3_prefix_exists(path):
        md_spec = ducky.metadata_spec(args.bucket, args.prefix)
        con.sql(f"""--sql
                    CREATE OR REPLACE VIEW existing_md AS
                    SELECT * FROM read_parquet('{md_spec}')
                    """)
    # Confirm that existing_md is well-formed, i.e., has no duplicate granules
    print("Checking validity of existing metadata ...")
    duplicates = con.sql("""--sql
        SELECT COUNT(*),
        FROM read_parquet('{md_spec}')
        GROUP BY tile_id, granule_key
        HAVING COUNT(*) > 1
    """)
    n = con.sql("SELECT COUNT(*) FROM duplicates").fetchone()[0]
    if n > 0:
        print(f"Error: {n} duplicate granules found in existing metadata.")
        print(con.sql("SELECT * FROM duplicates"))
        raise ValueError()
    # Confirm that all tiles for region are present in existing_md
    print("Checking that all tiles in region are present in the database ...")
    existing_tiles = con.sql("""--sql
        SELECT DISTINCT tile_id
        FROM existing_md
    """).fetchall()
    existing_tiles = set([t[0] for t in existing_tiles])
    if not covering_tiles.tile_id.isin(existing_tiles).all():
        raise ValueError(
            "Error: Region includes areas not yet present in the database." \
            " Please run initial tiling first."
        )
    
    # 3. Identify missing granules in each tile_id
    print("Checking for missing and out-of-date granules ...")
    # First, confirm that no existing granules have mismatched hashes
    mismatched = con.sql("""--sql
        SELECT
                e.tile_id,
                e.granule_key,
                e.granule_hash AS hash_existing,
                c.granule_hash AS hash_cmr
        FROM cmr_md AS c
        JOIN existing_md AS e
        USING (tile_id, granule_key)
        WHERE c.granule_hash <> e.granule_hash
    """)
    n = con.sql("SELECT COUNT(*) FROM mismatched").fetchone()[0]
    if n > 0:
        print(f"Error: {n} mismatched granule hashes found in tiles:")
        print(con.sql("SELECT DISTINCT tile_id FROM mismatched"))
        # TODO(amelia): Instead of raising an error, optionally 
        # remove these tiles and re-download them.
        raise ValueError()
    # Next, create a table of missing granules in each tile
    con.sql("""--sql
            CREATE OR REPLACE VIEW missing_granules AS
            SELECT *
            FROM cmr_md c
            LEFT JOIN existing_md e
            USING (tile_id, granule_key)
            WHERE e.granule_key IS NULL
            """)
    
    # 4. For each tile with missing granules, move the existing data into a 
    # backup location, marking the tile as needing to be re-processed.
    os.makedirs("updating", exist_ok=True)

    missing_tiles = con.sql("""--sql
        SELECT DISTINCT tile_id
        FROM missing_granules
    """).fetchall()
    missing_tiles = [t[0] for t in missing_tiles]
    rfs = s3_utils.RefreshableFSSpec("/iam/maap-data-reader")
    fs = rfs.get_fs()

    for tile in missing_tiles:
        tile_dir = ducky.tile_data_dir(args.bucket, args.prefix, tile)
        tile_updates_dir = ducky.tile_updates_dir(args.bucket, args.prefix, tile)
        fs.mv(tile_dir, tile_updates_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update tiles in the tiled GEDI database on MAAP."
    )
    parser.add_argument(
        "--job_code",
        type=str,
        required=True,
        help="Shared code for all MAAP tasks created by this run.",
    )
    parser.add_argument(
        "--shapefile",
        type=str,
        required=True,
        help="Path to region shapefile to process.",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        required=True,
        help="S3 bucket containing tiled GEDI database.",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        required=True,
        help="S3 prefix for tiled GEDI database.",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Print execution plan, but do not run any MAAP jobs.",
    )

    args = parser.parse_args()
    args.prefix = args.prefix.strip("/").rstrip("/")

    main(args)
