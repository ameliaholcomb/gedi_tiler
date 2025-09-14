import argparse
import geopandas as gpd
import hashlib
import pandas as pd
import maap.maap as MAAP

from typing import List

from common import shape_parser, granule_name, cmr_query, s3_utils
from database import ducky, tiles
from constants import GediProduct


def _get_granule_key_for_filename(filename: str) -> str:
    parsed = granule_name.parse_granule_filename(filename)
    return f"{parsed.orbit}_{parsed.sub_orbit_granule}"


def _hash_string_list(string_list: list) -> str:
    joined = ",".join([f"{len(item)}:{item}" for item in string_list])
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def _get_granule_metadata(
    shape: gpd.GeoSeries, products: List[GediProduct]
) -> gpd.GeoDataFrame:
    md_list = []
    for product in products:
        print("Querying NASA metadata API for product: ", product.value)
        df = cmr_query.query(product, spatial=shape, use_cloud=True)
        df["granule_key"] = df.granule_name.map(_get_granule_key_for_filename)
        df["product"] = product.value
        df.rename(columns={"granule_url": f"{product.value}_url"}, inplace=True)
        md_list.append(df)
    md = gpd.GeoDataFrame(
        pd.concat(md_list), geometry="granule_poly"
    ).reset_index(drop=True)

    # Filter out granules with that do not have each required product.
    nprod = md.groupby("granule_key")["product"].nunique()
    omit = nprod[nprod != len(products)].index
    md = md[~md.granule_key.isin(omit)].reset_index(drop=True)
    md.drop(columns=["product"], inplace=True)
    md = md.groupby(["granule_key"]).agg(
        {
            "granule_size": "sum",
            "granule_name": lambda x: list(x),
            GediProduct.L2A.value + "_url": "first",
            GediProduct.L2B.value + "_url": "first",
            GediProduct.L4A.value + "_url": "first",
            GediProduct.L4C.value + "_url": "first",
            "geometry": "first",
        }
    )
    md.rename(columns={"granule_name": "granule_names"}, inplace=True)
    md["granule_hash"] = md.granule_names.apply(_hash_string_list)
    return gpd.GeoDataFrame(
        md, geometry="geometry", crs="EPSG:4326"
    ).reset_index()


def main(args):
    # Metadata is written first, then data is backfilled by the DPS jobs.
    # DPS jobs can fail, be re-run, etc, but the metadata is only written once.

    # If metadata was written erroneously, search for metadata tiles with the
    # problematic tile_id or cmr_access_time and delete them.

    # This script generates the metadata describing the per-tile granule info for the region,
    # (which is also where each DPS tile builder looks to find which granules to process),
    # and updates S3://<db>/metadata/ with any tiles not already present in S3://<db>/metadata/.
    # It then submits a job for each tile in the region that does not already exist in S3://<db>/data/.

    # 1. Get required tiles for region
    covering_tiles, covering = tiles.get_covering_tiles_for_region(args.shape)
    products = [
        GediProduct.L2A,
        GediProduct.L2B,
        GediProduct.L4A,
        GediProduct.L4C,
    ]
    # Get CMR metadata for all granules covering the region
    cmr_md = _get_granule_metadata(
        shape_parser.check_and_format_shape(covering), products
    )
    # Join to find which granules are needed for each tile
    tile_granule_gdf = covering_tiles.sjoin(
        cmr_md, how="inner", predicate="intersects"
    )
    tile_granule_gdf.drop(columns=["index_right"], inplace=True)
    tile_granule_gdf["cmr_access_time"] = pd.Timestamp.now(tz="UTC")
    required_tiles = set(tile_granule_gdf.tile_id.unique())

    # 2. Get existing tiles in the database
    # check if the database path exists:
    print("Checking for existing tiles in the database...")
    path = ducky.data_prefix(args.bucket, args.prefix)
    if s3_utils.s3_prefix_exists(path):
        con = ducky.init_duckdb()
        data_spec = ducky.data_spec(args.bucket, args.prefix)
        existing_tiles = con.execute(
            f"SELECT DISTINCT tile_id FROM read_parquet('{data_spec}')"
        ).fetchall()
        existing_tiles = {x[0] for x in existing_tiles}
        # TODO: Do we need a way for jobs to mark that they completed but had no data?
    else:
        existing_tiles = set()

    missing_tiles = [x for x in required_tiles if x not in existing_tiles]
    print(f"{len(existing_tiles)} tiles already exist in the database, ")
    print(f"{len(missing_tiles)} new tiles to process.")
    if args.dry_run:
        return

    # 3. Create new metadata dataframe for tiles in region and write to S3
    # the metadata that we expect to describe the database after all jobs complete
    print("Writing metadata for required tiles to S3...")
    con = ducky.init_duckdb()
    ducky.gdf_to_duck(con, tile_granule_gdf, "tile_granule_gdf")
    md_prefix = ducky.metadata_prefix(args.bucket, args.prefix)
    con.sql(f"""
        COPY tile_granule_gdf TO '{md_prefix}' (
            FORMAT parquet,
            PARTITION_BY ({ducky.TILE_ID}),
            COMPRESSION zstd,
            OVERWRITE_OR_IGNORE
        );
    """)

    # 4. Submit jobs for tiles in required_tiles but not in existing_tiles
    # maap = MAAP()
    # for tile_id in missing_tiles:
    #     print(f"Submitting job for tile {tile_id}...")
    #     job_name = f"tiled-gedi-{tile_id}"
    #     job = maap.submitJob(
    #         identifier=job_name,
    #         algo_id=algorithm,
    #         version=branch,
    #         queue="maap-dps-worker-32gb",
    #         schema_path=args.schema,
    #         bucket=args.bucket,
    #         prefix=args.prefix,
    #         tile_id=tile_id,
    #         quality="quality",
    #     )
    #     print(f"Job {job_name} submitted with ID {job['id']}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Manage MAAP jobs to create a tiled GEDI database."
    )
    parser.add_argument(
        "--shapefile",
        type=str,
        required=True,
        help="Path to region shapefile to process.",
    )
    parser.add_argument(
        "--schema",
        type=str,
        required=True,
        help="Path to YML file defining database schema.",
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
    shp = gpd.read_file(args.shapefile)
    args.shape = shp.head(1)
    args.shape = shp

    main(args)
