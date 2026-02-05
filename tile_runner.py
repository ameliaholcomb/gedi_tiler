import argparse
import datetime as dt
import geopandas as gpd
import hashlib
from maap.maap import MAAP
import pandas as pd
import time

from typing import List, Optional

from gtiler.common import shape_parser, granule_name, cmr_query, s3_utils
from gtiler.common.jobs_manager import JobsManager
from gtiler.database import ducky, tiles
from gtiler.database.schema import GediProduct


def _get_granule_key_for_filename(filename: str) -> str:
    parsed = granule_name.parse_granule_filename(filename)
    return f"{parsed.orbit}_{parsed.sub_orbit_granule}"


def _hash_string_list(string_list: list) -> str:
    joined = ",".join([f"{len(item)}:{item}" for item in string_list])
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def _get_granule_metadata(
    shape: gpd.GeoSeries,
    products: List[GediProduct],
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
) -> gpd.GeoDataFrame:
    md_list = []
    for product in products:
        print("\tQuerying NASA metadata API for product: ", product.value)
        date_range = None
        if start_year is not None and end_year is not None:
            date_range = (
                dt.datetime(start_year, 1, 1),
                dt.datetime(end_year, 12, 31, 23, 59, 59),
            )
        df = cmr_query.query(
            product, spatial=shape, date_range=date_range, use_cloud=True
        )
        df["granule_key"] = df.granule_name.map(_get_granule_key_for_filename)
        df["product"] = product.value
        df.rename(columns={"granule_url": f"{product.value}_url"}, inplace=True)
        print(f"\tFound {len(df)} granules for product {product.value}.")
        md_list.append(df)
    md = gpd.GeoDataFrame(
        pd.concat(md_list), geometry="granule_poly"
    ).reset_index(drop=True)

    # Filter out granules with that do not have each required product.
    nprod = md.groupby("granule_key")["product"].nunique()
    omit = nprod[nprod != len(products)].index
    print(
        f"Excluding {len(omit)}/{len(nprod)} granules with incomplete product sets."
    )
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
    print("Determining required tiles for region...")
    covering_tiles, covering = tiles.get_covering_tiles_for_region(args.shape)
    products = [
        GediProduct.L2A,
        GediProduct.L2B,
        GediProduct.L4A,
        GediProduct.L4C,
    ]
    # Get CMR metadata for all granules covering the region
    cmr_md = _get_granule_metadata(
        shape_parser.check_and_format_shape(
            gpd.GeoDataFrame(geometry=covering),
            exterior_cw=False,
            simplify=True,
        ),
        products,
        start_year=args.start_year,
        end_year=args.end_year,
    )
    # Save the geometry column so that it will not be dropped in the sjoin
    cmr_md["granule_geometry"] = cmr_md.geometry
    # Join to find which granules are needed for each tile
    tile_granule_gdf = covering_tiles.sjoin(
        cmr_md, how="inner", predicate="intersects"
    )
    tile_granule_gdf.drop(columns=["index_right"], inplace=True)
    tile_granule_gdf["cmr_access_time"] = pd.Timestamp.now(tz="UTC")
    required_tiles = set(tile_granule_gdf.tile_id.unique())

    # 2. Get existing metadata tiles in S3
    con = ducky.init_duckdb()
    print("Scanning existing metadata ...")
    path = ducky.metadata_prefix(args.bucket, args.prefix)
    if s3_utils.s3_prefix_exists(path):
        md_spec = ducky.metadata_spec(args.bucket, args.prefix)
        existing_md = con.execute(
            f"SELECT DISTINCT tile_id FROM read_parquet('{md_spec}')"
        ).fetchall()
        existing_md = {x[0] for x in existing_md}

        tile_granule_gdf = tile_granule_gdf[
            ~tile_granule_gdf.tile_id.isin(existing_md)
        ]
    else:
        existing_md = set()

    # 3. Get existing tiles in the database
    # check if the database path exists:
    print("Checking for existing tiles in the database...")
    path = ducky.data_prefix(args.bucket, args.prefix)
    if s3_utils.s3_prefix_exists(path):
        data_spec = ducky.data_spec(args.bucket, args.prefix)
        existing_tiles = con.execute(
            f"SELECT DISTINCT tile_id FROM read_parquet('{data_spec}')"
        ).fetchall()
        existing_tiles = {x[0] for x in existing_tiles}
        # TODO: Do we need a way for jobs to mark that they completed but had no data?
    else:
        existing_tiles = set()

    # tiles with data but no metadata:
    wrong = [x for x in existing_tiles if x not in existing_md]
    if len(wrong) > 0:
        print(
            f"Warning: {len(wrong)} tiles have data but no metadata."
            " Please delete these tiles from the database before continuing:"
            ", ".join(wrong)
        )
        exit(1)

    missing_tiles = [x for x in required_tiles if x not in existing_tiles]
    relevant_md_tiles = {x for x in existing_md if x in required_tiles}
    relevant_data_tiles = {x for x in existing_tiles if x in required_tiles}
    # fmt: off
    print(f"\t{len(required_tiles)} tiles in the region.")
    print(f"\t{len(relevant_md_tiles)} metadata tiles in the database for this region.")
    print(f"\t{len(relevant_data_tiles)} tiles already exist in the database for this region.")
    print(f"\tPlanning to add metadata for {len(required_tiles) - len(relevant_md_tiles)} new tiles.")
    print(f"\t(Which should match this number: {tile_granule_gdf.tile_id.nunique()})")
    print(f"\tPlanning to create jobs to process data for {len(missing_tiles)} tiles.")
    # fmt: on

    if args.dry_run:
        return

    # 3. Create new metadata dataframe for tiles in region and write to S3
    # the metadata that we expect to describe the database after all jobs complete
    if len(tile_granule_gdf) > 0:
        input("To proceed to create tile metadata, press ENTER >>>")
        print("Writing metadata for required tiles to S3...")

        ducky.gdf_to_duck(
            con,
            tile_granule_gdf,
            "tile_granule_gdf",
            geometry_columns=["geometry", "granule_geometry"],
        )
        md_prefix = ducky.metadata_prefix(args.bucket, args.prefix)
        con.sql(f"""
            COPY tile_granule_gdf TO '{md_prefix}' (
                FORMAT parquet,
                PARTITION_BY ({ducky.TILE_ID}),
                COMPRESSION zstd,
                OVERWRITE_OR_IGNORE
            );
        """)

        logfile = f"logs/tile_plan_{args.job_code}_{args.job_iteration}.txt"
        with open(logfile, "w") as f:
            for tile_id in sorted(missing_tiles):
                f.write(f"{tile_id}\n")
        print(
            f"Proposed metadata for tiles listed in {logfile} written to database."
        )

    input("To proceed to create jobs, press ENTER >>>")

    # 4. Submit jobs for tiles in required_tiles but not in existing_tiles
    jobs_manager = JobsManager(
        job_code=args.job_code,
        job_iteration=args.job_iteration,
        s3_bucket=args.bucket,
        s3_prefix=args.prefix,
        algorithm_id="gedi-tile-writer",
        algorithm_version="amelia-deploy-fskBFkTO",
        tile_ids=missing_tiles,
    )
    jobs_manager.manage()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Manage MAAP jobs to create a tiled GEDI database."
    )
    parser.add_argument(
        "--job_code",
        type=str,
        required=True,
        help="Shared code for all MAAP tasks in this database build (subregion identifier).",
    )
    parser.add_argument(
        "--job_iteration",
        "-i",
        type=int,
        required=True,
        help="Iteration number for this run of the job code.",
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
        "--start_year",
        type=int,
        help="Start year for data to include (inclusive).",
    )
    parser.add_argument(
        "--end_year",
        type=int,
        help="End year for data to include (inclusive).",
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
