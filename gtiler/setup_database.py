import argparse
import duckdb
import fsspec
import h5py
import geopandas as gpd
import numpy as np
import pandas as pd
import re
from typing import Dict, List, Tuple
from shapely.geometry import box

from common import schema_parser, cmr_query, granule_name
from common import s3_utils


class Tile:
    minx: float
    maxx: float
    miny: float
    maxy: float
    id: str
    shape: gpd.GeoSeries

    def __init__(self, tile_id: str):
        self.id = tile_id
        retile = re.compile(r"^([NS])(\d{2})_([EW])(\d{3})$")
        if not retile.match(tile_id):
            raise ValueError(f"{tile_id} invalid: must be [NS][DD]_[EW][DDD].")
        match = retile.match(tile_id)
        ns = match.group(1)
        ew = match.group(3)
        lat = int(match.group(2))
        lon = int(match.group(4))
        if ns == "S":
            lat = -lat
        if ew == "W":
            lon = -lon
        self.minx = lon
        self.maxx = lon + 1
        self.miny = lat - 1
        self.maxy = lat
        self.shape = gpd.GeoSeries(
            [box(self.minx, self.miny, self.maxx, self.maxy)],
            crs="EPSG:4326",
        )


def get_cmd_args():
    p = argparse.ArgumentParser(
        description="Generate hierarchical H3 database for fast spatial querying."
    )
    p.add_argument(
        "-s",
        "--schema_path",
        dest="schema_path",
        type=str,
        required=True,
        help="Location of a file containing the database schema",
    )
    p.add_argument(
        "-b",
        "--bucket",
        dest="bucket",
        type=str,
        required=True,
        default=None,
        help="S3 bucket in which to write the output files.",
    )
    p.add_argument(
        "-p",
        "--prefix",
        dest="prefix",
        type=str,
        required=True,
        default=None,
        help="S3 prefix (folder) in which to write the output files.",
    )
    p.add_argument(
        "-tile_id",
        "--tile_id",
        dest="tile_id",
        type=str,
        required=True,
        default=None,
        help=(
            "Tile ID to process. 1ºx1º degree tiles in the format"
            "[N/S][DD][E/W][DDD], defining the coordinates of the"
            "top-left corner of the tile."
        ),
    )
    p.add_argument(
        "-c",
        "--check",
        dest="check",
        action="store_true",
        help="Only check for updates without writing any files.",
    )
    p.add_argument(
        "-test",
        "--test",
        dest="test",
        action="store_true",
        help="Quick test running over only 2 GEDI granules.",
    )
    cmdargs = p.parse_args()
    return cmdargs


def check_args(args: argparse.Namespace) -> argparse.Namespace:
    """Check the command line arguments and return the updated args."""

    args.schema = schema_parser.check_schema(args.schema_path)

    # Check for a valid TileID
    if args.tile_id:
        try:
            args.tile = Tile(args.tile_id)
        except Exception as e:
            raise ValueError(f"Could not parse tile ID {args.tile_id}: {e}")

    return args


def _get_indices_in_tile(f, beam, columns, tile):
    """Get the range of shot indices for a single beam that lie in the tile."""
    lat_col = [c for c in columns.values() if "lat_lowestmode" in c.lower()]
    lon_col = [c for c in columns.values() if "lon_lowestmode" in c.lower()]
    lats = f[f"{beam}/{lat_col[0]}"][:]
    lons = f[f"{beam}/{lon_col[0]}"][:]
    lat_min = np.argmin(np.abs(lats - tile.miny))
    lat_max = np.argmin(np.abs(lats - tile.maxy))
    lon_min = np.argmin(np.abs(lons - tile.minx))
    lon_max = np.argmin(np.abs(lons - tile.maxx))
    min_idx = max(lat_min, lon_min)
    max_idx = min(lat_max, lon_max)
    return min_idx, max_idx + 1


def load_granule_product(
    fs: fsspec.AbstractFileSystem,
    s3url: str,
    columns: Dict[str, str],
    tile: Tile,
) -> pd.DataFrame:
    """Load a GEDI HDF5 file and return a flattened dataframe.
    Args:
        s3url: S3 URL to the GEDI HDF5 file.
        columns: Dictionary of the form {df_name: sds_name},
            defining the columns to extract from the file.
            df_name is the desired output column name,
            sds_name is the full field in the HDF5 file (including group,
                but omitting BEAM).
            e.g. {"lat_lowestmode": "geolocation/lat_lowestmode"}
    """
    anci = {}
    with fs.open(s3url, mode="rb") as f, h5py.File(f) as hdf5:
        full_df = []
        for k in hdf5.keys():
            if not k.startswith("BEAM"):
                continue
            mini, maxi = _get_indices_in_tile(hdf5, k, columns, tile)
            if mini == maxi:  # no data in beam
                continue
            dfs = {}
            for j in columns.keys():
                if "ancillary" in columns[j].lower():
                    anci[j] = hdf5[f"{k}/{columns[j]}"][:][0]
                    continue
                d = hdf5[f"{k}/{columns[j]}"][mini:maxi]
                if d.ndim == 2:
                    for col in range(d.shape[-1]):
                        jj = f"{j}_{col:03d}"
                        dfs[jj] = d[:, col]
                else:
                    dfs[j] = d
            dfs = pd.DataFrame(dfs)
            dfs["beam_name"] = k
            full_df.append(dfs)
    full_df = pd.concat(full_df)
    for j in anci.keys():
        full_df[j] = anci[j]

    return full_df.dropna().set_index("shot_number")


def load_granule(
    fs: fsspec.AbstractFileSystem,
    granule: str,
    product_files: List[Tuple[str, str]],
    schema,
    tile: Tile,
) -> gpd.GeoDataFrame:
    """Load dataframes for all products and join into a single geodataframe.
    Args:
        granule: Granule name (e.g. OrbitID_GranuleID)
        product_files: List of tuples of the form (product, s3url)
            e.g. [("l4a", "s3://..."), ("l2b", "s3://...")]
        schema: Dictionary defining the data schema for each product.
    """
    dfs = []
    for product, s3url in product_files:
        print("Reading product", product, "from", s3url)
        columns = {
            name: schema[product]["variables"][name]["SDS_Name"]
            for name in schema[product]["variables"].keys()
        }
        df = load_granule_product(fs, s3url, columns, tile)
        dfs.append(df)
    full_df = dfs[0]
    for df in dfs[1:]:
        # expected repeated cols -- keep from first product only
        df.drop(
            columns=["beam_name", "lon_lowestmode", "lat_lowestmode"],
            inplace=True,
        )  # expected repeated cols -- keep from first product only
        full_df = full_df.join(df, how="inner")

    # Add derived data columns
    full_df["granule"] = granule
    gedi_count_start = pd.to_datetime("2018-01-01T00:00:00Z")
    full_df["absolute_time"] = gedi_count_start + pd.to_timedelta(
        full_df["delta_time"], "seconds"
    )
    # make shot_number a column now that the join is finished
    full_df.reset_index(inplace=True)
    return full_df


def _get_granule_key_for_filename(filename: str) -> str:
    parsed = granule_name.parse_granule_filename(filename)
    return f"{parsed.orbit}_{parsed.sub_orbit_granule}"


def _get_granule_metadata(
    shape: gpd.GeoSeries, products: List[str]
) -> gpd.GeoDataFrame:
    md_list = []
    for product in products:
        print("Querying NASA metadata API for product: ", product.value)
        df = cmr_query.query(product, spatial=shape, use_cloud=True)
        df["granule_key"] = df.granule_name.map(_get_granule_key_for_filename)
        df["product"] = product.value
        md_list.append(df)
    md = gpd.GeoDataFrame(
        pd.concat(md_list), geometry="granule_poly"
    ).reset_index(drop=True)

    # Filter out granules with that do not have each required product.
    nprod = md.groupby("granule_key")["product"].nunique()
    omit = nprod[nprod != len(products)].index
    md = md[~md.granule_key.isin(omit)].reset_index(drop=True)
    return md


def run_main(args: argparse.Namespace):
    """Main function to create a tile."""
    # Set up access to the ORNL and LP DAACs
    credentials = s3_utils.assume_role_credentials("/iam/maap-data-reader")
    fs = s3_utils.fsspec_access(credentials)

    # Get the list of products to process
    products = schema_parser.get_products(args.schema)

    # Get metadata for all granules intersecting the tile
    shape = args.tile.shape
    md = _get_granule_metadata(shape, products)

    if args.test:
        granules = md.granule_key.unique()[:2]
        md = md[md.granule_key.isin(granules)].reset_index(drop=True)
        print(f"Test mode: processing {len(md.granule_key.unique())} granules.")
    print(f"Processing {len(md.granule_key.unique())} granules.")

    granules = md.groupby("granule_key").agg(
        {"granule_url": list, "product": list}
    )
    dfs = []
    for granule_key, row in granules.iterrows():
        print(f"Loading granule {granule_key} with products {row['product']}")
        df = load_granule(
            fs,
            granule_key,
            list(zip(row["product"], row["granule_url"])),
            args.schema,
            args.tile,
        )
        print(f"Loaded {len(df)} shots from granule {granule_key}.")
        dfs.append(df)
    full_df = pd.concat(dfs)
    full_df["tile_id"] = args.tile_id

    aws_prefix = f"s3://{args.bucket}/{args.prefix}"
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    con.install_extension("aws")
    con.load_extension("aws")
    con.install_extension("httpfs")
    con.load_extension("httpfs")
    con.execute("CREATE SECRET ( TYPE s3, PROVIDER credential_chain);")

    df = con.sql("""
        SELECT *,
            ST_Point(lon_lowestmode, lat_lowestmode) AS geometry,
            date_part('year', absolute_time) AS year
        FROM full_df
    """)
    con.sql(f"""
        COPY df TO '{aws_prefix}' (
            FORMAT parquet,
            PARTITION_BY (tile_id, year),
            COMPRESSION zstd,
            ROW_GROUP_SIZE 10_000,
            OVERWRITE_OR_IGNORE
        );
    """)

    # TODO: Add to metadata parquet database with granule name, hash, files, etc

    return 0


if __name__ == "__main__":
    args = get_cmd_args()
    args = check_args(args)
    run_main(args)
