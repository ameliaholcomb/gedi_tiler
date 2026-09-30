import argparse
from botocore.exceptions import ReadTimeoutError, ConnectTimeoutError
import h5py
import geopandas as gpd
import logging
import numpy as np
import os
import pandas as pd
import pathlib
import psutil
import pyarrow as pa
import subprocess
import sys
import tempfile
from typing import List, Tuple

import time

from gtiler.database import ducky
from gtiler.database.tiles import Tile
from gtiler.common import s3_utils
from gtiler.common import checkpoint_lib
from gtiler.database.schema_v3 import SCHEMA, NULLABLE_DTYPES, TILE_COPY_OPTIONS
from gtiler.database.schema_v3 import Column, Product, GeometryColumn  # typing only

logger = logging.getLogger(__name__)


EASE_X_ORIGIN = -17367530.445161499083042
EASE_Y_ORIGIN = 7314540.830638599582016
EASE_X_SCALE = 1000.895023349556141
EASE_Y_SCALE = 1000.895023349562052

# Local disk and memory budget. Granule files are downloaded one at a time
# into the job's working directory and deleted after reading (the largest,
# L2A, is ~2 GB). Each batch's shots are written to a local parquet part
# (also uploaded as a checkpoint), so only one batch is held in memory. The
# final sort reads the parts, is capped at DUCKDB_MEMORY_LIMIT, and spills
# at most DUCKDB_MAX_TEMP to the same directory. A 200k-row group at
# full width took ~3.2 GB of that in the parquet writer, so the sort writes a
# local copy in small row groups and a second pass copies it into the
# output. One thread keeps the sort within the limit and the output in
# exact Hilbert order.
DUCKDB_MEMORY_LIMIT = "4GB"
DUCKDB_MAX_TEMP = "15GB"
DUCKDB_THREADS = 1
# Row group size for the parts and the sorted local copy.
# Small, so that writing them costs little memory.
STAGE_ROW_GROUP_SIZE = 50_000

# Columns read from the tile metadata. The geometry columns are not
# needed to build a tile, and reading them costs a conversion.
GRANULE_COLUMNS = [
    "granule_key",
    "level2A_url",
    "level2B_url",
    "level4A_url",
    "level4C_url",
    "time_start",
    "time_end",
    "quality_filter",
]

# Datasets some granules lack, read as nulls where missing. Six L4C V003
# granules (orbits 20757-20766) have only the rel2 WSCI quality flags.
MISSING_SDS = {
    "wsci_prediction/l4c_quality_flag_rel3_a1",
    "wsci_prediction/l4c_quality_flag_rel3_a10",
    "wsci_prediction/l4c_quality_flag_rel3_a2",
    "wsci_prediction/l4c_quality_flag_rel3_a5",
}
# Types of the columns the builder adds, beyond the schema's.

def get_cmd_args():
    p = argparse.ArgumentParser(
        description="Generate hierarchical H3 database for fast spatial querying."
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
        "-y",
        "--year",
        dest="year",
        type=int,
        required=True,
        help=(
            "Year to process. Only footprints acquired in this year are "
            "written, even when a granule spans New Year."
        ),
    )
    p.add_argument(
        "-g",
        "--generation",
        dest="generation",
        type=int,
        default=0,
        help=(
            "Generation number for this job. Used for optimistic concurrency"
            "control of checkpoints. Increment this number to start a new "
            "generation of checkpoints, which will cause older jobs to fail in "
            "favor of the new generation. If the generation number is not "
            "incremented, jobs issued for the same tile will simply win based on "
            "which writes to the checkpoint first."
        ),
    )
    p.add_argument(
        "-i",
        "--checkpoint_interval",
        dest="checkpoint_interval",
        type=int,
        default=30,
        help="Number of granules to process between writing checkpoints.",
    )
    p.add_argument(
        "-test",
        "--test",
        dest="test",
        action="store_true",
        help="Quick test running over only 2 GEDI granules.",
    )
    p.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="store_true",
        help="Enable DEBUG-level logging (default is INFO).",
    )
    cmdargs = p.parse_args()
    return cmdargs


def check_args(args: argparse.Namespace) -> argparse.Namespace:
    """Check the command line arguments and return the updated args."""

    args.prefix = args.prefix.strip("/").rstrip("/")

    # Check for a valid TileID
    if args.tile_id:
        try:
            args.tile = Tile(args.tile_id)
        except Exception as e:
            raise ValueError(f"Could not parse tile ID {args.tile_id}: {e}")

    return args


def _get_indices_in_tile(f, beam, geometry: GeometryColumn, tile):
    """Get the range of shot indices for a single beam that lie in the tile."""
    # TODO: This function could use some tests.
    # e.g. individual values can be nan, no data, lons/lats not in order
    lats = f[f"{beam}/{geometry.lat.SDS_Name}"][:]
    lons = f[f"{beam}/{geometry.lon.SDS_Name}"][:]
    return np.where(
        (lons >= tile.minx)
        & (lons < tile.maxx)
        & (lats > tile.miny)
        & (lats <= tile.maxy)
    )


def download_granule_file(
    rfs: s3_utils.DaacFS, s3url: str, local_path: str, retry_count: int = 3
):
    """Download a granule file, retrying timeouts with backoff and other
    failures (e.g. expired credentials) with fresh credentials."""
    try:
        rfs.get_fs(s3url).get_file(s3url, local_path)
    except (ReadTimeoutError, ConnectTimeoutError) as e:
        if retry_count <= 0:
            raise
        wait = 4 ** (3 - retry_count)  # 4s, 16s, 64s backoff
        logger.warning(
            "Timeout reading %s, retrying in %ds (%d attempts left): %s",
            s3url, wait, retry_count, e,
        )
        time.sleep(wait)
        download_granule_file(rfs, s3url, local_path, retry_count - 1)
    except Exception as e:
        if retry_count <= 0:
            raise
        logger.warning(
            "Reading %s failed, retrying with fresh credentials: %r", s3url, e
        )
        rfs.refresh(s3url)
        download_granule_file(rfs, s3url, local_path, retry_count - 1)


def _read_dataset(hdf5, path: str, column: Column, idxs) -> np.ndarray:
    """Read a dataset's in-tile values, checking its type against the
    schema. Byte strings are decoded."""
    d = hdf5[path][idxs]
    if column.dtype == "str":
        if d.dtype.kind != "S":
            raise TypeError(f"{path} is {d.dtype}, schema says str")
        return d.astype(str)
    if d.dtype != column.dtype:
        raise TypeError(f"{path} is {d.dtype}, schema says {column.dtype}")
    if column.is_profile and d.shape[1:] != (column.n_bins,):
        raise ValueError(f"{path} has shape {d.shape}, schema says {column.n_bins} bins")
    return d


def load_granule_product(
    rfs: s3_utils.DaacFS,
    s3url: str,
    product: Product,
    tile: Tile,
    work_dir: str,
) -> pd.DataFrame:
    """Load a GEDI HDF5 file and return a flattened dataframe of the
    product's in-tile shots, indexed by shot number.
    Args:
        s3url: S3 URL to the GEDI HDF5 file.
        work_dir: Local directory the file is downloaded into, and
            deleted from once read.
    """
    extra = [product.primary_key, product.geometry.lat, product.geometry.lon]
    local_path = os.path.join(work_dir, s3url.rsplit("/", 1)[1])
    try:
        # Download first: h5py reads straight from S3 cost ~2 s per
        # dataset, against seconds for the whole file.
        download_granule_file(rfs, s3url, local_path)
        with h5py.File(local_path, "r") as hdf5:
            full_df = []
            for k in hdf5.keys():
                if not k.startswith("BEAM"):
                    continue
                idxs = _get_indices_in_tile(hdf5, k, product.geometry, tile)
                n = len(idxs[0])
                dfs = {}
                for v in product.variables + extra:
                    path = f"{k}/{v.SDS_Name}"
                    if v.SDS_Name in MISSING_SDS and path not in hdf5:
                        dfs[v.variable] = pd.array(
                            [pd.NA] * n, dtype=NULLABLE_DTYPES[v.dtype]
                        )
                        continue
                    d = _read_dataset(hdf5, path, v, idxs)
                    dfs[v.variable] = profile_series(d) if v.is_profile else d
                dfs = pd.DataFrame(dfs)
                dfs["beam_name"] = k
                full_df.append(dfs)
    finally:
        pathlib.Path(local_path).unlink(missing_ok=True)
    full_df = pd.concat(full_df)
    if len(full_df) == 0:
        return pd.DataFrame()  # no tile data in granule
    return full_df.set_index("shot_number")


def profile_series(d: np.ndarray) -> pd.Series:
    """A profile's 2-D values as a column of lists, one per shot, backed by
    one Arrow array."""
    values = pa.FixedSizeListArray.from_arrays(pa.array(d.reshape(-1)), d.shape[1])
    return pd.Series(pd.arrays.ArrowExtensionArray(values))


def null_series(column: Column, index: pd.Index) -> pd.Series:
    """An all-null column of the column's type."""
    if column.is_profile:
        element = pa.from_numpy_dtype(np.dtype(column.dtype))
        values = pa.nulls(len(index), pa.list_(element, column.n_bins))
        return pd.Series(pd.arrays.ArrowExtensionArray(values), index=index)
    return pd.Series(pd.NA, index=index, dtype=NULLABLE_DTYPES[column.dtype])


def part_columns() -> List[Column]:
    """The columns of a checkpoint part, in the order the output keeps
    them. The geometry and grid columns are added when the tile is
    written."""
    first, *rest = SCHEMA.products
    geometry = first.geometry
    cols = [first.primary_key, *first.variables, geometry.lat, geometry.lon]
    cols.append(Column(variable="beam_name", SDS_Name="", dtype="str"))
    for product in rest:
        cols += product.variables
    cols.append(Column(variable="granule", SDS_Name="", dtype="str"))
    cols += [
        Column(variable=f"root_file_{p.product_level.name.lower()}", SDS_Name="", dtype="str")
        for p in SCHEMA.products
    ]
    cols.append(Column(variable="absolute_time", SDS_Name="", dtype="datetime64[ns, UTC]"))
    return cols


def load_granule(
    rfs: s3_utils.DaacFS,
    granule: str,
    product_files: List[Tuple[Product, str]],
    tile: Tile,
    qf: bool,
    work_dir: str,
) -> gpd.GeoDataFrame:
    """Load dataframes for all products and join into a single geodataframe.
    Args:
        granule: Granule name (e.g. OrbitID_GranuleID)
        product_files: List of tuples of the form (product, s3url). A
            null s3url (None/NaN) marks the product as missing for this
            granule: the file is not read, and its schema-expanded
            columns are null instead.
        qf: Keep only shots with l2a_quality_flag_rel3_l2a == 1.
        work_dir: Local directory for downloaded granule files.
    """
    available: List[Tuple[Product, str]] = []
    missing: List[Product] = []
    for product_schema, s3url in product_files:
        if s3url is None or pd.isna(s3url):
            missing.append(product_schema)
        else:
            available.append((product_schema, s3url))

    if not available:
        logger.warning("No product URLs available for granule %s", granule)
        return pd.DataFrame({})

    if missing:
        logger.info(
            "Granule %s missing %d product(s): %s",
            granule,
            len(missing),
            [p.product_level.value for p in missing],
        )

    dfs = []
    for product_schema, s3url in available:
        logger.debug(
            "Reading product %s from %s", product_schema.product_level, s3url
        )
        df = load_granule_product(rfs, s3url, product_schema, tile, work_dir)
        if len(df) == 0:
            return pd.DataFrame({})
        dfs.append(df)
    full_df = dfs[0]
    for df in dfs[1:]:
        # expected repeated cols -- keep from first product only
        df.drop(
            columns=["beam_name", "lon_lowestmode", "lat_lowestmode"],
            inplace=True,
        )
        full_df = full_df.join(df, how="inner")
    log_memory(logger, "load_granule after join")

    # Null-fill columns for products with null URLs in the metadata.
    for product_schema in missing:
        for column in product_schema.variables:
            full_df[column.variable] = null_series(column, full_df.index)

    # Add derived data columns
    full_df["granule"] = granule
    # Source file name per product, null where the product is missing.
    # The string dtype keeps an all-null column VARCHAR in the output.
    for product_schema, s3url in product_files:
        col = f"root_file_{product_schema.product_level.name.lower()}"
        name = None if s3url is None or pd.isna(s3url) else s3url.rsplit("/", 1)[1]
        full_df[col] = pd.Series(name, index=full_df.index, dtype="string")
    gedi_count_start = pd.to_datetime("2018-01-01T00:00:00Z")
    full_df["absolute_time"] = gedi_count_start + pd.to_timedelta(
        full_df["delta_time_l2a"], "seconds"
    )
    if qf:
        full_df = full_df[full_df["l2a_quality_flag_rel3_l2a"] == 1]
    # make shot_number a column now that the join is finished
    full_df.reset_index(inplace=True)
    return full_df


def load_tile_metadata(con, tile_id: str, bucket: str, prefix: str):
    """Load metadata for a specific tile from S3.
    Args:
        tile_id: Tile ID to load (e.g. N00W000)
        bucket: S3 bucket where the metadata is stored.
        prefix: S3 prefix (folder) where the metadata is stored.
    Returns:
        DataFrame with one row per granule covering the tile.
    """
    md_spec = ducky.metadata_spec(bucket, prefix, tile_id)
    columns = ", ".join(GRANULE_COLUMNS)
    return con.execute(
        f"SELECT {columns} FROM read_parquet('{md_spec}')"
    ).df()


def select_granules_for_year(granules: pd.DataFrame, year: int) -> pd.DataFrame:
    """Granules whose acquisition window overlaps the given year.

    A granule spanning New Year is selected by both adjacent years; each
    job writes only the footprints belonging to its own year.
    """
    start = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
    end = pd.Timestamp(year=year + 1, month=1, day=1, tz="UTC")
    return granules[
        (granules["time_start"] < end) & (granules["time_end"] >= start)
    ]


def log_memory(logger, message=""):
    """Log the current memory usage."""
    mem_usage_gb = psutil.Process().memory_info().rss / 1024**3
    logger.info(f"Current memory usage: {mem_usage_gb:.2f} GB {message}")


def write_part(con, df: pd.DataFrame, path: str):
    """Write a batch's shots to a local parquet part, with the columns in
    output order and cast to their schema types, so that every part of
    every tile-year has the same parquet schema."""
    columns = part_columns()
    names = [c.variable for c in columns]
    if set(df.columns) != set(names):
        raise ValueError(
            f"Batch columns differ from the schema: missing "
            f"{sorted(set(names) - set(df.columns))}, extra "
            f"{sorted(set(df.columns) - set(names))}"
        )
    for c in columns:
        actual = df[c.variable].dtype
        if c.is_profile:
            element = pa.from_numpy_dtype(np.dtype(c.dtype))
            ok = actual == pd.ArrowDtype(pa.list_(element, c.n_bins))
        else:
            allowed = {c.dtype, NULLABLE_DTYPES.get(c.dtype)}
            if c.dtype == "str":
                allowed |= {"object"}
            ok = str(actual) in allowed
        if not ok:
            raise TypeError(f"Column {c.variable} is {actual}, schema says {c.dtype}")
    select = ", ".join(
        f'CAST("{c.variable}" AS {c.duckdb_type}) AS "{c.variable}"' for c in columns
    )
    part = pa.Table.from_pandas(df[names], preserve_index=False)
    con.register("part_table", part)
    con.sql(f"""
        COPY (SELECT {select} FROM part_table) TO '{path}' (
            FORMAT parquet,
            COMPRESSION zstd,
            ROW_GROUP_SIZE {STAGE_ROW_GROUP_SIZE}
        );
    """)
    con.unregister("part_table")


def write_tile(con, parts: List[str], tile: Tile, work_dir: str) -> str:
    """Add the geometry and grid columns to the parts' rows and write the
    tile-year as Hilbert-ordered GeoParquet to a local file, returning its
    path. The parts are deleted once read.

    Sorts into a local file first, then copies that into the output. In one
    query the sort and the output's 200k-row group buffer run out of memory
    together on mid-sized tile-years (~340k-520k shots); apart, each fits.
    """
    tile_bounds = (
        f"ST_MakeBox2D(ST_Point({tile.minx}, {tile.miny}), "
        f"ST_Point({tile.maxx}, {tile.maxy}))"
    )
    con.execute("INSTALL h3 FROM community;")
    con.load_extension("h3")
    part_list = ", ".join(f"'{p}'" for p in parts)
    sorted_path = os.path.join(work_dir, "sorted.parquet")
    con.sql(f"""--sql
        COPY (
            SELECT *,
                -- OGC:CRS84 is WGS 84 in lon/lat order, which DuckDB,
                -- GeoParquet and GDAL all agree on (EPSG:4326 is lat/lon
                -- in DuckDB unless geometry_always_xy is set).
                ST_Point(lon_lowestmode, lat_lowestmode)::GEOMETRY('OGC:CRS84') AS geometry,
                ST_Transform(geometry, 'EPSG:6933') AS geometry_6933,
                FLOOR((ST_X(geometry_6933) - {EASE_X_ORIGIN}) / ({EASE_X_SCALE * 72}))::SMALLINT AS ease_72km_x,
                FLOOR(({EASE_Y_ORIGIN} - ST_Y(geometry_6933)) / ({EASE_Y_SCALE * 72}))::SMALLINT AS ease_72km_y,
                h3_latlng_to_cell(lat_lowestmode, lon_lowestmode, 12) AS h3_12,
                h3_latlng_to_cell(lat_lowestmode, lon_lowestmode, 3) AS h3_03
            FROM read_parquet([{part_list}])
            ORDER BY ST_Hilbert(geometry, {tile_bounds})
        ) TO '{sorted_path}' (
            FORMAT parquet,
            COMPRESSION zstd,
            ROW_GROUP_SIZE {STAGE_ROW_GROUP_SIZE}
        );
    """)
    for part in parts:
        pathlib.Path(part).unlink()

    # Keep the sorted order through the copy.
    con.execute("SET preserve_insertion_order = true;")
    output_path = os.path.join(work_dir, "output.parquet")
    con.sql(f"""--sql
        COPY (
            SELECT * FROM read_parquet('{sorted_path}')
        ) TO '{output_path}' ({TILE_COPY_OPTIONS});
    """)
    pathlib.Path(sorted_path).unlink()
    return output_path


def run_main(args: argparse.Namespace):
    """Main function to create a tile. Local scratch files (granule
    downloads, checkpoint parts, DuckDB spill, the output before upload)
    live in a directory under the working directory, removed when the job
    ends. None is under the DPS output directory."""
    with tempfile.TemporaryDirectory(dir=".", prefix="gtiler_") as work_dir:
        return build_tile(args, work_dir)


def build_tile(args: argparse.Namespace, work_dir: str):
    t1 = time.time()
    commit = subprocess.check_output(["git", "-C", os.path.dirname(__file__), "rev-parse", "HEAD"], text=True)
    logger.info("Running commit %s", commit.strip())

    con = ducky.init_duckdb(temp_dir=os.path.join(work_dir, "duckdb"))
    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}';")
    con.execute(f"SET max_temp_directory_size = '{DUCKDB_MAX_TEMP}';")
    con.execute(f"SET threads = {DUCKDB_THREADS};")

    logger.info("Reading metadata and checkpoints for tile ...")
    tile_metadata = load_tile_metadata(con, args.tile_id, args.bucket, args.prefix)
    granules = select_granules_for_year(tile_metadata, args.year)
    quality_filter = bool(tile_metadata["quality_filter"].iloc[0])
    logger.info(
        "%d of the tile's %d granules overlap %d.",
        len(granules),
        len(tile_metadata),
        args.year,
    )
    if args.test:
        granules = granules.head(2)
        logger.info("Testing mode: using %d granules.", len(granules))

    partition = (
        f"{args.prefix}/data/{ducky.TILE_ID}={args.tile_id}/"
        f"{ducky.YEAR}={args.year}/"
    )
    output_key = f"{partition}data_0.parquet"
    marker_key = f"{partition}{ducky.EMPTY_MARKER}"
    checkpointer = checkpoint_lib.Checkpointer(
        args.bucket,
        args.prefix,
        args.tile_id,
        args.year,
        generation=args.generation,
    )
    manifest = checkpointer.initialize(
        granules["granule_key"].tolist(), [output_key, marker_key]
    )
    if manifest.state == checkpoint_lib.DONE:
        logger.info("Tile-year already built; nothing to do.")
        return 0
    parts = checkpointer.download_parts(work_dir)
    remaining = (
        granules.set_index("granule_key").loc[manifest.remaining].reset_index()
    )

    t2 = time.time()
    logger.info("Resuming from %d checkpoint parts.", len(parts))
    logger.info("Planning to process %d new granules.", len(remaining))
    logger.info("Loading metadata and checkpoints took %.1f seconds.", t2 - t1)
    logger.info("Quality filtering is %s.", "on" if quality_filter else "off")

    # Set up access to the ORNL and LP DAACs
    rfs = s3_utils.DaacFS()

    batch_size = args.checkpoint_interval
    for i in range(0, len(remaining), batch_size):
        batch = remaining.iloc[i : i + batch_size]
        dfs = []
        for row in batch.itertuples():
            logger.info("Loading granule %s ...", row.granule_key)
            df = load_granule(
                rfs=rfs,
                granule=row.granule_key,
                product_files=[
                    (SCHEMA.products[0], row.level2A_url),
                    (SCHEMA.products[1], row.level2B_url),
                    (SCHEMA.products[2], row.level4A_url),
                    (SCHEMA.products[3], row.level4C_url),
                ],
                tile=args.tile,
                qf=quality_filter,
                work_dir=work_dir,
            )
            if len(df):
                # Granules spanning New Year also carry the adjacent
                # year's shots.
                df = df[df["absolute_time"].dt.year == args.year]
            logger.info(f"Loaded {len(df)} shots in granule {row.granule_key}")
            if len(df):
                dfs.append(df)
        # Only this batch's shots are held in memory; earlier batches are
        # in their parts.
        part = None
        if dfs:
            part = os.path.join(work_dir, f"batch-{i // batch_size:04d}.parquet")
            write_part(con, pd.concat(dfs), part)
            parts.append(part)
        del dfs
        log_memory(logger, "after processing batch")
        checkpointer.add_batch(
            part, remaining["granule_key"].iloc[i + batch_size :].tolist()
        )
    t3 = time.time()
    logger.info("Loading granules took %.1f seconds.", t3 - t2)

    if not parts:
        logger.info("No footprints to write. Marking empty: %s", marker_key)
        checkpointer.commit(None, marker_key)
        return 0

    output = write_tile(con, parts, args.tile, work_dir)
    t4 = time.time()
    logger.info("Writing parquet took %.1f seconds.", t4 - t3)
    checkpointer.commit(output, output_key)
    logger.info("Total time: %.1f seconds.", time.time() - t1)

    return 0


if __name__ == "__main__":
    args = get_cmd_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )
    args = check_args(args)
    run_main(args)
