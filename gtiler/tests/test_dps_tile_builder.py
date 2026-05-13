"""Integration tests for scripts/dps_tile_builder.py.

These tests run the full pipeline locally: real h5py parsing, beam
iteration, in-tile filtering, per-product joins, derived columns, and
DuckDB parquet write/partitioning — but against mini HDF5 fixtures (a
few in-tile shots per beam, only the SDS paths the schema references)
rather than full GEDI granules from S3. The fixtures are built by
`fixtures/build_granule_fixtures.py`; rerun that script if the schema
changes meaningfully.

Run with:
    conda run -n pyduck python -m pytest gtiler/tests/test_dps_tile_builder.py -v
"""

import argparse
import importlib.util
import pathlib
import sys

import duckdb
import fsspec
import geopandas as gpd
import pandas as pd
import pytest
from unittest.mock import patch

from gtiler.database.tiles import Tile


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
FIXTURES = pathlib.Path(__file__).parent / "fixtures"
TILE_ID = "N00_W050"


def _import_dps_tile_builder():
    """Load scripts/dps_tile_builder.py as a module (it's not a package)."""
    path = REPO_ROOT / "scripts" / "dps_tile_builder.py"
    spec = importlib.util.spec_from_file_location("dps_tile_builder", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["dps_tile_builder"] = module
    spec.loader.exec_module(module)
    return module


class _NullCheckpointer:
    """Stub Checkpointer that bypasses S3 — no prior state, writes are no-ops."""

    def __init__(self, *args, **kwargs):
        pass

    def initialize(self):
        return None

    def write_checkpoint(self, *args, **kwargs):
        pass


class _LocalFSSpec:
    """Stub RefreshableFSSpec that hands out a local fsspec filesystem.

    The fixture metadata's level*_url columns point at file:// paths, so
    load_granule_product's `rfs.get_fs().open(...)` reads the mini HDF5
    fixtures from disk instead of S3.
    """

    def __init__(self, *args, **kwargs):
        self._fs = fsspec.filesystem("file")

    def get_fs(self):
        return self._fs

    def refresh(self):
        pass


@pytest.fixture(scope="module")
def dps_tile_builder():
    return _import_dps_tile_builder()


@pytest.fixture
def fixture_metadata():
    path = FIXTURES / f"metadata/tile_id={TILE_ID}/data_0.parquet"
    return gpd.read_file(path)


@pytest.fixture
def args(tmp_path):
    return argparse.Namespace(
        bucket="test-bucket",
        prefix="test/prefix",
        tile_id=TILE_ID,
        tile=Tile(TILE_ID),
        generation=0,
        checkpoint_interval=30,
        test=False,
        quality=False,
    )


@pytest.fixture
def run_pipeline(dps_tile_builder, args, fixture_metadata, tmp_path):
    """Execute run_main against the fixture metadata, writing to tmp_path.

    Patches:
      - load_tile_metadata → return the fixture GeoDataFrame directly
        (avoids the s3://bucket/prefix/metadata/... path construction)
      - s3_utils.RefreshableFSSpec → local fsspec filesystem so the
        fixture's file:// granule URLs are read from disk, not S3
      - ducky.data_prefix → local tmp_path (DuckDB COPY writes to disk)
      - checkpoint_lib.Checkpointer → no-op (bypasses S3 checkpoint state)
    """
    local_prefix = str(tmp_path) + "/"
    with patch.object(
        dps_tile_builder, "load_tile_metadata", return_value=fixture_metadata
    ), patch.object(
        dps_tile_builder.s3_utils, "RefreshableFSSpec", _LocalFSSpec
    ), patch.object(
        dps_tile_builder.ducky, "data_prefix", return_value=local_prefix
    ), patch.object(
        dps_tile_builder.checkpoint_lib, "Checkpointer", _NullCheckpointer
    ):
        dps_tile_builder.run_main(args)
    return pathlib.Path(local_prefix)


def _parquet_glob(out_dir: pathlib.Path) -> str:
    return f"{out_dir}/tile_id={TILE_ID}/year=*/*.parquet"


class TestRunMain:
    def test_writes_partitioned_parquet_layout(self, run_pipeline):
        tile_dir = run_pipeline / f"tile_id={TILE_ID}"
        assert tile_dir.is_dir(), f"expected {tile_dir} to exist"
        year_dirs = sorted(tile_dir.glob("year=*"))
        assert year_dirs, "expected at least one year=* partition"
        for yd in year_dirs:
            files = list(yd.glob("*.parquet"))
            assert files, f"no parquet files in {yd}"

    def test_output_is_nonempty(self, run_pipeline):
        con = duckdb.connect()
        (n,) = con.sql(
            f"SELECT count(*) FROM '{_parquet_glob(run_pipeline)}'"
        ).fetchone()
        assert n > 0, "pipeline produced no shots for the fixture"

    def test_all_shots_lie_in_tile_bounds(self, run_pipeline):
        tile = Tile(TILE_ID)
        con = duckdb.connect()
        mnx, mxx, mny, mxy = con.sql(f"""
            SELECT min(lon_lowestmode), max(lon_lowestmode),
                   min(lat_lowestmode), max(lat_lowestmode)
            FROM '{_parquet_glob(run_pipeline)}'
        """).fetchone()
        # Mirrors the half-open box used in _get_indices_in_tile.
        assert tile.minx <= mnx and mxx < tile.maxx
        assert tile.miny < mny and mxy <= tile.maxy

    def test_tile_id_partition_value(self, run_pipeline):
        con = duckdb.connect()
        rows = con.sql(f"""
            SELECT DISTINCT tile_id FROM '{_parquet_glob(run_pipeline)}'
        """).fetchall()
        assert rows == [(TILE_ID,)]

    def test_year_partition_matches_absolute_time(self, run_pipeline):
        con = duckdb.connect()
        bad = con.sql(f"""
            SELECT count(*) FROM '{_parquet_glob(run_pipeline)}'
            WHERE date_part('year', absolute_time) <> year
        """).fetchone()[0]
        assert bad == 0

    def test_output_has_expected_columns(self, run_pipeline):
        first = next(
            (run_pipeline / f"tile_id={TILE_ID}").glob("year=*/*.parquet")
        )
        cols = set(pd.read_parquet(first).columns)
        # PARTITION_BY (tile_id, year) strips those two from the parquet
        # body and stores them in the directory names. Spot-check one
        # column from each product + every non-partition derived column.
        expected = {
            "shot_number",
            "lat_lowestmode",
            "lon_lowestmode",
            "elev_lowestmode",   # L2A
            "cover",             # L2B
            "agbd",              # L4A
            "wsci",              # L4C
            "granule",           # derived
            "absolute_time",     # derived
            "beam_name",         # derived
            "geometry",          # derived (added in run_main SQL)
        }
        missing = expected - cols
        assert not missing, f"missing expected columns: {missing}"

    def test_granule_column_matches_fixture_keys(
        self, run_pipeline, fixture_metadata
    ):
        con = duckdb.connect()
        rows = con.sql(f"""
            SELECT DISTINCT granule FROM '{_parquet_glob(run_pipeline)}'
        """).fetchall()
        granules = {r[0] for r in rows}
        expected = set(fixture_metadata["granule_key"])
        assert granules <= expected, (
            f"unexpected granule keys in output: {granules - expected}"
        )
        assert granules, "no granule values in output"
