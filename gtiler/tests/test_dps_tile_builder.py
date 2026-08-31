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
# Acquisition windows covering each fixture granule's shots, in the form
# CMR reports them. The two granules fall in different years.
GRANULE_WINDOWS = {
    "O15709_01": ("2021-09-20T18:00:00Z", "2021-09-20T19:33:00Z"),
    "O20346_01": ("2022-07-16T20:00:00Z", "2022-07-16T21:33:00Z"),
}
GRANULE_YEARS = {"O15709_01": 2021, "O20346_01": 2022}


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
    """Fixture metadata with quality filtering off, as tile_runner would
    record it for a --no-quality build. The stored granule URLs are
    absolute paths from the machine that built the fixtures, so resolve
    them against this checkout's granule directory."""
    path = FIXTURES / f"metadata/tile_id={TILE_ID}/data_0.parquet"
    md = gpd.read_file(path)
    md["quality_filter"] = False
    windows = md["granule_key"].map(GRANULE_WINDOWS)
    md["time_start"] = pd.to_datetime([w[0] for w in windows], utc=True)
    md["time_end"] = pd.to_datetime([w[1] for w in windows], utc=True)
    for col in [c for c in md.columns if c.endswith("_url")]:
        md[col] = md[col].map(
            lambda u: str(FIXTURES / "granules" / u.rsplit("/", 1)[1])
            if pd.notna(u)
            else u
        )
    return md


@pytest.fixture
def args(tmp_path):
    return argparse.Namespace(
        bucket="test-bucket",
        prefix="test/prefix",
        tile_id=TILE_ID,
        tile=Tile(TILE_ID),
        year=GRANULE_YEARS["O15709_01"],
        generation=0,
        checkpoint_interval=30,
        test=False,
    )


@pytest.fixture
def run_pipeline_factory(dps_tile_builder, args, tmp_path):
    """Returns a callable: run_pipeline_factory(metadata, subdir=None) -> output_dir.

    Patches:
      - load_tile_metadata → return the given metadata GeoDataFrame
      - s3_utils.RefreshableFSSpec → local fsspec filesystem so the
        fixture's file:// granule URLs are read from disk, not S3
      - ducky.data_prefix → local tmp_path (DuckDB COPY writes to disk)
      - checkpoint_lib.Checkpointer → no-op (bypasses S3 checkpoint state)

    Pass `subdir` to isolate two runs in the same test (e.g. to compare
    schemas across tiles); without it, the output lands directly under
    `tmp_path` so single-run tests don't need a subdir parameter.
    """

    def _run(metadata, subdir=None):
        out_dir = tmp_path / subdir if subdir else tmp_path
        out_dir.mkdir(parents=True, exist_ok=True)
        local_prefix = str(out_dir) + "/"
        with patch.object(
            dps_tile_builder, "load_tile_metadata", return_value=metadata
        ), patch.object(
            dps_tile_builder.s3_utils, "RefreshableFSSpec", _LocalFSSpec
        ), patch.object(
            dps_tile_builder.ducky, "data_prefix", return_value=local_prefix
        ), patch.object(
            dps_tile_builder.checkpoint_lib, "Checkpointer", _NullCheckpointer
        ):
            dps_tile_builder.run_main(args)
        return out_dir

    return _run


@pytest.fixture
def run_pipeline(run_pipeline_factory, fixture_metadata):
    """Default pipeline run against the unmodified fixture metadata."""
    return run_pipeline_factory(fixture_metadata)


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


def _read_output(out_dir: pathlib.Path) -> pd.DataFrame:
    """Concat every output parquet file into a single DataFrame.
    PARTITION_BY strips tile_id and year from the parquet bodies — they
    live only in directory names and aren't needed here."""
    files = list((out_dir / f"tile_id={TILE_ID}").glob("year=*/*.parquet"))
    assert files, f"no output parquet files under {out_dir}"
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


class TestMissingProductUrl:
    """When a granule's metadata row has a null product URL, the pipeline
    still produces the tile but with NaN-filled columns for that product.
    """

    @pytest.fixture
    def metadata_missing_l4c(self, fixture_metadata):
        """First granule has no L4C URL; second granule is unchanged."""
        md = fixture_metadata.copy()
        md.loc[md.index[0], "level4C_url"] = None
        return md

    @pytest.fixture
    def out_dir(self, run_pipeline_factory, metadata_missing_l4c):
        return run_pipeline_factory(metadata_missing_l4c)

    def test_tile_still_produced(self, out_dir):
        assert (out_dir / f"tile_id={TILE_ID}").is_dir()
        files = list((out_dir / f"tile_id={TILE_ID}").glob("year=*/*.parquet"))
        assert files, "expected parquet output despite missing L4C URL"

    def test_only_the_years_granule_is_present(self, out_dir):
        df = _read_output(out_dir)
        assert set(df["granule"].unique()) == {"O15709_01"}

    def test_l4c_columns_nan_for_the_missing_granule(self, out_dir):
        df = _read_output(out_dir)
        # Spot-check one scalar and one quality-flag column from L4C.
        for col in ("wsci", "wsci_quality_flag"):
            assert df[col].isna().all(), (
                f"{col} should be all-NaN for the granule with no L4C URL"
            )

    def test_l4c_columns_present_for_the_unaffected_granule(
        self, args, run_pipeline_factory, metadata_missing_l4c
    ):
        # The other granule keeps its L4C URL, in the following year.
        args.year = GRANULE_YEARS["O20346_01"]
        df = _read_output(run_pipeline_factory(metadata_missing_l4c))
        assert set(df["granule"].unique()) == {"O20346_01"}
        for col in ("wsci", "wsci_quality_flag"):
            assert df[col].notna().any(), (
                f"{col} should have real values for the unaffected granule"
            )

    def test_l2a_columns_unaffected(self, out_dir):
        # L2A is present regardless, so its columns have real values.
        df = _read_output(out_dir)
        for col in ("elev_lowestmode", "shot_number", "lat_lowestmode"):
            assert df[col].notna().all(), f"{col} should have no NaNs"

    def test_outputs_with_and_without_null_url_share_schema(
        self, run_pipeline_factory, fixture_metadata, metadata_missing_l4c
    ):
        """Outputs from a tile with a null product URL and a tile without
        one must be parquet-schema-compatible: a single DuckDB read
        across both must succeed and return the union of their rows."""
        out_full = run_pipeline_factory(fixture_metadata, subdir="full")
        out_missing = run_pipeline_factory(
            metadata_missing_l4c, subdir="missing"
        )

        con = duckdb.connect()
        df = con.sql(f"""
            SELECT * FROM read_parquet([
                '{out_full}/tile_id={TILE_ID}/year=*/*.parquet',
                '{out_missing}/tile_id={TILE_ID}/year=*/*.parquet'
            ])
        """).df()

        n_full = len(_read_output(out_full))
        n_missing = len(_read_output(out_missing))
        assert len(df) == n_full + n_missing, (
            f"single-read row count {len(df)} != "
            f"sum of per-tile counts ({n_full} + {n_missing})"
        )

        # Spot-check that columns from every product survived the unified
        # read — including L4C, which is NaN-filled in one of the two
        # inputs.
        for col in (
            "elev_lowestmode",  # L2A
            "cover",            # L2B
            "agbd",             # L4A
            "wsci",             # L4C
            "wsci_quality_flag",
            "granule",
        ):
            assert col in df.columns, f"{col} missing from unified read"


class TestQualityFilter:
    """Quality filtering is driven solely by the metadata's quality_filter
    column, which tile_runner sets per tile.

    The fixture h5 files have the first 5 shots/beam patched to fail every
    QF criterion (see build_granule_fixtures.QUALITY_PATTERN) and the rest
    patched to pass, so the QF behavior shows up as low-quality survivors
    in the output.
    """

    @pytest.fixture
    def metadata_qf_on(self, fixture_metadata):
        md = fixture_metadata.copy()
        md["quality_filter"] = True
        return md

    def test_low_quality_shots_dropped_when_enabled(
        self, run_pipeline_factory, metadata_qf_on
    ):
        df = _read_output(run_pipeline_factory(metadata_qf_on))
        assert int((df["quality_flag"] == 0).sum()) == 0, (
            "low-quality footprints should be filtered out when metadata "
            "enables quality filtering"
        )
        assert len(df) > 0, (
            "expected the high-quality footprints (quality_flag == 1) to "
            "survive QF"
        )

    def test_low_quality_shots_kept_when_disabled(
        self, run_pipeline_factory, fixture_metadata
    ):
        df = _read_output(run_pipeline_factory(fixture_metadata))
        assert int((df["quality_flag"] == 0).sum()) > 0, (
            "low-quality footprints should survive when metadata disables "
            "quality filtering"
        )

    def test_applies_when_a_product_url_is_missing(
        self, run_pipeline_factory, metadata_qf_on
    ):
        # Every QF criterion comes from L2A, so a missing L4C URL must not
        # change which shots the filter keeps.
        md = metadata_qf_on.copy()
        md.loc[md.index[0], "level4C_url"] = None
        df = _read_output(run_pipeline_factory(md))
        assert int((df["quality_flag"] == 0).sum()) == 0
        assert len(df) > 0
        assert df["wsci"].isna().any(), (
            "expected NaN-filled L4C columns for the granule with no L4C URL"
        )


class TestEmptyTile:
    """A tile whose granules contribute no footprints writes a marker
    instead of a partition, so it is not planned again."""

    @pytest.fixture
    def empty_tile_args(self, args):
        # A tile the fixture granules do not intersect.
        args.tile_id = "N80_E000"
        args.tile = Tile(args.tile_id)
        return args

    def test_writes_marker_and_no_parquet(
        self, empty_tile_args, run_pipeline_factory, fixture_metadata
    ):
        out = run_pipeline_factory(fixture_metadata)
        marker = (
            out
            / f"tile_id={empty_tile_args.tile_id}"
            / f"year={empty_tile_args.year}"
            / "_EMPTY"
        )
        assert marker.is_file(), f"expected an empty marker at {marker}"
        assert marker.stat().st_size == 0
        assert not list(out.glob("**/*.parquet")), (
            "an empty tile should not write a parquet partition"
        )


class TestYearSelection:
    """Each job writes one year. A granule is read by the job for every
    year its acquisition window overlaps, but contributes only the
    footprints acquired in that job's year."""

    def test_writes_only_the_requested_year(
        self, args, run_pipeline_factory, fixture_metadata
    ):
        args.year = GRANULE_YEARS["O20346_01"]
        out = run_pipeline_factory(fixture_metadata)
        df = _read_output(out)
        assert set(df["granule"].unique()) == {"O20346_01"}
        assert set(df["absolute_time"].dt.year) == {args.year}

    def test_partition_is_the_requested_year(
        self, args, run_pipeline_factory, fixture_metadata
    ):
        args.year = GRANULE_YEARS["O20346_01"]
        out = run_pipeline_factory(fixture_metadata)
        years = [d.name for d in (out / f"tile_id={TILE_ID}").glob("year=*")]
        assert years == [f"year={args.year}"]

    def test_year_with_no_granules_marks_empty(
        self, args, run_pipeline_factory, fixture_metadata
    ):
        args.year = 2019
        out = run_pipeline_factory(fixture_metadata)
        assert (out / f"tile_id={TILE_ID}" / "year=2019" / "_EMPTY").is_file()
        assert not list(out.glob("**/*.parquet"))


class TestSelectGranulesForYear:
    def _granules(self, starts, ends):
        return pd.DataFrame({
            "granule_key": [f"g{i}" for i in range(len(starts))],
            "time_start": pd.to_datetime(starts, utc=True),
            "time_end": pd.to_datetime(ends, utc=True),
        })

    def test_selects_overlapping_granules(self, dps_tile_builder):
        g = self._granules(
            ["2020-06-01T00:00:00Z", "2021-06-01T00:00:00Z"],
            ["2020-06-01T01:33:00Z", "2021-06-01T01:33:00Z"],
        )
        got = dps_tile_builder.select_granules_for_year(g, 2021)
        assert list(got["granule_key"]) == ["g1"]

    def test_granule_spanning_new_year_is_selected_by_both_years(
        self, dps_tile_builder
    ):
        g = self._granules(
            ["2023-12-31T23:30:00Z"], ["2024-01-01T01:03:00Z"]
        )
        for year in (2023, 2024):
            got = dps_tile_builder.select_granules_for_year(g, year)
            assert list(got["granule_key"]) == ["g0"], year
