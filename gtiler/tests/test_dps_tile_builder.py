"""Integration tests for scripts/dps/dps_tile_builder.py.

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
import contextlib
import importlib.util
import pathlib
import shutil
import sys

import boto3
import duckdb
import fsspec
import h5py
import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
from moto import mock_aws
from unittest.mock import patch

from gtiler.common import checkpoint_lib, s3_utils
from gtiler.database.schema_v3 import SCHEMA, SCHEMA_VERSION, VERSION_KEY
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
    """Load scripts/dps/dps_tile_builder.py as a module (it's not a package)."""
    path = REPO_ROOT / "scripts" / "dps" / "dps_tile_builder.py"
    spec = importlib.util.spec_from_file_location("dps_tile_builder", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["dps_tile_builder"] = module
    spec.loader.exec_module(module)
    return module


class _LocalCheckpointer:
    """Stub Checkpointer that bypasses S3: there is no prior progress, and
    commit copies the output to its key's place under out_dir."""

    out_dir = None

    def __init__(self, bucket, prefix, *args, **kwargs):
        self.prefix = prefix

    def initialize(self, granules, output_keys):
        self.manifest = checkpoint_lib.Manifest(0, "test", list(granules))
        return self.manifest

    def download_parts(self, work_dir):
        return []

    def add_batch(self, local_part, remaining):
        self.manifest.remaining = list(remaining)

    def commit(self, local_path, output_key):
        dest = self.out_dir / output_key.removeprefix(f"{self.prefix}/data/")
        dest.parent.mkdir(parents=True, exist_ok=True)
        if local_path is None:
            dest.write_bytes(b"")
        else:
            shutil.copy(local_path, dest)


class _LocalFSSpec:
    """Stub DaacFS that hands out a local fsspec filesystem.

    The fixture metadata's level*_url columns point at local paths, so
    load_granule_product's `rfs.get_fs(url).get_file(...)` copies the mini
    HDF5 fixtures from disk instead of S3.
    """

    def __init__(self, *args, **kwargs):
        self._fs = fsspec.filesystem("file")

    def get_fs(self, s3url):
        return self._fs

    def refresh(self, s3url):
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
      - s3_utils.DaacFS → local fsspec filesystem so the
        fixture's file:// granule URLs are read from disk, not S3
      - checkpoint_lib.Checkpointer → no prior progress; the output is
        copied under the output directory instead of uploaded

    Pass `subdir` to isolate two runs in the same test (e.g. to compare
    schemas across tiles); without it, the output lands directly under
    `tmp_path` so single-run tests don't need a subdir parameter.
    """

    def _run(metadata, subdir=None):
        out_dir = tmp_path / subdir if subdir else tmp_path
        out_dir.mkdir(parents=True, exist_ok=True)
        with patch.object(
            dps_tile_builder, "load_tile_metadata", return_value=metadata
        ), patch.object(
            dps_tile_builder.s3_utils, "DaacFS", _LocalFSSpec
        ), patch.object(
            dps_tile_builder.checkpoint_lib,
            "Checkpointer",
            type("Checkpointer", (_LocalCheckpointer,), {"out_dir": out_dir}),
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
            "elev_lowestmode_l2a",  # L2A
            "cover_l2b",            # L2B
            "agbd_l4a",             # L4A
            "wsci_l4c",             # L4C
            "granule",              # derived
            "absolute_time",        # derived
            "beam_name",            # derived
            "root_file_l2a",        # derived
            "root_file_l2b",
            "root_file_l4a",
            "root_file_l4c",
            # derived in run_main SQL
            "geometry",
            "geometry_6933",
            "ease_72km_x",
            "ease_72km_y",
            "h3_12",
            "h3_03",
        }
        missing = expected - cols
        assert not missing, f"missing expected columns: {missing}"

    def test_geometry_and_grid_column_types(self, run_pipeline):
        con = duckdb.connect()
        con.load_extension("spatial")
        types = dict(
            con.sql(f"""
                SELECT column_name, column_type
                FROM (DESCRIBE SELECT * FROM '{_parquet_glob(run_pipeline)}')
            """).fetchall()
        )
        assert types["geometry"] == "GEOMETRY('OGC:CRS84')"
        assert types["geometry_6933"] == "GEOMETRY('EPSG:6933')"
        assert types["ease_72km_x"] == "SMALLINT"
        assert types["ease_72km_y"] == "SMALLINT"
        assert types["h3_12"] == "BIGINT"
        assert types["h3_03"] == "BIGINT"

    def test_rows_are_in_hilbert_order(self, run_pipeline):
        tile = Tile(TILE_ID)
        con = duckdb.connect()
        con.load_extension("spatial")
        bad = con.sql(f"""
            SELECT count(*) FROM (
                SELECT h < lag(h) OVER (
                    PARTITION BY filename ORDER BY file_row_number
                ) AS out_of_order
                FROM (
                    SELECT filename, file_row_number, ST_Hilbert(
                        geometry,
                        ST_MakeBox2D(
                            ST_Point({tile.minx}, {tile.miny}),
                            ST_Point({tile.maxx}, {tile.maxy})
                        )
                    ) AS h
                    FROM read_parquet(
                        '{_parquet_glob(run_pipeline)}',
                        filename = true, file_row_number = true
                    )
                )
            )
            WHERE out_of_order
        """).fetchone()[0]
        assert bad == 0

    def test_geometry_is_lon_lat(self, run_pipeline):
        con = duckdb.connect()
        con.load_extension("spatial")
        bad = con.sql(f"""
            SELECT count(*) FROM '{_parquet_glob(run_pipeline)}'
            WHERE ST_X(geometry) <> lon_lowestmode
               OR ST_Y(geometry) <> lat_lowestmode
        """).fetchone()[0]
        assert bad == 0

    def test_root_files_match_metadata_urls(
        self, run_pipeline, fixture_metadata
    ):
        df = _read_output(run_pipeline)
        md = fixture_metadata.set_index("granule_key")
        for level in ("2A", "2B", "4A", "4C"):
            expected = df["granule"].map(
                md[f"level{level}_url"].str.rsplit("/", n=1).str[1]
            )
            assert (df[f"root_file_l{level.lower()}"] == expected).all()

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
        for col in ("wsci_l4c", "l4c_quality_flag_rel3_l4c"):
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
        for col in ("wsci_l4c", "l4c_quality_flag_rel3_l4c"):
            assert df[col].notna().any(), (
                f"{col} should have real values for the unaffected granule"
            )

    def test_root_file_null_for_the_missing_product(self, out_dir):
        con = duckdb.connect()
        (n, n_null, typ) = con.sql(f"""
            SELECT count(*), count(*) FILTER (root_file_l4c IS NULL),
                   typeof(any_value(root_file_l4c))
            FROM '{out_dir}/tile_id={TILE_ID}/year=*/*.parquet'
        """).fetchone()
        assert n_null == n > 0
        assert typ == "VARCHAR"

    def test_l2a_columns_unaffected(self, out_dir):
        # L2A is present regardless, so its columns have real values.
        df = _read_output(out_dir)
        for col in ("elev_lowestmode_l2a", "shot_number", "lat_lowestmode"):
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
            "elev_lowestmode_l2a",  # L2A
            "cover_l2b",            # L2B
            "agbd_l4a",             # L4A
            "wsci_l4c",             # L4C
            "l4c_quality_flag_rel3_l4c",
            "granule",
        ):
            assert col in df.columns, f"{col} missing from unified read"


class TestMissingRel3WsciFlags:
    """Some L4C granules have only the rel2 WSCI quality flags. Their shots
    are kept, with the rel3 flags null and the column type unchanged."""

    FLAGS = [
        f"wsci_prediction_l4c_quality_flag_rel3_{a}_l4c"
        for a in ("a1", "a10", "a2", "a5")
    ]

    @pytest.fixture
    def metadata_rel2_only(self, fixture_metadata, tmp_path):
        """First granule's L4C file with the rel3 WSCI flags removed."""
        md = fixture_metadata.copy()
        src = md["level4C_url"].iloc[0]
        dst = tmp_path / "rel2_only" / pathlib.Path(src).name
        dst.parent.mkdir()
        shutil.copy(src, dst)
        with h5py.File(dst, "r+") as f:
            for beam in [k for k in f if k.startswith("BEAM")]:
                for a in ("a1", "a10", "a2", "a5"):
                    del f[f"{beam}/wsci_prediction/l4c_quality_flag_rel3_{a}"]
        md.loc[md.index[0], "level4C_url"] = str(dst)
        return md

    def _schema(self, out_dir):
        return duckdb.sql(f"""
            SELECT name, type, logical_type
            FROM parquet_schema('{_parquet_glob(out_dir)}')
        """).df().set_index("name")

    def test_flags_null_and_other_columns_kept(
        self, run_pipeline_factory, fixture_metadata, metadata_rel2_only
    ):
        full = _read_output(run_pipeline_factory(fixture_metadata, "full"))
        df = _read_output(run_pipeline_factory(metadata_rel2_only, "rel2"))
        assert len(df) == len(full) > 0
        for col in self.FLAGS:
            assert df[col].isna().all(), col
        for col in ("wsci_l4c", "l4c_quality_flag_rel3_l4c"):
            assert df[col].notna().all(), col

    def test_flag_types_match_a_normal_tile(
        self, run_pipeline_factory, fixture_metadata, metadata_rel2_only
    ):
        full = self._schema(run_pipeline_factory(fixture_metadata, "full"))
        rel2 = self._schema(run_pipeline_factory(metadata_rel2_only, "rel2"))
        assert rel2.loc[self.FLAGS].equals(full.loc[self.FLAGS])


class TestNanValuesKept:
    """Shots with NaN in some columns (e.g. an L2B retrieval that failed)
    are kept, with the NaN in place."""

    @pytest.fixture
    def metadata_nan_cover(self, fixture_metadata, tmp_path):
        """First granule's L2B file with cover set to NaN on every shot."""
        md = fixture_metadata.copy()
        src = md["level2B_url"].iloc[0]
        dst = tmp_path / "nan_cover" / pathlib.Path(src).name
        dst.parent.mkdir()
        shutil.copy(src, dst)
        with h5py.File(dst, "r+") as f:
            for beam in [k for k in f if k.startswith("BEAM")]:
                f[f"{beam}/cover"][...] = float("nan")
        md.loc[md.index[0], "level2B_url"] = str(dst)
        return md

    def test_shots_with_nan_are_kept(
        self, run_pipeline_factory, fixture_metadata, metadata_nan_cover
    ):
        full = _read_output(run_pipeline_factory(fixture_metadata, "full"))
        df = _read_output(run_pipeline_factory(metadata_nan_cover, "nan"))
        assert len(df) == len(full) > 0
        assert df["cover_l2b"].isna().all()
        assert df["pai_l2b"].notna().all()


class TestQualityFilter:
    """Quality filtering is driven solely by the metadata's quality_filter
    column, which tile_runner sets per tile, and keeps only shots with
    l2a_quality_flag_rel3_l2a == 1.

    The fixture shots are unmodified V003 data, which has both passing
    and failing shots in each granule.
    """

    QF = "l2a_quality_flag_rel3_l2a"

    @pytest.fixture
    def metadata_qf_on(self, fixture_metadata):
        md = fixture_metadata.copy()
        md["quality_filter"] = True
        return md

    def test_low_quality_shots_dropped_when_enabled(
        self, run_pipeline_factory, metadata_qf_on
    ):
        df = _read_output(run_pipeline_factory(metadata_qf_on))
        assert len(df) > 0
        assert (df[self.QF] == 1).all()

    def test_low_quality_shots_kept_when_disabled(
        self, run_pipeline_factory, fixture_metadata
    ):
        df = _read_output(run_pipeline_factory(fixture_metadata))
        assert (df[self.QF] == 0).any()

    def test_only_the_l2a_flag_is_used(
        self, run_pipeline_factory, fixture_metadata, metadata_qf_on
    ):
        # Shots failing only other criteria (e.g. sensitivity) survive.
        unfiltered = _read_output(
            run_pipeline_factory(fixture_metadata, subdir="off")
        )
        filtered = _read_output(run_pipeline_factory(metadata_qf_on, subdir="on"))
        expected = set(unfiltered.loc[unfiltered[self.QF] == 1, "shot_number"])
        assert set(filtered["shot_number"]) == expected

    def test_applies_when_a_product_url_is_missing(
        self, run_pipeline_factory, metadata_qf_on
    ):
        # The QF column comes from L2A, so a missing L4C URL must not
        # change which shots the filter keeps.
        md = metadata_qf_on.copy()
        md.loc[md.index[0], "level4C_url"] = None
        df = _read_output(run_pipeline_factory(md))
        assert len(df) > 0
        assert (df[self.QF] == 1).all()
        assert df["wsci_l4c"].isna().all()


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


class TestGranuleDownload:
    def test_download_is_removed_after_reading(
        self, dps_tile_builder, fixture_metadata, tmp_path
    ):
        url = fixture_metadata["level2A_url"].iloc[0]
        df = dps_tile_builder.load_granule_product(
            _LocalFSSpec(),
            url,
            dps_tile_builder.SCHEMA.products[0],
            Tile(TILE_ID),
            str(tmp_path),
        )
        assert len(df) > 0
        assert not list(tmp_path.iterdir())


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


def _schema(out_dir):
    return duckdb.sql(f"""
        SELECT name, type, logical_type, converted_type
        FROM parquet_schema('{_parquet_glob(out_dir)}')
    """).df()


class TestOutputTypes:
    """Every column is written as its schema type, whichever products and
    datasets a tile-year's granules have."""

    def test_columns_have_their_schema_types(self, dps_tile_builder, run_pipeline):
        types = {
            r[0]: r[1]
            for r in duckdb.sql(f"""
                DESCRIBE SELECT * FROM read_parquet(
                    '{_parquet_glob(run_pipeline)}', hive_partitioning=false
                )
            """).fetchall()
        }
        for c in dps_tile_builder.part_columns():
            expected = c.duckdb_type
            assert types[c.variable] == {"TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE"}.get(
                expected, expected
            ), c.variable

    def test_missing_product_writes_the_same_schema(
        self, run_pipeline_factory, fixture_metadata
    ):
        md = fixture_metadata.copy()
        md.loc[md.index[0], "level4C_url"] = None
        full = _schema(run_pipeline_factory(fixture_metadata, "full"))
        missing = _schema(run_pipeline_factory(md, "missing"))
        pd.testing.assert_frame_equal(full, missing)


class TestProfiles:
    """Each profile is one list column of its bins, as the schema says."""

    PROFILES = [v for p in SCHEMA.products for v in p.variables if v.is_profile]

    def test_every_list_has_the_schema_bin_count(self, run_pipeline):
        con = duckdb.connect()
        for v in self.PROFILES:
            bad = con.sql(f"""
                SELECT count(*) FROM '{_parquet_glob(run_pipeline)}'
                WHERE len("{v.variable}") != {v.n_bins}
            """).fetchone()[0]
            assert bad == 0, v.variable

    def test_lists_hold_the_granule_values_in_bin_order(
        self, run_pipeline, fixture_metadata
    ):
        con = duckdb.connect()
        shot, beam, granule, rh = con.sql(f"""
            SELECT shot_number, beam_name, granule, rh_l2a
            FROM '{_parquet_glob(run_pipeline)}'
            WHERE list_distinct(rh_l2a) != [rh_l2a[1]]  -- not all one value
            LIMIT 1
        """).fetchone()
        url = fixture_metadata.set_index("granule_key").loc[granule, "level2A_url"]
        with h5py.File(url) as f:
            i = np.flatnonzero(f[f"{beam}/shot_number"][:] == shot)[0]
            expected = f[f"{beam}/rh"][i]
        np.testing.assert_array_equal(np.array(rh, dtype=np.float32), expected)

    def test_missing_product_profiles_are_null_lists(
        self, run_pipeline_factory, fixture_metadata
    ):
        md = fixture_metadata.copy()
        md["level4A_url"] = None
        out = run_pipeline_factory(md)
        n, n_null, typ = duckdb.sql(f"""
            SELECT count(*), count(*) FILTER (xvar_l4a IS NULL), typeof(any_value(xvar_l4a))
            FROM '{_parquet_glob(out)}'
        """).fetchone()
        assert n_null == n > 0
        assert typ == "FLOAT[]"

    def test_file_records_the_schema_version(self, run_pipeline):
        (path,) = (run_pipeline / f"tile_id={TILE_ID}").glob("year=*/*.parquet")
        metadata = pq.read_metadata(path).metadata
        assert metadata[VERSION_KEY.encode()] == str(SCHEMA_VERSION).encode()


class TestLocalFiles:
    def test_nothing_is_left_in_the_working_directory(
        self, run_pipeline_factory, fixture_metadata, tmp_path, monkeypatch
    ):
        cwd = tmp_path / "cwd"
        cwd.mkdir()
        monkeypatch.chdir(cwd)
        run_pipeline_factory(fixture_metadata, "out")
        assert not list(cwd.iterdir())


class TestCheckpointedRun:
    """The pipeline against the real Checkpointer, on moto's S3. Both
    fixture granules are planned for the year, one per batch; only the
    first has shots in it."""

    OUTPUT_KEY = (
        f"test/prefix/data/tile_id={TILE_ID}/year=2021/data_0.parquet"
    )
    MANIFEST_KEY = f"test/prefix/checkpoints/{TILE_ID}/2021/manifest.json"

    @pytest.fixture
    def s3(self):
        def single_put(bucket, key, body, *, if_match=None, if_none_match=None):
            return s3_utils.conditional_put(
                bucket, key, body.read(),
                if_match=if_match, if_none_match=if_none_match,
            )

        with mock_aws(), patch.object(
            s3_utils, "conditional_multipart_put", single_put
        ):
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")
            yield client

    @pytest.fixture
    def both_granules(self, fixture_metadata):
        md = fixture_metadata.copy()
        md["time_start"] = pd.Timestamp("2021-06-01", tz="UTC")
        return md

    def _run(self, dps_tile_builder, args, metadata, load_granule=None):
        args.checkpoint_interval = 1
        patches = [
            patch.object(dps_tile_builder, "load_tile_metadata", return_value=metadata),
            patch.object(dps_tile_builder.s3_utils, "DaacFS", _LocalFSSpec),
        ]
        if load_granule:
            patches.append(patch.object(dps_tile_builder, "load_granule", load_granule))
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            dps_tile_builder.run_main(args)

    def _output(self, s3, tmp_path, name):
        path = tmp_path / name
        s3.download_file("test-bucket", self.OUTPUT_KEY, str(path))
        return pd.read_parquet(path)

    def _keys(self, s3):
        return sorted(
            o["Key"] for o in s3.list_objects_v2(Bucket="test-bucket").get("Contents", [])
        )

    def test_writes_output_and_leaves_only_the_manifest(
        self, s3, dps_tile_builder, args, both_granules, tmp_path
    ):
        self._run(dps_tile_builder, args, both_granules)
        assert len(self._output(s3, tmp_path, "out.parquet")) > 0
        assert self._keys(s3) == sorted([self.MANIFEST_KEY, self.OUTPUT_KEY])

    def test_resumes_after_a_crash(
        self, s3, dps_tile_builder, args, both_granules, tmp_path
    ):
        real = dps_tile_builder.load_granule
        self._run(dps_tile_builder, args, both_granules)
        expected = self._output(s3, tmp_path, "expected.parquet")
        s3.delete_object(Bucket="test-bucket", Key=self.OUTPUT_KEY)
        s3.delete_object(Bucket="test-bucket", Key=self.MANIFEST_KEY)

        def crash_on_second(granule, **kw):
            if granule == "O20346_01":
                raise RuntimeError("killed")
            return real(granule=granule, **kw)

        with pytest.raises(RuntimeError, match="killed"):
            self._run(dps_tile_builder, args, both_granules, crash_on_second)
        assert not _exists(s3, self.OUTPUT_KEY)

        loaded = []

        def record(granule, **kw):
            loaded.append(granule)
            return real(granule=granule, **kw)

        self._run(dps_tile_builder, args, both_granules, record)
        assert loaded == ["O20346_01"]
        pd.testing.assert_frame_equal(
            self._output(s3, tmp_path, "resumed.parquet"), expected
        )

    def test_a_second_job_finds_it_built(
        self, s3, dps_tile_builder, args, both_granules
    ):
        self._run(dps_tile_builder, args, both_granules)
        loaded = []
        self._run(
            dps_tile_builder, args, both_granules,
            lambda granule, **kw: loaded.append(granule),
        )
        assert loaded == []


def _exists(s3, key):
    return "Contents" in s3.list_objects_v2(Bucket="test-bucket", Prefix=key)
