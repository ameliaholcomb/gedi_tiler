"""Tests for scripts/tools/migrations/l4a_ebt_sa_refit.py: EBT_SA and
DBT_SA shots are re-predicted with the revised model, and nothing else
changes.

The source file is the builder's output, whose fixture shots are all
EBT_SA, with every third shot relabelled DBT_SA and every third GSW_SA.
Remove with the migration.
"""

import sys

import duckdb
import numpy as np
import pyarrow.parquet as pq
import pytest

from gtiler.database.schema_v3 import SCHEMA_VERSION, VERSION_KEY

from test_dps_tile_builder import (  # noqa: F401  (fixtures)
    REPO_ROOT,
    TILE_ID,
    args,
    dps_tile_builder,
    fixture_metadata,
    run_pipeline,
    run_pipeline_factory,
)

sys.path.insert(0, str(REPO_ROOT / "scripts" / "tools"))
from migrations import l4a_ebt_sa_refit as migration  # noqa: E402


@pytest.fixture
def con():
    c = duckdb.connect()
    c.load_extension("spatial")
    c.execute("SET threads = 1; SET preserve_insertion_order = true;")
    return c


@pytest.fixture
def source(con, run_pipeline, tmp_path):
    (built,) = (run_pipeline / f"tile_id={TILE_ID}").glob("year=*/*.parquet")
    path = str(tmp_path / "source.parquet")
    con.sql(f"""
        COPY (
            SELECT * EXCLUDE (file_row_number) REPLACE (
                CASE file_row_number % 3 WHEN 1 THEN 'DBT_SA' WHEN 2 THEN 'GSW_SA'
                ELSE predict_stratum_l4a END AS predict_stratum_l4a
            )
            FROM read_parquet('{built}', file_row_number = true, hive_partitioning = false)
        ) TO '{path}' ({migration.TILE_FORMAT_OPTIONS})
    """)
    return path


@pytest.fixture
def migrated(con, source, tmp_path):
    path = str(tmp_path / "migrated.parquet")
    migration.rewrite(con, source, path)
    return path


def _read(con, path):
    return con.sql(f"SELECT * FROM read_parquet('{path}', hive_partitioning = false)").df()


def test_rewrite_passes_its_check(con, source, migrated):
    migration.check(con, source, migrated)


def test_rewrite_matches_the_notebook(con, source, migrated):
    before, after = _read(con, source), _read(con, migrated)
    x = np.stack(before.xvar_l4a.values)[:, :2].astype(np.float64)
    affected = before.predict_stratum_l4a.isin(migration.STRATA).values & (x != -9999).all(axis=1)
    assert affected.sum() > 0 and (~affected).sum() > 0
    # The notebook's prediction, which squares negative agbd_t.
    t = np.c_[np.ones(len(x)), x] @ np.array(migration.PAR)
    notebook = (t**2 * migration.BIAS_CORRECTION).astype(np.float32)
    positive = affected & (t >= 0)
    np.testing.assert_allclose(after.agbd_l4a.values[positive], notebook[positive], rtol=1e-6)
    assert (after.agbd_l4a.values[affected & (t < 0)] == 0).all()
    assert (after.agbd_l4a.values[affected] != before.agbd_l4a.values[affected]).all()
    for c in migration.TARGETS:
        assert (after[c].values[~affected] == before[c].values[~affected]).all(), c


def test_rewrite_records_its_marker(source, migrated):
    metadata = pq.read_metadata(migrated).metadata
    assert metadata[migration.MARKER_KEY.encode()] == migration.MARKER_VALUE.encode()
    assert metadata[VERSION_KEY.encode()] == str(SCHEMA_VERSION).encode()
    assert migration.pending(TILE_ID, source)
    assert not migration.pending(TILE_ID, migrated)


def test_pending_only_in_south_america(source):
    assert not migration.pending("N50_E010", source)


@pytest.mark.parametrize(
    "column, changed, where",
    [
        ("agbd_l4a", "(agbd_l4a + 1)::FLOAT", "predict_stratum_l4a = 'GSW_SA'"),
        ("agbd_prediction_agbd_se_a5_l4a", "(agbd_prediction_agbd_se_a5_l4a + 1)::FLOAT", "predict_stratum_l4a = 'GSW_SA'"),
        ("rh_l2a", "list_transform(rh_l2a, x -> x + 1)::FLOAT[]", "true"),
    ],
)
def test_check_catches_an_unexpected_change(con, source, migrated, tmp_path, column, changed, where):
    bad = str(tmp_path / "bad.parquet")
    con.sql(f"""
        COPY (
            SELECT * EXCLUDE (file_row_number) REPLACE (
                CASE WHEN file_row_number = (
                    SELECT min(file_row_number) FROM read_parquet('{migrated}', file_row_number = true) WHERE {where}
                ) THEN {changed} ELSE {column} END AS {column}
            )
            FROM read_parquet('{migrated}', file_row_number = true, hive_partitioning = false)
        ) TO '{bad}' ({migration.TILE_FORMAT_OPTIONS})
    """)
    with pytest.raises(RuntimeError, match="1 rows differ"):
        migration.check(con, source, bad)


@pytest.mark.parametrize(
    "column, type",
    [("agbd_l4a", "FLOAT"), ("agbd_prediction_agbd_pi_upper_a10_l4a", "FLOAT"), ("response_limit_flag_l4a", "UTINYINT")],
)
def test_check_catches_a_wrong_prediction(con, source, migrated, tmp_path, column, type):
    bad = str(tmp_path / "bad.parquet")
    con.sql(f"""
        COPY (
            SELECT * EXCLUDE (file_row_number) REPLACE (
                CASE WHEN file_row_number = (
                    SELECT min(file_row_number) FROM read_parquet('{migrated}', file_row_number = true)
                    WHERE predict_stratum_l4a = 'EBT_SA' AND agbd_l4a > 0
                ) THEN ({column} + 1)::{type} ELSE {column} END AS {column}
            )
            FROM read_parquet('{migrated}', file_row_number = true, hive_partitioning = false)
        ) TO '{bad}' ({migration.TILE_FORMAT_OPTIONS})
    """)
    with pytest.raises(RuntimeError, match=f"1 rows of {column} differ"):
        migration.check(con, source, bad)


def test_check_catches_reordered_rows(con, source, migrated, tmp_path):
    bad = str(tmp_path / "bad.parquet")
    con.sql(f"""
        COPY (
            SELECT * EXCLUDE (file_row_number)
            FROM read_parquet('{migrated}', file_row_number = true, hive_partitioning = false)
            ORDER BY CASE file_row_number WHEN 0 THEN 2 WHEN 2 THEN 0 ELSE file_row_number END
        ) TO '{bad}' ({migration.TILE_FORMAT_OPTIONS})
    """)
    with pytest.raises(RuntimeError, match="differ"):
        migration.check(con, source, bad)
