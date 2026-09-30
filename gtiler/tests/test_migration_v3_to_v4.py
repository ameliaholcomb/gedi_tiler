"""Tests for scripts/migrations/v3_to_v4_profile_lists.py: a layout 3 file,
migrated, is the file the builder writes today.

Layout 3 files are made from the builder's output by flattening each
profile list back into one column per bin, in the list's place, and
dropping the recorded version. Remove with the migration.
"""

import sys

import duckdb
import pyarrow.parquet as pq
import pytest

from gtiler.database.schema_v3 import SCHEMA, SCHEMA_VERSION, VERSION_KEY

from test_dps_tile_builder import (  # noqa: F401  (fixtures)
    REPO_ROOT,
    TILE_ID,
    args,
    dps_tile_builder,
    fixture_metadata,
    run_pipeline,
    run_pipeline_factory,
)

sys.path.insert(0, str(REPO_ROOT / "scripts"))
from migrations import v3_to_v4_profile_lists as migration  # noqa: E402

PROFILES = {v.variable: v for p in SCHEMA.products for v in p.variables if v.is_profile}


@pytest.fixture
def v4_file(run_pipeline):
    (path,) = (run_pipeline / f"tile_id={TILE_ID}").glob("year=*/*.parquet")
    return str(path)


@pytest.fixture
def con():
    c = duckdb.connect()
    c.load_extension("spatial")
    c.execute("SET threads = 1; SET preserve_insertion_order = true;")
    return c


@pytest.fixture
def v3_file(con, v4_file, tmp_path):
    columns = [r[0] for r in con.sql(f"DESCRIBE SELECT * FROM read_parquet('{v4_file}', hive_partitioning = false)").fetchall()]
    select = []
    for c in columns:
        if c in PROFILES:
            select += [f'"{c}"[{i + 1}] AS "{c}_{i}"' for i in range(PROFILES[c].n_bins)]
        else:
            select.append(f'"{c}"')
    path = str(tmp_path / "v3.parquet")
    con.sql(f"""
        COPY (SELECT {", ".join(select)} FROM read_parquet('{v4_file}', hive_partitioning = false))
        TO '{path}' (FORMAT parquet, GEOPARQUET_VERSION 'V2', COMPRESSION zstd)
    """)
    return path


def _schema(path):
    return [(f.name, str(f.type)) for f in pq.read_schema(path).remove_metadata()]


def test_migrated_file_matches_the_builders(con, v3_file, v4_file, tmp_path):
    migrated = str(tmp_path / "migrated.parquet")
    migration.rewrite(con, v3_file, migrated)
    migration.check(con, v3_file, migrated)
    assert _schema(migrated) == _schema(v4_file)
    columns = [r[0] for r in con.sql(f"DESCRIBE SELECT * FROM '{migrated}'").fetchall()]
    differs = " OR ".join(f'm."{c}" IS DISTINCT FROM b."{c}"' for c in columns)
    (differ,) = con.sql(f"""
        SELECT count(*) FROM '{migrated}' m
        POSITIONAL JOIN read_parquet('{v4_file}', hive_partitioning = false) b
        WHERE {differs}
    """).fetchone()
    assert differ == 0
    metadata = pq.read_metadata(migrated).metadata
    assert metadata[VERSION_KEY.encode()] == str(SCHEMA_VERSION).encode()
    assert set(metadata) == set(pq.read_metadata(v4_file).metadata)


@pytest.mark.parametrize(
    "column, changed",
    [("rh_l2a", "list_transform(rh_l2a, x -> x + 1)"), ("agbd_l4a", "agbd_l4a + 1")],
)
def test_check_catches_a_changed_value(con, v3_file, tmp_path, column, changed):
    migrated = str(tmp_path / "migrated.parquet")
    migration.rewrite(con, v3_file, migrated)
    bad = str(tmp_path / "bad.parquet")
    con.sql(f"""
        COPY (
            SELECT * EXCLUDE (file_row_number) REPLACE (
                CASE WHEN file_row_number = 0 THEN {changed} ELSE {column} END AS {column}
            )
            FROM read_parquet('{migrated}', file_row_number = true)
        ) TO '{bad}' (FORMAT parquet, GEOPARQUET_VERSION 'V2')
    """)
    with pytest.raises(RuntimeError, match="1 rows differ"):
        migration.check(con, v3_file, bad)


def test_check_catches_reordered_rows(con, v3_file, tmp_path):
    migrated = str(tmp_path / "migrated.parquet")
    migration.rewrite(con, v3_file, migrated)
    bad = str(tmp_path / "bad.parquet")
    con.sql(f"""
        COPY (
            SELECT * EXCLUDE (file_row_number)
            FROM read_parquet('{migrated}', file_row_number = true)
            ORDER BY CASE file_row_number WHEN 0 THEN 1 WHEN 1 THEN 0 ELSE file_row_number END
        ) TO '{bad}' (FORMAT parquet, GEOPARQUET_VERSION 'V2')
    """)
    with pytest.raises(RuntimeError, match="rows differ"):
        migration.check(con, v3_file, bad)
