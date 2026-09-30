"""Migrate a tile-year file from layout 3 to 4: gather each profile's
flattened bin columns (rh_l2a_0 .. rh_l2a_100) into one list column
(rh_l2a), and rewrite with the current write settings. Rows and all other
columns are kept as they are, in the same order.

A one-off: remove once every file is migrated.
"""

from gtiler.database.schema_v3 import SCHEMA, TILE_COPY_OPTIONS

FROM_VERSION = 3
TO_VERSION = 4

PROFILES = [v for p in SCHEMA.products for v in p.variables if v.is_profile]


def _bins(profile):
    """Layout 3's column for each bin of a profile, in bin order."""
    return [f'"{profile.variable}_{i}"' for i in range(profile.n_bins)]


def _as_list(profile, table=""):
    """A list of the profile's bin columns, cast to the profile's type."""
    bins = ", ".join(f"{table}{b}" for b in _bins(profile))
    return f"[{bins}]::{profile.duckdb_type}"


def rewrite(con, source: str, output: str):
    columns = [
        r[0] for r in con.sql(f"DESCRIBE SELECT * FROM read_parquet('{source}', hive_partitioning = false)").fetchall()
    ]
    first_bins = {f"{p.variable}_0": p for p in PROFILES}
    all_bins = {b.strip('"') for p in PROFILES for b in _bins(p)}
    select = []
    for c in columns:
        if c in first_bins:
            p = first_bins[c]
            select.append(f'{_as_list(p)} AS "{p.variable}"')
        elif c not in all_bins:
            select.append(f'"{c}"')
    con.execute("SET preserve_insertion_order = true;")
    con.sql(f"""--sql
        COPY (
            SELECT {", ".join(select)} FROM read_parquet('{source}', hive_partitioning = false)
        ) TO '{output}' ({TILE_COPY_OPTIONS});
    """)


def check(con, source: str, output: str):
    """Raise unless `output` holds exactly the rows of `source`, in the
    same order, with each list equal to its bins and every other value
    equal."""
    src = f"read_parquet('{source}', hive_partitioning = false)"
    out = f"read_parquet('{output}', hive_partitioning = false)"
    columns = [r[0] for r in con.sql(f"DESCRIBE SELECT * FROM {out}").fetchall()]
    profiles = {p.variable: p for p in PROFILES}
    differs = [
        f'{_as_list(profiles[c], "s.")} IS DISTINCT FROM o."{c}"'
        if c in profiles
        else f's."{c}" IS DISTINCT FROM o."{c}"'
        for c in columns
    ]
    (n_src,) = con.sql(f"SELECT count(*) FROM {src}").fetchone()
    (n_out,) = con.sql(f"SELECT count(*) FROM {out}").fetchone()
    if n_src != n_out:
        raise RuntimeError(f"{output} has {n_out} rows, {source} has {n_src}")
    (bad,) = con.sql(f"""--sql
        SELECT count(*)
        FROM (SELECT * FROM {src}) s POSITIONAL JOIN (SELECT * FROM {out}) o
        WHERE {" OR ".join(differs)}
    """).fetchone()
    if bad:
        raise RuntimeError(f"{output}: {bad} rows differ from {source}")
