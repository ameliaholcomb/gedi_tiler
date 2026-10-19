"""Re-predict L4A AGBD for the EBT_SA and DBT_SA prediction strata with the
revised EBT_SA model (issgedi_l4a_r003_estimators_20260722, group 4 /
EBT_SA / selection 3), from each shot's stored predictors, as in
https://github.com/ornldaac/gedi_tutorials/blob/c5df3067adf4324684888b32945f5f0a7e7d51c2/notebooks/gedi_l4a_v003_correction_south_america.ipynb

Both strata were predicted with the EBT_SA model, whose predictor
definition (sqrt of RH50 and RH98) is unchanged, so the stored xvar
still holds each shot's predictors. Every column that depends on the
model's coefficients is rewritten, for the selected prediction (xvar) and
for each algorithm setting group's (xvar_a1 .. xvar_a10):

    agbd_t = [1, x1, x2] @ par
    agbd_t_se = sqrt([1, x1, x2] @ vcov @ [1, x1, x2]' + rse^2)
    agbd = c * agbd_t^2, or 0 where agbd_t < 0
    agbd_se = c * agbd_t_se^2
    agbd_pi_lower = c * (agbd_t - z * agbd_t_se)^2, or -9999 where that
        bound is negative
    agbd_pi_upper = c * (agbd_t + z * agbd_t_se)^2, or -9999 where that
        bound is negative
    response_limit_flag = 2 where agbd > 1.2 * response_max_value, else 0
    predictor_limit_flag = 2 where any x_i > 1.2 * predictor_max_value_i,
        else 0

with c the Snowdon bias correction and z = qt(0.95, dof). With the old
model's values from the granules' ANCILLARY/model_data, these reproduce
the V003 values. Setting agbd to 0 where agbd_t < 0 follows the V002
and V003 granules, and agbd_pi_upper to -9999 where its bound is
negative follows V002 (no V003 shot has one); the notebook squares
agbd_t instead. Shots without predictors (-9999) keep their no-data
values. All other columns, rows and their order are unchanged.

Rewritten files record MARKER_KEY in their footer. A one-off: remove once
every file is rewritten.
"""

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from gtiler.database.schema_v3 import SCHEMA_VERSION, TILE_FORMAT_OPTIONS, VERSION_KEY
from gtiler.database.tiles import Tile

STRATA = ("EBT_SA", "DBT_SA")

# The revised EBT_SA model, from the notebook.
PAR = [-133.884363713764, 6.95698142185319, 6.28656682203761]
VCOV = [
    [2.50305363959644, -0.111702554712149, -0.114315207874669],
    [-0.111702554712149, 0.0654051766999497, -0.0523471595323726],
    [-0.114315207874669, -0.0523471595323726, 0.0598715899876015],
]
RSE = 3.53298787494478
BIAS_CORRECTION = 1.15323996418645
RESPONSE_MAX = 1578.0
PREDICTOR_MAX = [12.2270192606375, 12.7730184373154]
# qt(0.95, 3485), for the model's 3485 degrees of freedom.
Z = 1.6452909803940292
# The limit flags mark values beyond 1.2 times the training maxima.
LIMIT = 1.2
NODATA = -9999
XVAR_BINS = 4

MARKER_KEY = "gtiler_l4a_ebt_sa_refit"
MARKER_VALUE = "20260722"
COPY_OPTIONS = f"""{TILE_FORMAT_OPTIONS},
    KV_METADATA {{{VERSION_KEY}: '{SCHEMA_VERSION}', {MARKER_KEY}: '{MARKER_VALUE}'}}
"""

# The selected prediction, then each algorithm setting group's.
GROUPS = ["", "a1", "a2", "a5", "a10"]
OUTPUTS = [
    "agbd",
    "agbd_t",
    "agbd_t_se",
    "agbd_se",
    "agbd_pi_lower",
    "agbd_pi_upper",
    "response_limit_flag",
    "predictor_limit_flag",
]


def _column(name, group):
    return f"{name}_l4a" if group == "" else f"agbd_prediction_{name}_{group}_l4a"


TARGETS = {_column(n, g): g for g in GROUPS for n in OUTPUTS}


def _in_south_america(tile_id):
    """Whether the tile lies within a box around South America, which
    holds every EBT_SA and DBT_SA shot."""
    t = Tile(tile_id)
    return -120 <= t.minx and t.maxx <= -20 and -60 <= t.miny and t.maxy <= 35


def pending(tile_id, path, filesystem=None):
    """Whether the file at `path`, of tile `tile_id`, still needs
    rewriting: it has EBT_SA or DBT_SA shots and does not record
    MARKER_KEY."""
    if not _in_south_america(tile_id):
        return False
    if MARKER_KEY.encode() in pq.read_metadata(path, filesystem=filesystem).metadata:
        return False
    strata = pq.read_table(path, columns=["predict_stratum_l4a"], filesystem=filesystem).column(0)
    return pc.any(pc.is_in(strata, pa.array(STRATA))).as_py()


def _affected(group, table=""):
    """Whether a row is an EBT_SA or DBT_SA shot with predictors in `group`."""
    x = f"{table}{_column('xvar', group)}"
    strata = ", ".join(f"'{s}'" for s in STRATA)
    return f"({table}predict_stratum_l4a IN ({strata}) AND {x}[1] != {NODATA} AND {x}[2] != {NODATA})"


def _predictions(group):
    """SQL for each output of `group`, given the helper columns t_<group>
    (agbd_t) and se_<group> (agbd_t_se) as DOUBLE."""
    x = _column("xvar", group)
    t, se = f"t_{group}", f"se_{group}"
    lower, upper = f"({t} - {Z} * {se})", f"({t} + {Z} * {se})"
    agbd = f"CASE WHEN {t} < 0 THEN 0 ELSE {BIAS_CORRECTION} * {t} * {t} END"
    return {
        "agbd": f"({agbd})::FLOAT",
        "agbd_t": f"{t}::FLOAT",
        "agbd_t_se": f"{se}::FLOAT",
        "agbd_se": f"({BIAS_CORRECTION} * {se} * {se})::FLOAT",
        "agbd_pi_lower": f"(CASE WHEN {lower} < 0 THEN {NODATA} ELSE {BIAS_CORRECTION} * {lower} * {lower} END)::FLOAT",
        "agbd_pi_upper": f"(CASE WHEN {upper} < 0 THEN {NODATA} ELSE {BIAS_CORRECTION} * {upper} * {upper} END)::FLOAT",
        "response_limit_flag": f"(CASE WHEN {agbd} > {LIMIT * RESPONSE_MAX} THEN 2 ELSE 0 END)::UTINYINT",
        "predictor_limit_flag": f"(CASE WHEN {x}[1] > {LIMIT * PREDICTOR_MAX[0]} "
        f"OR {x}[2] > {LIMIT * PREDICTOR_MAX[1]} THEN 2 ELSE 0 END)::UTINYINT",
    }


def rewrite(con, source: str, output: str):
    helpers, replace = [], []
    for g in GROUPS:
        x1, x2 = (f"{_column('xvar', g)}[{i}]::DOUBLE" for i in (1, 2))
        terms = [1, x1, x2]
        quadratic = " + ".join(
            f"{VCOV[i][j]} * {a} * {b}" for i, a in enumerate(terms) for j, b in enumerate(terms)
        )
        helpers += [
            f"{PAR[0]} + {PAR[1]} * {x1} + {PAR[2]} * {x2} AS t_{g}",
            f"sqrt({quadratic} + {RSE ** 2}) AS se_{g}",
        ]
        affected = _affected(g)
        replace += [
            f"CASE WHEN {affected} THEN {expr} ELSE {_column(n, g)} END AS {_column(n, g)}"
            for n, expr in _predictions(g).items()
        ]
    helper_names = ", ".join(f"{h}_{g}" for g in GROUPS for h in ("t", "se"))
    con.execute("SET preserve_insertion_order = true;")
    con.sql(f"""--sql
        COPY (
            SELECT * EXCLUDE ({helper_names}) REPLACE ({", ".join(replace)})
            FROM (
                SELECT *, {", ".join(helpers)}
                FROM read_parquet('{source}', hive_partitioning = false)
            )
        ) TO '{output}' ({COPY_OPTIONS});
    """)


def _expected(x):
    """Each output for predictors `x` (rows of x1, x2), computed
    independently of the SQL, as the types they are stored as."""
    X = np.c_[np.ones(len(x)), x]
    t = X @ np.array(PAR)
    se = np.sqrt(np.einsum("ij,jk,ik->i", X, np.array(VCOV), X) + RSE**2)
    agbd = np.where(t < 0, 0, BIAS_CORRECTION * t**2)
    lower, upper = t - Z * se, t + Z * se
    return {
        "agbd": agbd.astype(np.float32),
        "agbd_t": t.astype(np.float32),
        "agbd_t_se": se.astype(np.float32),
        "agbd_se": (BIAS_CORRECTION * se**2).astype(np.float32),
        "agbd_pi_lower": np.where(lower < 0, NODATA, BIAS_CORRECTION * lower**2).astype(np.float32),
        "agbd_pi_upper": np.where(upper < 0, NODATA, BIAS_CORRECTION * upper**2).astype(np.float32),
        "response_limit_flag": np.where(agbd > LIMIT * RESPONSE_MAX, 2, 0).astype(np.uint8),
        "predictor_limit_flag": np.where((x > LIMIT * np.array(PREDICTOR_MAX)).any(axis=1), 2, 0).astype(np.uint8),
    }


def check(con, source: str, output: str):
    """Raise unless `output` holds the rows of `source`, in the same order,
    with the same schema, with every value equal except the rewritten
    columns of the affected rows, which must match an independent
    computation of the revised model."""
    src = f"read_parquet('{source}', hive_partitioning = false)"
    out = f"read_parquet('{output}', hive_partitioning = false)"
    schema = con.sql(f"DESCRIBE SELECT * FROM {src}").fetchall()
    if con.sql(f"DESCRIBE SELECT * FROM {out}").fetchall() != schema:
        raise RuntimeError(f"{output} has a different schema from {source}")
    (n_src,) = con.sql(f"SELECT count(*) FROM {src}").fetchone()
    (n_out,) = con.sql(f"SELECT count(*) FROM {out}").fetchone()
    if n_src != n_out:
        raise RuntimeError(f"{output} has {n_out} rows, {source} has {n_src}")
    differs = [
        f'(NOT {_affected(TARGETS[c], "s.")} AND s."{c}" IS DISTINCT FROM o."{c}")'
        if c in TARGETS
        else f's."{c}" IS DISTINCT FROM o."{c}"'
        for c, *_ in schema
    ]
    (bad,) = con.sql(f"""--sql
        SELECT count(*)
        FROM (SELECT * FROM {src}) s POSITIONAL JOIN (SELECT * FROM {out}) o
        WHERE {" OR ".join(differs)}
    """).fetchone()
    if bad:
        raise RuntimeError(f"{output}: {bad} rows differ from {source} outside the rewritten values")

    for g in GROUPS:
        x = _column("xvar", g)
        names = [_column(n, g) for n in OUTPUTS]
        rows = con.sql(f"""--sql
            SELECT s."{x}" AS x, {", ".join(f'o."{n}"' for n in names)}
            FROM (SELECT * FROM {src}) s POSITIONAL JOIN (SELECT * FROM {out}) o
            WHERE {_affected(g, "s.")}
        """).to_arrow_table()
        xs = pc.list_flatten(rows["x"]).to_numpy().reshape(-1, XVAR_BINS)[:, :2].astype(np.float64)
        for n, expected in _expected(xs).items():
            written = rows[_column(n, g)].to_numpy()
            if n.endswith("_flag"):
                wrong = expected != written
            else:
                wrong = ~np.isclose(written, expected, rtol=1e-6, atol=1e-6)
            if wrong.any():
                raise RuntimeError(f"{output}: {wrong.sum()} rows of {_column(n, g)} differ from the revised model")
