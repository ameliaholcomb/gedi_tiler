"""One-shot script to build mini local HDF5 fixtures for dps_tile_builder tests.

For each granule in the test metadata fixture, pulls the four real GEDI
product files from S3, then writes mini HDF5 files containing the same
group/dataset structure but only ~20 in-tile shots per beam. Updates the
metadata fixture so its level*_url columns point at the local mini files.

Run once to (re)build fixtures:
    conda run -n pyduck python gtiler/tests/fixtures/build_granule_fixtures.py
"""

import pathlib
import sys

import geopandas as gpd
import h5py
import numpy as np

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from gtiler.common import s3_utils  # noqa: E402
from gtiler.database.schema import SCHEMA  # noqa: E402
from gtiler.database.tiles import Tile  # noqa: E402

FIXTURES = pathlib.Path(__file__).parent
TILE_ID = "N00_W050"
N_SHOTS_PER_BEAM = 20  # keep tiny but enough to survive quality filters
N_LOW_QUALITY_PER_BEAM = 5  # first N shots/beam patched to fail QF

PRODUCT_BY_LEVEL = {
    "level2A": SCHEMA.products[0],
    "level2B": SCHEMA.products[1],
    "level4A": SCHEMA.products[2],
    "level4C": SCHEMA.products[3],
}

# Quality-flag patching pattern. For each per-beam dataset, the first
# N_LOW_QUALITY_PER_BEAM shots get the "fail" value and the rest get the
# "pass" value. This lets the missing-product test verify that disabling
# QF tile-wide actually preserves low-quality footprints.
#
# The L2A entries cover every column the run_main quality filter reads
# (quality_flag, sensitivity, sensitivity_a2, degrade_flag, surface_flag);
# the other levels set their respective product quality flags so that
# any future cross-product QF logic has consistent fixture state.
QUALITY_PATTERN = {
    "level2A": [
        ("quality_flag", 0, 1),
        ("sensitivity", 0.5, 0.95),
        ("geolocation/sensitivity_a2", 0.5, 0.98),
        ("degrade_flag", 1, 0),  # 0 ∈ QDEGRADE; 1 ∉ QDEGRADE
        ("surface_flag", 0, 1),
    ],
    "level2B": [
        ("l2a_quality_flag", 0, 1),
        ("l2b_quality_flag", 0, 1),
    ],
    "level4A": [
        ("l2_quality_flag", 0, 1),
        ("l4_quality_flag", 0, 1),
    ],
    "level4C": [
        ("wsci_quality_flag", 0, 1),
    ],
}


def _in_tile_indices(hdf5, beam, product, tile):
    lats = hdf5[f"{beam}/{product.geometry.lat.SDS_Name}"][:]
    lons = hdf5[f"{beam}/{product.geometry.lon.SDS_Name}"][:]
    return np.where(
        (lons >= tile.minx)
        & (lons < tile.maxx)
        & (lats > tile.miny)
        & (lats <= tile.maxy)
    )[0]


def _copy_attrs(src, dst):
    for k, v in src.attrs.items():
        try:
            dst.attrs[k] = v
        except Exception:
            pass


def _sds_paths_for_product(product) -> list[str]:
    """Return the per-beam SDS paths that load_granule_product reads."""
    paths = [
        product.primary_key.SDS_Name,
        product.geometry.lat.SDS_Name,
        product.geometry.lon.SDS_Name,
    ]
    paths.extend(v.SDS_Name for v in product.variables)
    return paths


def _write_subset(
    src_beam: h5py.Group,
    dst_beam: h5py.Group,
    idx: np.ndarray,
    sds_paths: list[str],
):
    """Copy only the listed datasets from src_beam to dst_beam, slicing
    along the first axis when it matches the beam's shot count (ancillary
    scalars with shape (1,) get copied whole)."""
    n_shots = src_beam["shot_number"].shape[0]
    for path in sds_paths:
        if path not in src_beam:
            continue
        src = src_beam[path]
        data = src[:]
        if data.ndim >= 1 and data.shape[0] == n_shots:
            data = data[idx, ...]
        parent = "/".join(path.split("/")[:-1])
        if parent:
            dst_beam.require_group(parent)
        ds = dst_beam.create_dataset(path, data=data)
        _copy_attrs(src, ds)
    _copy_attrs(src_beam, dst_beam)


def patch_quality_flags(local_path: pathlib.Path, level: str, n_low: int):
    """In-place: set the first n_low shots/beam to QF-fail values and
    the rest to QF-pass values, per QUALITY_PATTERN[level]."""
    with h5py.File(local_path, "r+") as f:
        for beam in [k for k in f.keys() if k.startswith("BEAM")]:
            for sds, fail_v, pass_v in QUALITY_PATTERN[level]:
                if sds not in f[beam]:
                    continue
                ds = f[f"{beam}/{sds}"]
                n = ds.shape[0]
                vals = np.full(n, pass_v, dtype=ds.dtype)
                vals[:n_low] = fail_v
                ds[...] = vals


def build_mini_granule(
    rfs: s3_utils.RefreshableFSSpec,
    src_urls: dict,  # {"level2A": url, ...}
    out_dir: pathlib.Path,
    granule_key: str,
    tile: Tile,
) -> dict:
    """Returns a dict of {level: local_path_str} for the written mini files."""
    out_dir.mkdir(parents=True, exist_ok=True)

    # Determine the in-tile shot_numbers per beam from the L2A file.
    l2a_product = PRODUCT_BY_LEVEL["level2A"]
    shot_numbers_by_beam: dict[str, np.ndarray] = {}
    with rfs.get_fs().open(src_urls["level2A"], mode="rb") as f, h5py.File(f) as h:
        for beam in [k for k in h.keys() if k.startswith("BEAM")]:
            idx = _in_tile_indices(h, beam, l2a_product, tile)
            if len(idx) == 0:
                continue
            idx = idx[:N_SHOTS_PER_BEAM]
            shot_numbers_by_beam[beam] = h[f"{beam}/shot_number"][idx]

    if not shot_numbers_by_beam:
        raise RuntimeError(
            f"granule {granule_key} has no in-tile shots — pick a different one"
        )

    written: dict[str, str] = {}
    for level, url in src_urls.items():
        sds_paths = _sds_paths_for_product(PRODUCT_BY_LEVEL[level])
        local_path = out_dir / f"{granule_key}_{level}.h5"
        with rfs.get_fs().open(url, mode="rb") as f, h5py.File(f) as src:
            with h5py.File(local_path, "w") as dst:
                _copy_attrs(src, dst)
                for beam, target_shots in shot_numbers_by_beam.items():
                    if beam not in src:
                        continue
                    shots = src[f"{beam}/shot_number"][:]
                    matches = np.where(np.isin(shots, target_shots))[0]
                    if len(matches) == 0:
                        continue
                    dst_beam = dst.create_group(beam)
                    _write_subset(src[beam], dst_beam, matches, sds_paths)
        written[level] = str(local_path)
        print(f"  wrote {local_path} ({local_path.stat().st_size / 1024:.1f} KB)")
    return written


def main():
    tile = Tile(TILE_ID)
    metadata_path = FIXTURES / f"metadata/tile_id={TILE_ID}/data_0.parquet"
    md = gpd.read_file(metadata_path)
    print(f"loaded fixture metadata with {len(md)} granules")

    rfs = s3_utils.RefreshableFSSpec("/iam/maap-data-reader")
    granules_dir = FIXTURES / "granules"

    new_rows = md.copy()
    for i, row in md.iterrows():
        print(f"\nbuilding mini files for {row.granule_key} ...")
        local = build_mini_granule(
            rfs=rfs,
            src_urls={
                "level2A": row.level2A_url,
                "level2B": row.level2B_url,
                "level4A": row.level4A_url,
                "level4C": row.level4C_url,
            },
            out_dir=granules_dir,
            granule_key=row.granule_key,
            tile=tile,
        )
        for level, path in local.items():
            patch_quality_flags(
                pathlib.Path(path), level, N_LOW_QUALITY_PER_BEAM
            )
            new_rows.at[i, f"{level}_url"] = f"file://{path}"

    new_rows.to_parquet(metadata_path)
    print(f"\nupdated metadata fixture at {metadata_path}")
    print(new_rows[["granule_key", "level2A_url"]].to_string())


if __name__ == "__main__":
    main()
