This repo contains the code to build, maintain, and query the Tiled GEDI database on the MAAP.

## Using the database

To try out the database, first run
```bash
git clone git@github.com:ameliaholcomb/gedi_tiler.git
cd gedi_tiler
conda env update -f environment.yml
```
Then you're ready check out the examples and tutorial in `tiling_demo.ipynb`!

### Reading the GEDI V3 database

V3 files are Parquet format V2 with zstd compression and native GeoParquet 2
geometry columns, and each profile (e.g. `rh_l2a`) is one list column. Readers
need:

- **Python:** pyarrow ≥ 20 (so pandas 2.x with pyarrow ≥ 20), or DuckDB ≥ 1.3
  with the spatial extension.
- **R:** arrow ≥ 20, duckdb ≥ 1.3, or sf (GDAL built with Parquet support).
- **Not supported:** nanoparquet (it reads the files but returns wrong values
  without an error), fastparquet (no BYTE_STREAM_SPLIT decoding), and pyarrow
  or R arrow ≤ 19 (they reject the geometry type).

Profile lists are 1-indexed in SQL and R, so `rh_l2a[99]` is RH98; in
pyarrow and pandas they are 0-indexed arrays (`rh_l2a[98]`). `shot_number` and
the H3 cells are 64-bit integers: in R, install bit64 so arrow reads them
exactly, and connect with `duckdb(bigint = "integer64")` in R duckdb.

These requirements are not yet in `pyproject.toml`, because the GEDI V2
database does not need them.

## Creating a tiled database
To create a new tiled database using DPS, run
```bash
conda env update -f environment.yml
python scripts/runners/tile_runner.py --shapefile <PATH/TO/SHAPEFILE> --bucket <AWS BUCKET> --prefix <PATH/TO/STORE/DATABASE> --job_code <DPS JOB NAME> 
```
The database will be structured as:
```
s3://{BUCKET}/{PREFIX}/ - data/
                          |_ tile_id=<name>/
                          |       |_ year=2019
                          |       |      |_ data_0.parquet
                          |       |_ year=2020
                          |       |      |_ data_0.parquet
                          |       |_ year=...
                          |_ tile_id=...
                        - metadata/
                          |_ tile_id=<name>/
                          |       |_ data_0.parquet
                          |_ tile_id=...
                        - checkpoints/
                          |_ <tile_id>/<year>/manifest.json (and part-*.parquet while a job runs)
                          |_ ...
```

Note that you may need to re-run the tile_runner script multiple times on the same region to process all of the tiles, to account for DPS job failures.
It is safe to re-run this script as many times as you need until it reports that no new tiles need to be added to the database.

Rerunning the script _while tile creation jobs are still running_ submits duplicate jobs for their tile-years. This is safe but wasteful: jobs claim a tile-year's checkpoint manifest with conditional S3 writes, the newest claim wins, and the others stop without writing output.
To check if there are jobs still running, search for DPS jobs matching the `job_code` string passed to the script using the dps-job-management view.
Tile creation jobs checkpoint throughout, and a job for the same tile-year resumes from the checkpoint, so they can be cancelled without losing much work.

## Managing a tiled database

To remove a tile from the database, delete the folder `tile_id=...` from ALL OF the `data/`, `metadata/`, and (if applicable) `checkpoints/` subfolders.

To change the files already in a database (e.g. a new column type), write a migration in `scripts/tools/migrations/`: a module with `FROM_VERSION`, `TO_VERSION`, and `rewrite(con, source, output)` and `check(con, source, output)` functions, where the versions are the file layout versions recorded in each file's footer (`SCHEMA_VERSION` in `gtiler/database/schema_v3.py`). Then run it on DPS with
```bash
python scripts/runners/rewrite_runner.py --bucket <AWS BUCKET> --prefix <PATH/TO/DATABASE> --migration <MODULE NAME> --algo_version <gedi-tile-rewriter VERSION> --job_code <DPS JOB NAME> -i 1 --submit_interval 1
```
Each file is replaced only after its check passes, and only if it hasn't changed since it was read. Files already at `TO_VERSION` are skipped, so rerun the runner until it plans no jobs. Remove the migration once every file is migrated.
