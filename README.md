This repo contains the code to build, maintain, and query the Tiled GEDI database on the MAAP.

## Using the database

To try out the database, first run
```bash
conda env update -f environment.yml
```
Then you're ready check out the examples and tutorial in `tiling_demo.ipynb`!

Please do not modify the demo database
(`s3://maap-ops-workspace/shared/ameliah/gedi-test/brazil_tiled/`).
For example, do not
- manually modify the files in this location, including writing new files to these folders
- use SQL "INSERT" or "UPDATE" statements with DuckDB

## Creating a tiled database
To create a new tiled database using DPS, run
```bash
conda env update -f environment.yml
python tile_runner.py --shapefile <PATH/TO/SHAPEFILE> --bucket <AWS BUCKET> --prefix <PATH/TO/STORE/DATABASE> --job_code <DPS JOB NAME> 
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
                          |_ <tile_id>/checkpoint.pkl
                          |_ ...
```

Note that you may need to re-run the tile_runner script multiple times on the same region to process all of the tiles, to account for DPS job failures.
It is safe to re-run this script as many times as you need until it reports that no new tiles need to be added to the database.

However, due to bugs in the listJobs API (https://github.com/MAAP-Project/maap-api-nasa/issues/177), rerunning the script _while there are still tile creation jobs running_ will create duplicate jobs.
This is not necessarily safe and could result in corrupt data/undefined behavior if two jobs try to write the same tile at the same time.
To check if there are jobs still running, search for DPS jobs matching the `job_code` string passed to the script using the dps-job-management view.
Tile creation jobs checkpoint throughout and can be cancelled without losing too much work.

## Managing a tiled database

To remove a tile from the database, delete the folder `tile_id=...` from ALL OF the `data/`, `metadata/`, and (if applicable) `checkpoints/` subfolders.
