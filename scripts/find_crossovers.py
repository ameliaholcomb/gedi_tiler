"""
Module for finding repeat GEDI footprints (crossovers) using H3 spatial indexing.

Provides functionality to identify pairs of GEDI shots that are within a specified
distance of each other, with support for user-supplied filters and data columns.
"""
import argparse
import h3
import geopandas as gpd
from maap.maap import MAAP
from typing import List, Optional, Union

from gtiler.database import ducky
from gtiler.database.query_lib import crossovers

def main(args):
    shape = gpd.read_file(args.shapefile).to_crs("EPSG:4326")
    if len(shape) > 1:
        raise ValueError("Currently, only single polygons are supported.")
    geom = shape.geometry.values[0]

    maap = MAAP()
    username = maap.profile.account_info()['username']
    temp_dir = f"s3://maap-ops-workspace/{username}/.tmp/duckdb_tmp"
    con = ducky.init_duckdb(temp_dir)
    data_spec = ducky.brazil_data_spec()

    res = crossovers.find_repeat_footprints(
        con,
        data_spec,
        geom,
        args.distance_m,
        args.filters,
        args.columns
    )
    print(f"Found {len(res)} repeat footprint pairs.")

    res.to_parquet(args.outfile, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find repeat GEDI footprints (crossovers) using H3 spatial indexing."
    )
    parser.add_argument(
        "--shapefile",
        type=str,
        required=True,
        help="Path to shapefile defining region to search for crossovers.",
    )
    parser.add_argument(
        "--distance_m",
        type=int,
        required=True,
        default=40,
        help="Maximum distance in meters for repeat footprints."
    )
    # repeat list of columns
    parser.add_argument(
        "--columns",
        nargs='*',
        help="List of columns to include in the output data. Shot number, latitude, longitude, and metric distance between footprints are always included.",
    )
    parser.add_argument(
        "--filters",
        type=str,
        required=False,
        help="String of quality filters, e.g.\n'l4_quality_flag = 1 AND sensitivity > 0.95'"
    )
    parser.add_argument(
        "--outfile",
        type=str,
        required=True,
        help="Path in which to store repeat footprints data"
    )
    args = parser.parse_args()
    main(args)