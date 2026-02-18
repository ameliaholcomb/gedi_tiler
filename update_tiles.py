import argparse

from gtiler.database import ducky

def main(args):
    # 1. Create a new database containing only updated years
    # TODO: Could do some validation checks here.
    command = f"""python3 tile_runner.py \
               --bucket {args.bucket} \
               --prefix {args.prefix}_updates \
               --shapefile {args.shapefile} \
               --start_year {args.start_year} \
               --end_year {args.end_year}"""
    print("Create an updates table using the following command:")
    print(command)
    input("When these jobs have completed, press ENTER to continue >>>")

    # 2. Merge the update data into the main tiled database
    command = f"""aws s3 mv \
                s3://{args.bucket}/{args.prefix}_updates/data \
                s3://{args.bucket}/{args.prefix}/data \
                --recursive"""
    print("Merge the update data into the main tiled database using the following command:")
    print(command)
    input("When this command has completed, press ENTER to continue >>>")

    # 3. Update the metadata
    con = ducky.init_duckdb()
    if args.save_md:
        # copy existing metadata to metadata_old
        existing_md_prefix = ducky.metadata_prefix(args.bucket, args.prefix)
        command = f"""aws s3 cp \
                    {existing_md_prefix} \
                    {existing_md_prefix.rstrip('/')}_old/ \
                    --recursive"""
        print("Back up existing metadata using the following command:")
        print(command)
        input("When this command has completed, press ENTER to continue >>>")
    
    existing_md_spec = ducky.metadata_spec(args.bucket, args.prefix)
    existing_md = con.sql(f"SELECT * FROM read_parquet('{existing_md_spec}')")
    update_md_spec = ducky.metadata_spec(args.bucket, f"{args.prefix}_updates")
    update_md = con.sql(f"SELECT * FROM read_parquet('{update_md_spec}')")
    
    # Merge the old and new metadata.
    # Choose the most recent cmr_access_time timestamp to resolve duplicates
    # because the S3 mv command above overwrites old data from a given year.
    full_md = con.sql("""--sql
        WITH CombinedData AS (
            -- Step 1: Combine all rows from both tables.
            SELECT * FROM existing_md
            UNION ALL
            SELECT * FROM update_md
        ),
        RankedData AS (
            -- Step 2: Rank the rows within each (granule_id, tile_id) partition.
            SELECT 
                *,
                -- Order duplicate (granule_id, tile_id) rows based on the timestamp in 
                -- DESCending order, meaning the most recent timestamp gets rank 1.
                ROW_NUMBER() OVER (
                    PARTITION BY granule_id, tile_id
                    ORDER BY cmr_access_time DESC
                ) AS rn
            FROM CombinedData
        )
        -- Step 3: Select only the row with the most recent timestamp.
        SELECT *
        FROM RankedData
        WHERE rn = 1;
    """)
    md_prefix = ducky.metadata_prefix(args.bucket, args.prefix)
    con.sql(f"""
        COPY full_md TO '{md_prefix}' (
        FORMAT PARQUET,
        PARTITION_BY (tile_id),
        COMPRESSION zstd,
        OVERWRITE_OR_IGNORE
        );
    """)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update tiles in the tiled GEDI database."
    )
    parser.add_argument(
        "--bucket",
        type=str,
        required=True,
        help="S3 bucket where tiled GEDI data is stored.",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        required=True,
        help="S3 prefix where tiled GEDI data is stored.",
    )
    parser.add_argument(
        "--shapefile",
        type=str,
        required=True,
        help="Path to shapefile defining region to update.",
    )
    parser.add_argument(
        "--start_year",
        type=int,
        required=True,
        help="Start year of data to update (inclusive).",
    )
    parser.add_argument(
        "--end_year",
        type=int,
        required=True,
        help=("End year of data to update (inclusive).\n"
              "To update one year, set both start_year and end_year to the same value."),
    )
    parser.add_argument(
        "--save_md",
        action="store_true",
        help="Save old metadata files to metadata_old.",
    )
    args = parser.parse_args()
    main(args)



