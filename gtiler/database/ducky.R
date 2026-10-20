# Alternative to ducky.py for R users
library("duckdb")
library("sf")

# Constants
TILE_ID <- "tile_id"
YEAR <- "year"



init_duckdb <- function(temp_dir = "/tmp/duckdb") {
  # integer64: shot numbers are too large to hold exactly in a double.
  con <- dbConnect(duckdb(), bigint = "integer64")
  for (ext in c("spatial", "aws", "httpfs", "icu", "ducklake")) {
    dbExecute(con, sprintf("INSTALL %s; LOAD %s;", ext, ext))
  }
  # Set AWS credentials explicitly; using the credential chain sometimes
  # fetches the incorrect credentials on the MAAP Hub.
  env <- system2("aws", c("configure", "export-credentials", "--format", "env-no-export"), stdout = TRUE)
  creds <- setNames(sub("^[^=]*=", "", env), sub("=.*", "", env))
  dbExecute(con, sprintf("
    CREATE OR REPLACE SECRET (
      TYPE s3,
      KEY_ID '%s',
      SECRET '%s',
      SESSION_TOKEN '%s',
      REGION 'us-west-2'
    )
  ", creds[["AWS_ACCESS_KEY_ID"]], creds[["AWS_SECRET_ACCESS_KEY"]], creds[["AWS_SESSION_TOKEN"]]))
  dbExecute(con, "SET preserve_insertion_order = false")
  dbExecute(con, "SET memory_limit = '8GB'")
  # date_part on a timestamptz follows the session zone, and the year
  # it returns decides which partition a footprint lands in.
  dbExecute(con, "SET TimeZone = 'UTC'")
  dbExecute(con, sprintf("SET temp_directory = '%s'", temp_dir))
  dbExecute(con, "SET max_temp_directory_size = '100GB'")
  return(con)
}

# Attach the DuckLake catalog read-only.
# The catalog is copied to local disk first: DuckDB reading it in place
# on S3 adds ~20 s to the first query of every session, while the copy
# takes a couple of seconds.
attach_ducklake <- function(con, bucket, prefix, name = "gedi_dl") {
  local_dir <- file.path(tempdir(), "gtiler", bucket, prefix)
  dir.create(local_dir, recursive = TRUE, showWarnings = FALSE)
  local_path <- file.path(local_dir, "gedi.ducklake")
  src <- sprintf("s3://%s/%s/ducklake/gedi.ducklake", bucket, prefix)
  if (system2("aws", c("s3", "cp", "--quiet", src, local_path)) != 0) {
    stop("Failed to download ", src)
  }
  dbExecute(con, sprintf("ATTACH 'ducklake:%s' AS %s (READ_ONLY)", local_path, name))
  invisible(dbExecute(con, sprintf("USE %s", name)))
}

# Create a temp view `name` of the footprints in the tiles covering
# one or more regions (see `covering_tile_ids`).
# The view holds whole 1x1 degree tiles, so filter it with ST_Intersects
# for the footprints in the region itself. It lasts for this connection:
# run set_region again after reconnecting or changing the regions.
# `table` defaults to the data table of the one attached DuckLake.
set_region <- function(con, regions, name = "gedi_region", table = NULL) {
  if (is.null(table)) {
    lakes <- dbGetQuery(con,
      "SELECT database_name FROM duckdb_databases() WHERE type = 'ducklake'"
    )$database_name
    if (length(lakes) != 1) {
      stop(sprintf("Found DuckLakes [%s]: pass table= to choose.", paste(lakes, collapse = ", ")))
    }
    table <- paste0(lakes, ".data")
  }
  tile_ids <- covering_tile_ids(regions)
  dbExecute(con, sprintf(
    "CREATE OR REPLACE TEMP VIEW %s AS SELECT * FROM %s WHERE %s",
    name, table, tile_clause(tile_ids)
  ))
  message(sprintf("%s: %d tile%s", name, length(tile_ids), if (length(tile_ids) == 1) "" else "s"))
}

# Collect a DuckDB table (e.g. from dplyr::tbl) into an sf data frame.
# If multiple geometry columns are specified,
# the first will be set as the active geometry.
duck_to_sf <- function(table, geometry_columns = "geometry", crs = 4326) {
  wkb <- lapply(geometry_columns, function(col) dbplyr::sql(sprintf('ST_AsWKB("%s")', col)))
  df <- dplyr::collect(dplyr::mutate(table, !!!setNames(wkb, geometry_columns)))
  for (col in geometry_columns) {
    df[[col]] <- st_as_sfc(structure(df[[col]], class = "WKB"), crs = crs)
  }
  return(st_sf(df, sf_column_name = geometry_columns[1]))
}

# Load an sf data frame into a DuckDB temp table `name`, keeping its CRS.
sf_to_duck <- function(con, x, name) {
  col <- attr(x, "sf_column")
  df <- st_drop_geometry(x)
  df[[col]] <- blob::as_blob(unclass(st_as_binary(st_geometry(x))))
  duckdb::duckdb_register(con, "sf_to_duck_wkb", df)
  on.exit(duckdb::duckdb_unregister(con, "sf_to_duck_wkb"))
  dbExecute(con, sprintf(
    "CREATE OR REPLACE TEMP TABLE %s AS SELECT * REPLACE (ST_SetCRS(ST_GeomFromWKB(\"%s\"), '%s') AS \"%s\") FROM sf_to_duck_wkb",
    name, col, st_crs(x)$srid, col
  ))
  return(dplyr::tbl(con, name))
}

topleft_to_text <- function(xmin, ymax) {
  lon_ew <- ifelse(xmin < 0, "W", "E")
  lat_ns <- ifelse(ymax < 0, "S", "N")
  return(sprintf("%s%02d_%s%03d", lat_ns, abs(ymax), lon_ew, abs(xmin)))
}

# IDs of the tiles covering one or more regions: an sf or sfc object,
# or a list of them, each in its own CRS.
covering_tile_ids <- function(regions) {
  if (inherits(regions, c("sf", "sfc"))) {
    regions <- list(regions)
  }
  return(sort(unique(unlist(lapply(regions, region_tile_ids))), method = "radix"))
}

# IDs of the 1x1 degree tiles that intersect a region in any CRS.
# The tiles are densified and reprojected into the region's CRS, rather
# than the region into lon/lat, so the region is matched exactly as drawn.
region_tile_ids <- function(region) {
  # Tile edges are straight lines in lon/lat, not great circles: keep s2 off,
  # and densify with no CRS so that st_segmentize interpolates coordinates.
  old_s2 <- suppressMessages(sf_use_s2(FALSE))
  on.exit(suppressMessages(sf_use_s2(old_s2)))
  crs <- st_crs(region)
  region <- suppressMessages(st_union(st_geometry(region)))
  # Candidates: the region's lon/lat bounds, plus a tile on each side. The
  # region is densified first so that edges curving in lon/lat count.
  bounds <- st_bbox(region)
  step <- max(bounds$xmax - bounds$xmin, bounds$ymax - bounds$ymin) / 1000
  dense <- if (step > 0) st_set_crs(st_segmentize(st_set_crs(region, NA), step), crs) else region
  ll <- st_bbox(st_transform(dense, 4326))
  lons <- max(-180, floor(ll$xmin) - 1):min(179, ceiling(ll$xmax))
  lats <- max(-89, ceiling(ll$ymin) - 1):min(90, ceiling(ll$ymax) + 1)
  tiles <- expand.grid(x = lons, y = lats)
  boxes <- st_sfc(mapply(function(x, y) {
    st_polygon(list(rbind(c(x, y - 1), c(x + 1, y - 1), c(x + 1, y), c(x, y), c(x, y - 1))))
  }, tiles$x, tiles$y, SIMPLIFY = FALSE))
  projected <- st_transform(st_set_crs(st_segmentize(boxes, 0.01), 4326), crs)
  hits <- lengths(suppressMessages(st_intersects(projected, region))) > 0
  return(topleft_to_text(tiles$x[hits], tiles$y[hits]))
}

# A literal IN list: DuckDB skips files on it exactly, but plans
# IN (subquery) and list_contains with no or only min/max pruning.
tile_clause <- function(tile_ids) {
  return(sprintf("tile_id IN (%s)", paste(sprintf("'%s'", tile_ids), collapse = ", ")))
}

# Create a filter clause to help DuckDB look at only the tiles
# covering one or more regions (see `covering_tile_ids`).
spatial_filter_clause <- function(regions) {
  return(tile_clause(covering_tile_ids(regions)))
}

get_covering_tiles_wkt <- function(region_wkt) {
  region_sf <- st_as_sf(data.frame(wkt_geometry = region_wkt), wkt = "wkt_geometry")
  return(get_covering_tiles(region_sf))
}

get_covering_tiles <- function(region_sf) {
  # Create 1x1 degree tiles covering the globe
  xs <- -180:179
  ys <- -90:89

  data_points <- expand.grid(x_min = xs, y_min = ys)
  data_points$x_max <- data_points$x_min + 1
  data_points$y_max <- data_points$y_min + 1

  bbox_geometries <- lapply(1:nrow(data_points), function(i) {
    st_polygon(list(matrix(c(
      data_points$x_min[i], data_points$y_min[i],
      data_points$x_max[i], data_points$y_min[i],
      data_points$x_max[i], data_points$y_max[i],
      data_points$x_min[i], data_points$y_max[i],
      data_points$x_min[i], data_points$y_min[i] 
    ), ncol = 2, byrow = TRUE)))
  })
  tile_ids <- mapply(topleft_to_text, data_points$x_min, data_points$y_max)

  bbox_sf <- st_as_sf(
    data.frame(
      tile_id = tile_ids,
      geometry = st_sfc(bbox_geometries)
    ), crs = 4326
  )
  # Figure out which tiles intersect the region
  region_sf <- st_transform(region_sf, crs = 4326)
  return(st_join(bbox_sf, region_sf, join = st_intersects, left = FALSE)$tile_id)
}
