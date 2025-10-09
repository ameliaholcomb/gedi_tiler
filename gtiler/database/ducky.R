# Alternative to ducky.py for R users
library("duckdb")
devtools::install_github("cboettig/duckdbfs")
library("duckdbfs")
library("sf")

# Constants
TILE_ID <- "tile_id"
YEAR <- "year"



demo_data_spec <- function() {
    return("s3://maap-ops-workspace/shared/ameliah/gedi-test/brazil_tiles/data/")
}

get_gedi_demo_table <- function() {
    data_spec <- demo_data_spec()
    s3_region <- "us-west-2"
    duckdbfs::duckdb_s3_config()
    duckdbfs::load_spatial()
    gedi <- duckdbfs::open_dataset(data_spec, anonymous = TRUE, s3_region=s3_region)
}


topleft_to_text <- function(xmin, ymax) {
  lon_ew <- if (xmin < 0) "W" else "E"
  lat_ns <- if (ymax < 0) "S" else "N"
  return(sprintf("%s%02d_%s%03d", lat_ns, abs(ymax), lon_ew, abs(xmin)))
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
