import geopandas as gpd
import math
import re
import shapely
from shapely.geometry import box
from typing import List, Tuple


class Tile:
    minx: float
    maxx: float
    miny: float
    maxy: float
    id: str
    shape: gpd.GeoSeries

    def __init__(self, tile_id: str):
        self.id = tile_id
        retile = re.compile(r"^([NS])(\d{2})_([EW])(\d{3})$")
        if not retile.match(tile_id):
            raise ValueError(f"{tile_id} invalid: must be [NS][DD]_[EW][DDD].")
        match = retile.match(tile_id)
        ns = match.group(1)
        ew = match.group(3)
        lat = int(match.group(2))
        lon = int(match.group(4))
        if ns == "S":
            lat = -lat
        if ew == "W":
            lon = -lon
        self.minx = lon
        self.maxy = lat
        self.maxx = lon + 1
        self.miny = lat - 1
        self.shape = box(self.minx, self.miny, self.maxx, self.maxy)


def _to_nesw(
    lon: float, lat: float
) -> tuple[tuple[float, float], tuple[str, str]]:
    lon_ew = "W" if lon < 0 else "E"
    lat_ns = "S" if lat < 0 else "N"
    return (abs(lon), abs(lat)), (lon_ew, lat_ns)


def _to_text(lon: float, lat: float) -> str:
    (lon_abs, lat_abs), (lon_dir, lat_dir) = _to_nesw(lon, lat)
    return f"{lat_dir}{int(lat_abs):02d}_{lon_dir}{int(lon_abs):03d}"


def get_covering_tiles_for_region(
    shape: gpd.GeoDataFrame,
) -> Tuple[gpd.GeoDataFrame, gpd.GeoSeries]:
    """Get the 1x1 degree tiles that cover a region.
    Args:
        shape: region to cover
    Returns:
        tuple of (dataframe of tiles, total geometry of covering region)
    """
    # generate all the tiles in the world, by upper-left corner;
    # the range ends are exclusive.
    minx = -180
    maxx = 180
    miny = -90
    maxy = 90
    tiles = []
    for i in range(minx, maxx):
        for j in range(maxy, miny, -1):
            tile = Tile(_to_text(i, j))
            tiles.append(tile)
    tile_df = gpd.GeoDataFrame(
        {"tile_id": [tile.id for tile in tiles]},
        geometry=[tile.shape for tile in tiles],
        crs="EPSG:4326",
    )

    covering_tiles = tile_df.sjoin(
        shape, how="inner", predicate="intersects"
    ).drop_duplicates(subset=["tile_id", "geometry"])
    covering_tiles = covering_tiles[["tile_id", "geometry"]]
    covering = gpd.GeoSeries(covering_tiles.union_all(), crs="EPSG:4326")
    return covering_tiles.reset_index(drop=True), covering


def get_covering_tile_ids(shape: gpd.GeoDataFrame) -> List[str]:
    """IDs of the 1x1 degree tiles that intersect a region in any CRS.

    The tiles are densified and reprojected into the region's CRS, rather
    than the region into lon/lat, so the region is matched exactly as drawn.
    """
    region = shape.union_all()
    # Candidates: the region's lon/lat bounds, plus a tile on each side. The
    # region is densified first so that edges curving in lon/lat count.
    minx, miny, maxx, maxy = region.bounds
    step = max(maxx - minx, maxy - miny) / 1000
    dense = shapely.segmentize(region, step) if step > 0 else region
    lon0, lat0, lon1, lat1 = (
        gpd.GeoSeries([dense], crs=shape.crs).to_crs("EPSG:4326").total_bounds
    )
    lons = range(max(-180, math.floor(lon0) - 1), min(180, math.ceil(lon1) + 1))
    lats = range(max(-89, math.ceil(lat0) - 1), min(90, math.ceil(lat1) + 1) + 1)
    ids = [_to_text(x, y) for x in lons for y in lats]
    boxes = [box(x, y - 1, x + 1, y) for x in lons for y in lats]
    projected = gpd.GeoSeries(
        shapely.segmentize(boxes, 0.01), crs="EPSG:4326"
    ).to_crs(shape.crs)
    return [i for i, hit in zip(ids, projected.intersects(region)) if hit]
