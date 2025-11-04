import geopandas as gpd
import re
from shapely.geometry import box
from typing import Tuple


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
    # generate all the tiles in the world
    minx = -180
    maxx = 179
    miny = -89
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
