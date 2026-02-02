from geopandas import gpd
from shapely import geometry, orient_polygons


class DetailError(Exception):
    """Used when too many points in a shape for NASA's API"""

    def __init__(self, n_coords: int):
        self.n_coords = n_coords


def get_covering_region_for_shape(
    shp: gpd.GeoDataFrame, tile_size: int = 1
) -> gpd.GeoDataFrame:
    """The NASA CMR API can only handle shapes with less than 5000 points.
    To simplify shapes without adding lots of extra area
    (as a bounding box or convex hull would), we instead tile the region into
    covering 1x1 degree boxes, and return the union of those boxes.
    """
    step = tile_size
    # generate all the tiles in the world
    minx = -180
    maxx = 180
    miny = -90
    maxy = 90
    tiles = []
    for i in range(minx, maxx, step):
        for j in range(maxy, miny, -step):
            tile = geometry.box(i, j - step, i + step, j)
            tiles.append(tile)
    tile_df = gpd.GeoDataFrame(geometry=tiles, crs="EPSG:4326")

    covering_tiles = tile_df.sjoin(shp, how="inner", predicate="intersects")
    covering = gpd.GeoSeries(covering_tiles.union_all(), crs="EPSG:4326")
    return covering


def get_n_coords(shp: gpd.GeoDataFrame) -> int:
    """Returns the number of coordinates in a shape"""
    n_coords = 0
    for row in shp.geometry:
        if row.geom_type.startswith("Multi"):
            n_coords += sum([len(part.exterior.coords) for part in row.geoms])
        else:
            n_coords += len(row.exterior.coords)
    return n_coords


def orient_shape(shp: gpd.GeoDataFrame, exterior_cw: bool) -> gpd.GeoSeries:
    """Orients the shape(s) in a GeoDataFrame to be clockwise"""
    return shp.geometry.apply(orient_polygons, exterior_cw=exterior_cw)


def close_holes(shp: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Closes any holes in polygons in a GeoDataFrame"""

    def _close_holes_in_polygon(polygon: geometry.Polygon) -> geometry.Polygon:
        return geometry.Polygon(polygon.exterior)

    def _close_holes_in_geometry(
        geom: geometry.base.BaseGeometry,
    ) -> geometry.base.BaseGeometry:
        if geom.geom_type == "Polygon":
            return _close_holes_in_polygon(geom)
        elif geom.geom_type == "MultiPolygon":
            closed_parts = [
                _close_holes_in_polygon(part) for part in geom.geoms
            ]
            return geometry.MultiPolygon(closed_parts)
        else:
            return geom  # Return as is if not Polygon or MultiPolygon

    closed_geometries = shp.geometry.apply(_close_holes_in_geometry)
    return gpd.GeoDataFrame(
        shp.drop(columns="geometry"), geometry=closed_geometries, crs=shp.crs
    )


def check_and_format_shape(
    shp: gpd.GeoDataFrame,
    simplify: bool = False,
    max_coords: int = 500,
    exterior_cw: bool = True,
) -> gpd.GeoSeries:
    """
    Checks a shape for compatibility with NASA's API.

    Args:
        shp (gpd.GeoDataFrame): The shape to check and format.
        simplify (bool): Whether to simplify the shape if it doesn't meet the max_coords threshold.
        max_coords (int): Threshold for simplifying, must be less than 500.
        exterior_cw (bool): Whether to orient exteriors clockwise (True) or counter-clockwise (False).
            NOTE: NASA's API has different requirements for different shape formats:
            - ESRI Shapefile zipped: exterior_cw=True
            - GeoJSON: exterior_cw=False
            - Spatial params: exterior_cw=False
            - KML: CURRENTLY NOT SUPPORTED (exteriors AND interiors would need to be ccw)

    Raises:
        ValueError: If max_coords is not less than 500 or more than one polygon is supplied.
        DetailError: If simplify is not true and the shape does not have less than max_coord points.

    Returns:
        GeoSeries: The possibly simplified shape.
    """
    # Though the NASA API technically specifies a limit of 5000 points,
    # in practice it can only accept URLs of length up to 6000 characters.
    # Since each point is typically ~12 characters (e.g. -123.0,45.0),
    # we set a more convservative limit of 500 points here.
    if max_coords > 500:
        raise ValueError("NASA's API can only cope with less than 500 points")

    n_coords = get_n_coords(shp)
    if n_coords > max_coords:
        if not simplify:
            raise DetailError(n_coords)
        shp = get_covering_region_for_shape(shp, tile_size=5)
        n_coords = get_n_coords(shp)
        if n_coords > max_coords:
            raise ValueError(
                f"""Covering region still has {n_coords},
                but max_coords is {max_coords}"""
            )

    # The NASA API has some complicated and undocumented requirements
    # about polygon holes. So for simplicity, we just close any holes.
    shp = close_holes(shp)
    return orient_shape(shp, exterior_cw=exterior_cw)
