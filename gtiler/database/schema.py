from dataclasses import dataclass
from enum import Enum


class GediProduct(Enum):
    L2A = "level2A"
    L2B = "level2B"
    L3 = "level3"
    L4A = "level4A"
    L4B = "level4B"
    L4C = "level4C"


@dataclass
class Column:
    variable: str
    SDS_Name: str
    is_profile: bool = False


@dataclass
class DerivedColumn:
    "A column not in the original GEDI dataset, derived from one or more source columns."

    variable: str
    # The following are provided for documentation purposes only.
    SDS_Name: str
    description: str
    product_level: list[GediProduct]
    dtype: str
    unit: str


@dataclass
class GeometryColumn:
    lat: Column
    lon: Column


@dataclass
class Product:
    variables: list[Column]
    product_level: GediProduct
    primary_key: Column
    geometry: GeometryColumn


@dataclass
class Derived:
    variables: list[DerivedColumn]


@dataclass
class Table:
    name: str
    description: str
    products: list[Product]
    derived: list[DerivedColumn]


# Define the schema for the tiled GEDI database.
# fmt: off
SCHEMA = Table(
    name="tiled_gedi_database",
    description="Tiled GEDI database containing all available GEDI data products.",
    products=[
        Product(
            product_level=GediProduct.L2A,
            primary_key=Column(variable="shot_number", SDS_Name="shot_number"),
            geometry=GeometryColumn(
                lat=Column(variable="lat_lowestmode", SDS_Name="lat_lowestmode"),
                lon=Column(variable="lon_lowestmode", SDS_Name="lon_lowestmode"),
            ),
            variables=[
                Column(variable="elev_lowestmode", SDS_Name="elev_lowestmode"),
                Column(variable="delta_time", SDS_Name="delta_time"),
                Column(variable="sensitivity", SDS_Name="sensitivity"),
                Column(variable="sensitivity_a1", SDS_Name="geolocation/sensitivity_a1"),
                Column(variable="sensitivity_a2", SDS_Name="geolocation/sensitivity_a2"),
                Column(variable="degrade_flag", SDS_Name="degrade_flag"),
                Column(variable="quality_flag", SDS_Name="quality_flag"),
                Column(variable="landsat_treecover", SDS_Name="land_cover_data/landsat_treecover"),
                Column(variable="modis_treecover", SDS_Name="land_cover_data/modis_treecover"),
                Column(variable="modis_treecover_sd", SDS_Name="land_cover_data/modis_treecover_sd"),
                Column(variable="modis_nonvegetated", SDS_Name="land_cover_data/modis_nonvegetated"),
                Column(variable="modis_nonvegetated_sd", SDS_Name="land_cover_data/modis_nonvegetated_sd"),
                Column(variable="solar_elevation", SDS_Name="solar_elevation"),
                Column(variable="solar_azimuth", SDS_Name="solar_azimuth"),
                Column(variable="energy_total", SDS_Name="energy_total"),
                Column(variable="digital_elevation_model", SDS_Name="digital_elevation_model"),
                Column(variable="digital_elevation_model_srtm", SDS_Name="digital_elevation_model_srtm"),
                Column(variable="num_detectedmodes", SDS_Name="num_detectedmodes"),
                Column(variable="rh", SDS_Name="rh", is_profile=True),
                Column(variable="selected_algorithm", SDS_Name="selected_algorithm"),
                Column(variable="surface_flag", SDS_Name="surface_flag"),
                # TODO should we also do elev_highestreturn?
                Column(variable="elev_highestreturn_a1", SDS_Name="geolocation/elev_highestreturn_a1"),
                Column(variable="elev_highestreturn_a2", SDS_Name="geolocation/elev_highestreturn_a2"),
                Column(variable="stale_return_flag", SDS_Name="geolocation/stale_return_flag"),
                Column(variable="rx_maxamp", SDS_Name="rx_assess/rx_maxamp"),
                Column(variable="sd_corrected", SDS_Name="rx_assess/sd_corrected"),
                Column(variable="rx_algrunflag", SDS_Name="rx_processing_a2/rx_algrunflag"),
                Column(variable="zcross", SDS_Name="rx_processing_a2/zcross"),
                Column(variable="toploc", SDS_Name="rx_processing_a2/toploc"),
            ]
        ),
        Product(
            product_level=GediProduct.L2B,
            primary_key=Column(variable="shot_number", SDS_Name="shot_number"),
            geometry=GeometryColumn(
                lat=Column(variable="lat_lowestmode", SDS_Name="geolocation/lat_lowestmode"),
                lon=Column(variable="lon_lowestmode", SDS_Name="geolocation/lon_lowestmode")
            ),
            variables=[
                Column(variable="l2b_algorithm_run_flag", SDS_Name="algorithmrun_flag"),
                Column(variable="l2a_quality_flag", SDS_Name="l2a_quality_flag"),
                Column(variable="l2b_quality_flag", SDS_Name="l2b_quality_flag"),
                Column(variable="cover", SDS_Name="cover"),
                Column(variable="cover_z", SDS_Name="cover_z", is_profile=True),
                Column(variable="fhd_normal", SDS_Name="fhd_normal"),
                Column(variable="omega", SDS_Name="omega"),
                Column(variable="pai", SDS_Name="pai"),
                Column(variable="pai_z", SDS_Name="pai_z", is_profile=True),
                Column(variable="pavd_z", SDS_Name="pavd_z", is_profile=True),
                Column(variable="pgap_theta", SDS_Name="pgap_theta"),
                Column(variable="pgap_theta_error", SDS_Name="pgap_theta_error"),
                Column(variable="rg", SDS_Name="rg"),
                Column(variable="rhog", SDS_Name="rhog"),
                Column(variable="rhog_error", SDS_Name="rhog_error"),
                Column(variable="rhov", SDS_Name="rhov"),
                Column(variable="rhov_error", SDS_Name="rhov_error"),
                Column(variable="rossg", SDS_Name="rossg"),
                Column(variable="rv", SDS_Name="rv"),
                Column(variable="rx_range_highestreturn", SDS_Name="rx_range_highestreturn"),
                Column(variable="selected_l2a_algorithm", SDS_Name="selected_l2a_algorithm"),
                Column(variable="selected_rg_algorithm", SDS_Name="selected_rg_algorithm"),
                Column(variable="dz", SDS_Name="ancillary/dz"),
            ]
        ),
        Product(
            product_level=GediProduct.L4A,
            primary_key=Column(variable="shot_number", SDS_Name="shot_number"),
            geometry=GeometryColumn(
                lat=Column(variable="lat_lowestmode", SDS_Name="lat_lowestmode"),
                lon=Column(variable="lon_lowestmode", SDS_Name="lon_lowestmode")
            ),
            variables=[
                Column(variable="l2_quality_flag", SDS_Name="l2_quality_flag"),
                Column(variable="l4_quality_flag", SDS_Name="l4_quality_flag"),
                Column(variable="l4_algorithm_run_flag", SDS_Name="algorithm_run_flag"),
                Column(variable="xvar", SDS_Name="xvar"),
                Column(variable="predictor_limit_flag", SDS_Name="predictor_limit_flag"),
                Column(variable="response_limit_flag", SDS_Name="response_limit_flag"),
                Column(variable="agbd", SDS_Name="agbd"),
                Column(variable="agbd_pi_lower", SDS_Name="agbd_pi_lower"),
                Column(variable="agbd_pi_upper", SDS_Name="agbd_pi_upper"),
                Column(variable="agbd_se", SDS_Name="agbd_se"),
                Column(variable="agbd_t", SDS_Name="agbd_t"),
                Column(variable="agbd_t_se", SDS_Name="agbd_t_se"),
                Column(variable="predict_stratum", SDS_Name="predict_stratum"),
                Column(variable="landsat_water_persistence", SDS_Name="land_cover_data/landsat_water_persistence"),
                Column(variable="leaf_off_flag", SDS_Name="land_cover_data/leaf_off_flag"),
                Column(variable="leaf_off_doy", SDS_Name="land_cover_data/leaf_off_doy"),
                Column(variable="leaf_on_cycle", SDS_Name="land_cover_data/leaf_on_cycle"),
                Column(variable="leaf_on_doy", SDS_Name="land_cover_data/leaf_on_doy"),
                Column(variable="urban_proportion", SDS_Name="land_cover_data/urban_proportion"),
                Column(variable="pft_class", SDS_Name="land_cover_data/pft_class"),
                Column(variable="region_class", SDS_Name="land_cover_data/region_class"),
            ]
        ),
        Product(
            product_level=GediProduct.L4C,
            primary_key=Column(variable="shot_number", SDS_Name="shot_number"),
            geometry=GeometryColumn(
                lat=Column(variable="lat_lowestmode", SDS_Name="lat_lowestmode"),
                lon=Column(variable="lon_lowestmode", SDS_Name="lon_lowestmode")
            ),
            variables=[
                Column(variable="wsci", SDS_Name="wsci"),
                Column(variable="wsci_pi_lower", SDS_Name="wsci_pi_lower"),
                Column(variable="wsci_pi_upper", SDS_Name="wsci_pi_upper"),
                Column(variable="wsci_quality_flag", SDS_Name="wsci_quality_flag"),
                Column(variable="wsci_xy", SDS_Name="wsci_xy"),
                Column(variable="wsci_xy_pi_lower", SDS_Name="wsci_xy_pi_lower"),
                Column(variable="wsci_xy_pi_upper", SDS_Name="wsci_xy_pi_upper"),
                Column(variable="wsci_z", SDS_Name="wsci_z"),
                Column(variable="wsci_z_pi_lower", SDS_Name="wsci_z_pi_lower"),
                Column(variable="wsci_z_pi_upper", SDS_Name="wsci_z_pi_upper"),
            ]
        )
    ],    
    derived=[
        DerivedColumn(
            variable="absolute_time",
            SDS_Name="delta_time",
            description="Timestamp of GEDI footprint, derived from delta_time",
            product_level=[GediProduct.L2A],
            dtype="datetime64[ns, UTC]",
            unit="UTC timestamp"
        ),
        DerivedColumn(
            variable="granule",
            SDS_Name="N/A",
            description="Granule name, derived from input file name",
            product_level=[GediProduct.L2A, GediProduct.L2B, GediProduct.L4A, GediProduct.L4C],
            dtype="string[10]",
            unit="N/A"
        ),
        DerivedColumn(
            variable="tile_id",
            SDS_Name="N/A",
            description="1x1 degree tile ID containing the GEDI footprint, derived from lat/lon",
            product_level=[GediProduct.L2A, GediProduct.L2B, GediProduct.L4A, GediProduct.L4C],
            dtype="string[7]",
            unit="N/A"
        ),
        DerivedColumn(
            variable="geometry",
            SDS_Name="lat_lowestmode/lon_lowestmode",
            description="geometry column containing a point with the lat/lon of the GEDI footprint",
            product_level=[GediProduct.L2A, GediProduct.L2B, GediProduct.L4A, GediProduct.L4C],
            dtype="geometry(Point)",
            unit="EPSG:4326"
        ),
        DerivedColumn(
            variable="beam_name",
            SDS_Name="N/A",
            description="Name of the GEDI beam (e.g. BEAM0001), derived from h5 groups",
            product_level=[GediProduct.L2A, GediProduct.L2B, GediProduct.L4A, GediProduct.L4C],
            dtype="string[7]",
            unit="N/A"
        )
    ]
)
# fmt: on
