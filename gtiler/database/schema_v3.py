from dataclasses import dataclass
from enum import Enum


class GediProduct(Enum):
    L2A = "level2A"
    L2B = "level2B"
    L3 = "level3"
    L4A = "level4A"
    L4B = "level4B"
    L4C = "level4C"


# Column types, as numpy dtype names ("str" for fixed-width byte strings),
# with the pandas dtype that holds them with nulls and the DuckDB type they
# are written as.
NULLABLE_DTYPES = {
    "bool": "boolean",
    "uint8": "UInt8",
    "uint16": "UInt16",
    "int16": "Int16",
    "uint64": "UInt64",
    "float32": "Float32",
    "float64": "Float64",
    "str": "string",
}
DUCKDB_TYPES = {
    "bool": "BOOLEAN",
    "uint8": "UTINYINT",
    "uint16": "USMALLINT",
    "int16": "SMALLINT",
    "uint64": "UBIGINT",
    "float32": "FLOAT",
    "float64": "DOUBLE",
    "str": "VARCHAR",
    "datetime64[ns, UTC]": "TIMESTAMPTZ",
}

# The version of the tile-year file layout, recorded in each file's
# parquet key-value metadata under VERSION_KEY. Version 4 stores each
# profile as one list column; version 3 files (and those recording no
# version) flattened profiles into one column per bin.
SCHEMA_VERSION = 4
VERSION_KEY = "gtiler_schema_version"


def file_version(key_value_metadata) -> int:
    """The layout version a file records, given its parquet key-value
    metadata (pyarrow's FileMetaData.metadata). Files from before versions
    were recorded are version 3."""
    return int((key_value_metadata or {}).get(VERSION_KEY.encode(), b"3"))

# DuckDB COPY options for tile-year files. zstd decompresses at the same
# speed whatever the level, so a high level costs only write time.
# PARQUET_VERSION V2 adds encodings (BYTE_STREAM_SPLIT for floats, delta
# for integers and strings) that shrink the files.
TILE_ROW_GROUP_SIZE = 200_000
TILE_COPY_OPTIONS = f"""
    FORMAT parquet,
    GEOPARQUET_VERSION 'V2',
    PARQUET_VERSION V2,
    COMPRESSION zstd,
    COMPRESSION_LEVEL 9,
    ROW_GROUP_SIZE {TILE_ROW_GROUP_SIZE},
    KV_METADATA {{{VERSION_KEY}: '{SCHEMA_VERSION}'}}
"""


@dataclass
class Column:
    variable: str
    SDS_Name: str
    # The dataset's type in the V003 granules. Taken from the files, which
    # agree with the V003 data dictionaries except that the dictionaries
    # give L4A degrade_include_flag as UINT8 (it is bool) and
    # elev_highestreturn_outlier_flag as FLOAT32 (it is uint8).
    dtype: str
    # A profile is a 2-D dataset, stored as one list column of its bins
    # along the second axis (e.g. rh_l2a[1] .. rh_l2a[101], a FLOAT[]).
    is_profile: bool = False
    # For profile columns, the number of bins: the length of every list.
    n_bins: int = 0

    @property
    def duckdb_type(self) -> str:
        base = DUCKDB_TYPES[self.dtype]
        return f"{base}[]" if self.is_profile else base


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


PRIMARY_KEY = Column(variable="shot_number", SDS_Name="shot_number", dtype="uint64")
SHOT_GEOMETRY = GeometryColumn(
    lat=Column(variable="lat_lowestmode", SDS_Name="lat_lowestmode", dtype="float64"),
    lon=Column(variable="lon_lowestmode", SDS_Name="lon_lowestmode", dtype="float64"),
)

# Define the schema for the tiled GEDI v3 database.
# Source variables are named <group>_<name>_<product> (e.g. agbd_l4a);
# profiles are list columns (rh_l2a, with rh_l2a[99] the 98th percentile).
# A variable repeated across products is kept only from the lowest product
# level.
# fmt: off
SCHEMA = Table(
    name="tiled_gedi_database_v3",
    description="Tiled GEDI v3 database containing all available GEDI data products.",
    products=[

        Product(
            product_level=GediProduct.L2A,
            primary_key=PRIMARY_KEY,
            geometry=SHOT_GEOMETRY,
            variables=[
                Column(variable="beam_l2a", SDS_Name="beam", dtype="uint16"),
                Column(variable="channel_l2a", SDS_Name="channel", dtype="uint8"),
                Column(variable="degrade_flag_l2a", SDS_Name="degrade_flag", dtype="uint8"),
                Column(variable="delta_time_l2a", SDS_Name="delta_time", dtype="float64"),
                Column(variable="digital_elevation_model_l2a", SDS_Name="digital_elevation_model", dtype="float32"),
                Column(variable="digital_elevation_model_srtm_l2a", SDS_Name="digital_elevation_model_srtm", dtype="float32"),
                Column(variable="elev_lowestmode_l2a", SDS_Name="elev_lowestmode", dtype="float32"),
                Column(variable="elevation_bias_flag_l2a", SDS_Name="elevation_bias_flag", dtype="uint8"),
                Column(variable="geolocation_elev_highestreturn_a1_l2a", SDS_Name="geolocation/elev_highestreturn_a1", dtype="float32"),
                Column(variable="geolocation_elev_highestreturn_a10_l2a", SDS_Name="geolocation/elev_highestreturn_a10", dtype="float32"),
                Column(variable="geolocation_elev_highestreturn_a2_l2a", SDS_Name="geolocation/elev_highestreturn_a2", dtype="float32"),
                Column(variable="geolocation_elev_highestreturn_a5_l2a", SDS_Name="geolocation/elev_highestreturn_a5", dtype="float32"),
                Column(variable="geolocation_elev_lowestmode_a1_l2a", SDS_Name="geolocation/elev_lowestmode_a1", dtype="float32"),
                Column(variable="geolocation_elev_lowestmode_a10_l2a", SDS_Name="geolocation/elev_lowestmode_a10", dtype="float32"),
                Column(variable="geolocation_elev_lowestmode_a2_l2a", SDS_Name="geolocation/elev_lowestmode_a2", dtype="float32"),
                Column(variable="geolocation_elev_lowestmode_a5_l2a", SDS_Name="geolocation/elev_lowestmode_a5", dtype="float32"),
                Column(variable="geolocation_elev_lowestreturn_a1_l2a", SDS_Name="geolocation/elev_lowestreturn_a1", dtype="float32"),
                Column(variable="geolocation_elev_lowestreturn_a10_l2a", SDS_Name="geolocation/elev_lowestreturn_a10", dtype="float32"),
                Column(variable="geolocation_elev_lowestreturn_a2_l2a", SDS_Name="geolocation/elev_lowestreturn_a2", dtype="float32"),
                Column(variable="geolocation_elev_lowestreturn_a5_l2a", SDS_Name="geolocation/elev_lowestreturn_a5", dtype="float32"),
                Column(variable="geolocation_l2a_quality_flag_rel3_a1_l2a", SDS_Name="geolocation/l2a_quality_flag_rel3_a1", dtype="uint8"),
                Column(variable="geolocation_l2a_quality_flag_rel3_a10_l2a", SDS_Name="geolocation/l2a_quality_flag_rel3_a10", dtype="uint8"),
                Column(variable="geolocation_l2a_quality_flag_rel3_a2_l2a", SDS_Name="geolocation/l2a_quality_flag_rel3_a2", dtype="uint8"),
                Column(variable="geolocation_l2a_quality_flag_rel3_a5_l2a", SDS_Name="geolocation/l2a_quality_flag_rel3_a5", dtype="uint8"),
                Column(variable="geolocation_lat_highestreturn_a1_l2a", SDS_Name="geolocation/lat_highestreturn_a1", dtype="float64"),
                Column(variable="geolocation_lat_highestreturn_a10_l2a", SDS_Name="geolocation/lat_highestreturn_a10", dtype="float64"),
                Column(variable="geolocation_lat_highestreturn_a2_l2a", SDS_Name="geolocation/lat_highestreturn_a2", dtype="float64"),
                Column(variable="geolocation_lat_highestreturn_a5_l2a", SDS_Name="geolocation/lat_highestreturn_a5", dtype="float64"),
                Column(variable="geolocation_lat_lowestmode_a1_l2a", SDS_Name="geolocation/lat_lowestmode_a1", dtype="float64"),
                Column(variable="geolocation_lat_lowestmode_a10_l2a", SDS_Name="geolocation/lat_lowestmode_a10", dtype="float64"),
                Column(variable="geolocation_lat_lowestmode_a2_l2a", SDS_Name="geolocation/lat_lowestmode_a2", dtype="float64"),
                Column(variable="geolocation_lat_lowestmode_a5_l2a", SDS_Name="geolocation/lat_lowestmode_a5", dtype="float64"),
                Column(variable="geolocation_lat_lowestreturn_a1_l2a", SDS_Name="geolocation/lat_lowestreturn_a1", dtype="float64"),
                Column(variable="geolocation_lat_lowestreturn_a10_l2a", SDS_Name="geolocation/lat_lowestreturn_a10", dtype="float64"),
                Column(variable="geolocation_lat_lowestreturn_a2_l2a", SDS_Name="geolocation/lat_lowestreturn_a2", dtype="float64"),
                Column(variable="geolocation_lat_lowestreturn_a5_l2a", SDS_Name="geolocation/lat_lowestreturn_a5", dtype="float64"),
                Column(variable="geolocation_lon_highestreturn_a1_l2a", SDS_Name="geolocation/lon_highestreturn_a1", dtype="float64"),
                Column(variable="geolocation_lon_highestreturn_a10_l2a", SDS_Name="geolocation/lon_highestreturn_a10", dtype="float64"),
                Column(variable="geolocation_lon_highestreturn_a2_l2a", SDS_Name="geolocation/lon_highestreturn_a2", dtype="float64"),
                Column(variable="geolocation_lon_highestreturn_a5_l2a", SDS_Name="geolocation/lon_highestreturn_a5", dtype="float64"),
                Column(variable="geolocation_lon_lowestmode_a1_l2a", SDS_Name="geolocation/lon_lowestmode_a1", dtype="float64"),
                Column(variable="geolocation_lon_lowestmode_a10_l2a", SDS_Name="geolocation/lon_lowestmode_a10", dtype="float64"),
                Column(variable="geolocation_lon_lowestmode_a2_l2a", SDS_Name="geolocation/lon_lowestmode_a2", dtype="float64"),
                Column(variable="geolocation_lon_lowestmode_a5_l2a", SDS_Name="geolocation/lon_lowestmode_a5", dtype="float64"),
                Column(variable="geolocation_lon_lowestreturn_a1_l2a", SDS_Name="geolocation/lon_lowestreturn_a1", dtype="float64"),
                Column(variable="geolocation_lon_lowestreturn_a10_l2a", SDS_Name="geolocation/lon_lowestreturn_a10", dtype="float64"),
                Column(variable="geolocation_lon_lowestreturn_a2_l2a", SDS_Name="geolocation/lon_lowestreturn_a2", dtype="float64"),
                Column(variable="geolocation_lon_lowestreturn_a5_l2a", SDS_Name="geolocation/lon_lowestreturn_a5", dtype="float64"),
                Column(variable="geolocation_num_detectedmodes_a1_l2a", SDS_Name="geolocation/num_detectedmodes_a1", dtype="uint8"),
                Column(variable="geolocation_num_detectedmodes_a10_l2a", SDS_Name="geolocation/num_detectedmodes_a10", dtype="uint8"),
                Column(variable="geolocation_num_detectedmodes_a2_l2a", SDS_Name="geolocation/num_detectedmodes_a2", dtype="uint8"),
                Column(variable="geolocation_num_detectedmodes_a5_l2a", SDS_Name="geolocation/num_detectedmodes_a5", dtype="uint8"),
                Column(variable="geolocation_sensitivity_a1_l2a", SDS_Name="geolocation/sensitivity_a1", dtype="float32"),
                Column(variable="geolocation_sensitivity_a10_l2a", SDS_Name="geolocation/sensitivity_a10", dtype="float32"),
                Column(variable="geolocation_sensitivity_a2_l2a", SDS_Name="geolocation/sensitivity_a2", dtype="float32"),
                Column(variable="geolocation_sensitivity_a5_l2a", SDS_Name="geolocation/sensitivity_a5", dtype="float32"),
                Column(variable="geolocation_stale_return_flag_l2a", SDS_Name="geolocation/stale_return_flag", dtype="uint8"),
                Column(variable="l2a_quality_flag_rel2_l2a", SDS_Name="l2a_quality_flag_rel2", dtype="uint8"),
                Column(variable="l2a_quality_flag_rel3_l2a", SDS_Name="l2a_quality_flag_rel3", dtype="uint8"),
                Column(variable="land_cover_data_landsat_treecover_l2a", SDS_Name="land_cover_data/landsat_treecover", dtype="float64"),
                Column(variable="land_cover_data_landsat_water_persistence_l2a", SDS_Name="land_cover_data/landsat_water_persistence", dtype="uint8"),
                Column(variable="land_cover_data_leaf_off_flag_l2a", SDS_Name="land_cover_data/leaf_off_flag", dtype="uint8"),
                Column(variable="land_cover_data_pft_class_l2a", SDS_Name="land_cover_data/pft_class", dtype="uint8"),
                Column(variable="land_cover_data_phenology_phase_l2a", SDS_Name="land_cover_data/phenology_phase", dtype="uint8"),
                Column(variable="land_cover_data_phenology_year_l2a", SDS_Name="land_cover_data/phenology_year", dtype="uint16"),
                Column(variable="land_cover_data_region_class_l2a", SDS_Name="land_cover_data/region_class", dtype="uint8"),
                Column(variable="land_cover_data_urban_proportion_l2a", SDS_Name="land_cover_data/urban_proportion", dtype="uint8"),
                Column(variable="land_cover_data_worldcover_class_l2a", SDS_Name="land_cover_data/worldcover_class", dtype="uint8"),
                Column(variable="num_detectedmodes_l2a", SDS_Name="num_detectedmodes", dtype="uint8"),
                Column(variable="rx_assess_mean_l2a", SDS_Name="rx_assess/mean", dtype="float32"),
                Column(variable="rx_assess_mean_64kadjusted_l2a", SDS_Name="rx_assess/mean_64kadjusted", dtype="float32"),
                Column(variable="rx_assess_quality_flag_l2a", SDS_Name="rx_assess/quality_flag", dtype="uint8"),
                Column(variable="rx_assess_rx_assess_flag_l2a", SDS_Name="rx_assess/rx_assess_flag", dtype="uint16"),
                Column(variable="rx_assess_rx_energy_l2a", SDS_Name="rx_assess/rx_energy", dtype="float32"),
                Column(variable="rx_assess_rx_maxamp_l2a", SDS_Name="rx_assess/rx_maxamp", dtype="float32"),
                Column(variable="rx_assess_sd_corrected_l2a", SDS_Name="rx_assess/sd_corrected", dtype="float32"),
                Column(variable="rx_processing_a1_botloc_l2a", SDS_Name="rx_processing_a1/botloc", dtype="float32"),
                Column(variable="rx_processing_a1_rx_algrunflag_l2a", SDS_Name="rx_processing_a1/rx_algrunflag", dtype="uint8"),
                Column(variable="rx_processing_a1_selected_mode_flag_l2a", SDS_Name="rx_processing_a1/selected_mode_flag", dtype="uint8"),
                Column(variable="rx_processing_a1_toploc_l2a", SDS_Name="rx_processing_a1/toploc", dtype="float32"),
                Column(variable="rx_processing_a1_zcross_l2a", SDS_Name="rx_processing_a1/zcross", dtype="float32"),
                Column(variable="rx_processing_a10_botloc_l2a", SDS_Name="rx_processing_a10/botloc", dtype="float32"),
                Column(variable="rx_processing_a10_rx_algrunflag_l2a", SDS_Name="rx_processing_a10/rx_algrunflag", dtype="uint8"),
                Column(variable="rx_processing_a10_selected_mode_flag_l2a", SDS_Name="rx_processing_a10/selected_mode_flag", dtype="uint8"),
                Column(variable="rx_processing_a10_toploc_l2a", SDS_Name="rx_processing_a10/toploc", dtype="float32"),
                Column(variable="rx_processing_a10_zcross_l2a", SDS_Name="rx_processing_a10/zcross", dtype="float32"),
                Column(variable="rx_processing_a2_botloc_l2a", SDS_Name="rx_processing_a2/botloc", dtype="float32"),
                Column(variable="rx_processing_a2_rx_algrunflag_l2a", SDS_Name="rx_processing_a2/rx_algrunflag", dtype="uint8"),
                Column(variable="rx_processing_a2_selected_mode_flag_l2a", SDS_Name="rx_processing_a2/selected_mode_flag", dtype="uint8"),
                Column(variable="rx_processing_a2_toploc_l2a", SDS_Name="rx_processing_a2/toploc", dtype="float32"),
                Column(variable="rx_processing_a2_zcross_l2a", SDS_Name="rx_processing_a2/zcross", dtype="float32"),
                Column(variable="rx_processing_a5_botloc_l2a", SDS_Name="rx_processing_a5/botloc", dtype="float32"),
                Column(variable="rx_processing_a5_rx_algrunflag_l2a", SDS_Name="rx_processing_a5/rx_algrunflag", dtype="uint8"),
                Column(variable="rx_processing_a5_selected_mode_flag_l2a", SDS_Name="rx_processing_a5/selected_mode_flag", dtype="uint8"),
                Column(variable="rx_processing_a5_toploc_l2a", SDS_Name="rx_processing_a5/toploc", dtype="float32"),
                Column(variable="rx_processing_a5_zcross_l2a", SDS_Name="rx_processing_a5/zcross", dtype="float32"),
                Column(variable="selected_algorithm_l2a", SDS_Name="selected_algorithm", dtype="uint8"),
                Column(variable="selected_mode_l2a", SDS_Name="selected_mode", dtype="uint8"),
                Column(variable="sensitivity_l2a", SDS_Name="sensitivity", dtype="float32"),
                Column(variable="solar_azimuth_l2a", SDS_Name="solar_azimuth", dtype="float32"),
                Column(variable="solar_elevation_l2a", SDS_Name="solar_elevation", dtype="float32"),
                Column(variable="surface_flag_l2a", SDS_Name="surface_flag", dtype="uint8"),
                Column(variable="geolocation_rh_a1_l2a", SDS_Name="geolocation/rh_a1", is_profile=True, n_bins=101, dtype="int16"),
                Column(variable="geolocation_rh_a10_l2a", SDS_Name="geolocation/rh_a10", is_profile=True, n_bins=101, dtype="int16"),
                Column(variable="geolocation_rh_a2_l2a", SDS_Name="geolocation/rh_a2", is_profile=True, n_bins=101, dtype="int16"),
                Column(variable="geolocation_rh_a5_l2a", SDS_Name="geolocation/rh_a5", is_profile=True, n_bins=101, dtype="int16"),
                Column(variable="rh_l2a", SDS_Name="rh", is_profile=True, n_bins=101, dtype="float32"),
            ]
        ),
        Product(
            product_level=GediProduct.L2B,
            primary_key=PRIMARY_KEY,
            geometry=SHOT_GEOMETRY,
            variables=[
                Column(variable="cover_l2b", SDS_Name="cover", dtype="float32"),
                Column(variable="fhd_normal_l2b", SDS_Name="fhd_normal", dtype="float32"),
                Column(variable="geolocation_local_beam_azimuth_l2b", SDS_Name="geolocation/local_beam_azimuth", dtype="float32"),
                Column(variable="geolocation_local_beam_elevation_l2b", SDS_Name="geolocation/local_beam_elevation", dtype="float32"),
                Column(variable="l2_algrunflag_l2b", SDS_Name="l2_algrunflag", dtype="uint8"),
                Column(variable="l2b_quality_flag_rel2_l2b", SDS_Name="l2b_quality_flag_rel2", dtype="uint8"),
                Column(variable="l2b_quality_flag_rel3_l2b", SDS_Name="l2b_quality_flag_rel3", dtype="uint8"),
                Column(variable="omega_l2b", SDS_Name="omega", dtype="float32"),
                Column(variable="pai_l2b", SDS_Name="pai", dtype="float32"),
                Column(variable="pgap_theta_l2b", SDS_Name="pgap_theta", dtype="float32"),
                Column(variable="pgap_theta_error_l2b", SDS_Name="pgap_theta_error", dtype="float32"),
                Column(variable="rg_l2b", SDS_Name="rg", dtype="float32"),
                Column(variable="rhov_rhog_l2b", SDS_Name="rhov_rhog", dtype="float32"),
                Column(variable="rhov_rhog_se_l2b", SDS_Name="rhov_rhog_se", dtype="float32"),
                Column(variable="rossg_l2b", SDS_Name="rossg", dtype="float32"),
                Column(variable="rv_l2b", SDS_Name="rv", dtype="float32"),
                Column(variable="rv_rg_r_l2b", SDS_Name="rv_rg_r", dtype="float32"),
                Column(variable="rx_processing_fhd_normal_a1_l2b", SDS_Name="rx_processing/fhd_normal_a1", dtype="float32"),
                Column(variable="rx_processing_fhd_normal_a10_l2b", SDS_Name="rx_processing/fhd_normal_a10", dtype="float32"),
                Column(variable="rx_processing_fhd_normal_a2_l2b", SDS_Name="rx_processing/fhd_normal_a2", dtype="float32"),
                Column(variable="rx_processing_fhd_normal_a5_l2b", SDS_Name="rx_processing/fhd_normal_a5", dtype="float32"),
                Column(variable="rx_processing_l2_algrunflag_a1_l2b", SDS_Name="rx_processing/l2_algrunflag_a1", dtype="uint8"),
                Column(variable="rx_processing_l2_algrunflag_a10_l2b", SDS_Name="rx_processing/l2_algrunflag_a10", dtype="uint8"),
                Column(variable="rx_processing_l2_algrunflag_a2_l2b", SDS_Name="rx_processing/l2_algrunflag_a2", dtype="uint8"),
                Column(variable="rx_processing_l2_algrunflag_a5_l2b", SDS_Name="rx_processing/l2_algrunflag_a5", dtype="uint8"),
                Column(variable="rx_processing_rg_a1_l2b", SDS_Name="rx_processing/rg_a1", dtype="float32"),
                Column(variable="rx_processing_rg_a10_l2b", SDS_Name="rx_processing/rg_a10", dtype="float32"),
                Column(variable="rx_processing_rg_a2_l2b", SDS_Name="rx_processing/rg_a2", dtype="float32"),
                Column(variable="rx_processing_rg_a5_l2b", SDS_Name="rx_processing/rg_a5", dtype="float32"),
                Column(variable="rx_processing_rg_error_a1_l2b", SDS_Name="rx_processing/rg_error_a1", dtype="float32"),
                Column(variable="rx_processing_rg_error_a10_l2b", SDS_Name="rx_processing/rg_error_a10", dtype="float32"),
                Column(variable="rx_processing_rg_error_a2_l2b", SDS_Name="rx_processing/rg_error_a2", dtype="float32"),
                Column(variable="rx_processing_rg_error_a5_l2b", SDS_Name="rx_processing/rg_error_a5", dtype="float32"),
                Column(variable="rx_processing_rv_a1_l2b", SDS_Name="rx_processing/rv_a1", dtype="float32"),
                Column(variable="rx_processing_rv_a10_l2b", SDS_Name="rx_processing/rv_a10", dtype="float32"),
                Column(variable="rx_processing_rv_a2_l2b", SDS_Name="rx_processing/rv_a2", dtype="float32"),
                Column(variable="rx_processing_rv_a5_l2b", SDS_Name="rx_processing/rv_a5", dtype="float32"),
                Column(variable="rx_processing_rx_energy_a1_l2b", SDS_Name="rx_processing/rx_energy_a1", dtype="float32"),
                Column(variable="rx_processing_rx_energy_a10_l2b", SDS_Name="rx_processing/rx_energy_a10", dtype="float32"),
                Column(variable="rx_processing_rx_energy_a2_l2b", SDS_Name="rx_processing/rx_energy_a2", dtype="float32"),
                Column(variable="rx_processing_rx_energy_a5_l2b", SDS_Name="rx_processing/rx_energy_a5", dtype="float32"),
                Column(variable="rx_range_highestreturn_l2b", SDS_Name="rx_range_highestreturn", dtype="float64"),
                Column(variable="selected_rg_algorithm_l2b", SDS_Name="selected_rg_algorithm", dtype="uint8"),
                Column(variable="cover_z_l2b", SDS_Name="cover_z", is_profile=True, n_bins=30, dtype="float32"),
                Column(variable="pai_z_l2b", SDS_Name="pai_z", is_profile=True, n_bins=30, dtype="float32"),
                Column(variable="pavd_z_l2b", SDS_Name="pavd_z", is_profile=True, n_bins=30, dtype="float32"),
                Column(variable="rch_l2b", SDS_Name="rch", is_profile=True, n_bins=101, dtype="int16"),
                Column(variable="rx_processing_rch_a1_l2b", SDS_Name="rx_processing/rch_a1", is_profile=True, n_bins=101, dtype="int16"),
                Column(variable="rx_processing_rch_a10_l2b", SDS_Name="rx_processing/rch_a10", is_profile=True, n_bins=101, dtype="int16"),
                Column(variable="rx_processing_rch_a2_l2b", SDS_Name="rx_processing/rch_a2", is_profile=True, n_bins=101, dtype="int16"),
                Column(variable="rx_processing_rch_a5_l2b", SDS_Name="rx_processing/rch_a5", is_profile=True, n_bins=101, dtype="int16"),
            ]
        ),
        Product(
            product_level=GediProduct.L4A,
            primary_key=PRIMARY_KEY,
            geometry=SHOT_GEOMETRY,
            variables=[
                Column(variable="agbd_l4a", SDS_Name="agbd", dtype="float32"),
                Column(variable="agbd_pi_lower_l4a", SDS_Name="agbd_pi_lower", dtype="float32"),
                Column(variable="agbd_pi_upper_l4a", SDS_Name="agbd_pi_upper", dtype="float32"),
                Column(variable="agbd_prediction_agbd_a1_l4a", SDS_Name="agbd_prediction/agbd_a1", dtype="float32"),
                Column(variable="agbd_prediction_agbd_a10_l4a", SDS_Name="agbd_prediction/agbd_a10", dtype="float32"),
                Column(variable="agbd_prediction_agbd_a2_l4a", SDS_Name="agbd_prediction/agbd_a2", dtype="float32"),
                Column(variable="agbd_prediction_agbd_a5_l4a", SDS_Name="agbd_prediction/agbd_a5", dtype="float32"),
                Column(variable="agbd_prediction_agbd_pi_lower_a1_l4a", SDS_Name="agbd_prediction/agbd_pi_lower_a1", dtype="float32"),
                Column(variable="agbd_prediction_agbd_pi_lower_a10_l4a", SDS_Name="agbd_prediction/agbd_pi_lower_a10", dtype="float32"),
                Column(variable="agbd_prediction_agbd_pi_lower_a2_l4a", SDS_Name="agbd_prediction/agbd_pi_lower_a2", dtype="float32"),
                Column(variable="agbd_prediction_agbd_pi_lower_a5_l4a", SDS_Name="agbd_prediction/agbd_pi_lower_a5", dtype="float32"),
                Column(variable="agbd_prediction_agbd_pi_upper_a1_l4a", SDS_Name="agbd_prediction/agbd_pi_upper_a1", dtype="float32"),
                Column(variable="agbd_prediction_agbd_pi_upper_a10_l4a", SDS_Name="agbd_prediction/agbd_pi_upper_a10", dtype="float32"),
                Column(variable="agbd_prediction_agbd_pi_upper_a2_l4a", SDS_Name="agbd_prediction/agbd_pi_upper_a2", dtype="float32"),
                Column(variable="agbd_prediction_agbd_pi_upper_a5_l4a", SDS_Name="agbd_prediction/agbd_pi_upper_a5", dtype="float32"),
                Column(variable="agbd_prediction_agbd_se_a1_l4a", SDS_Name="agbd_prediction/agbd_se_a1", dtype="float32"),
                Column(variable="agbd_prediction_agbd_se_a10_l4a", SDS_Name="agbd_prediction/agbd_se_a10", dtype="float32"),
                Column(variable="agbd_prediction_agbd_se_a2_l4a", SDS_Name="agbd_prediction/agbd_se_a2", dtype="float32"),
                Column(variable="agbd_prediction_agbd_se_a5_l4a", SDS_Name="agbd_prediction/agbd_se_a5", dtype="float32"),
                Column(variable="agbd_prediction_agbd_t_a1_l4a", SDS_Name="agbd_prediction/agbd_t_a1", dtype="float32"),
                Column(variable="agbd_prediction_agbd_t_a10_l4a", SDS_Name="agbd_prediction/agbd_t_a10", dtype="float32"),
                Column(variable="agbd_prediction_agbd_t_a2_l4a", SDS_Name="agbd_prediction/agbd_t_a2", dtype="float32"),
                Column(variable="agbd_prediction_agbd_t_a5_l4a", SDS_Name="agbd_prediction/agbd_t_a5", dtype="float32"),
                Column(variable="agbd_prediction_agbd_t_se_a1_l4a", SDS_Name="agbd_prediction/agbd_t_se_a1", dtype="float32"),
                Column(variable="agbd_prediction_agbd_t_se_a10_l4a", SDS_Name="agbd_prediction/agbd_t_se_a10", dtype="float32"),
                Column(variable="agbd_prediction_agbd_t_se_a2_l4a", SDS_Name="agbd_prediction/agbd_t_se_a2", dtype="float32"),
                Column(variable="agbd_prediction_agbd_t_se_a5_l4a", SDS_Name="agbd_prediction/agbd_t_se_a5", dtype="float32"),
                Column(variable="agbd_prediction_predictor_limit_flag_a1_l4a", SDS_Name="agbd_prediction/predictor_limit_flag_a1", dtype="uint8"),
                Column(variable="agbd_prediction_predictor_limit_flag_a10_l4a", SDS_Name="agbd_prediction/predictor_limit_flag_a10", dtype="uint8"),
                Column(variable="agbd_prediction_predictor_limit_flag_a2_l4a", SDS_Name="agbd_prediction/predictor_limit_flag_a2", dtype="uint8"),
                Column(variable="agbd_prediction_predictor_limit_flag_a5_l4a", SDS_Name="agbd_prediction/predictor_limit_flag_a5", dtype="uint8"),
                Column(variable="agbd_prediction_response_limit_flag_a1_l4a", SDS_Name="agbd_prediction/response_limit_flag_a1", dtype="uint8"),
                Column(variable="agbd_prediction_response_limit_flag_a10_l4a", SDS_Name="agbd_prediction/response_limit_flag_a10", dtype="uint8"),
                Column(variable="agbd_prediction_response_limit_flag_a2_l4a", SDS_Name="agbd_prediction/response_limit_flag_a2", dtype="uint8"),
                Column(variable="agbd_prediction_response_limit_flag_a5_l4a", SDS_Name="agbd_prediction/response_limit_flag_a5", dtype="uint8"),
                Column(variable="agbd_prediction_selected_mode_flag_a1_l4a", SDS_Name="agbd_prediction/selected_mode_flag_a1", dtype="uint8"),
                Column(variable="agbd_prediction_selected_mode_flag_a10_l4a", SDS_Name="agbd_prediction/selected_mode_flag_a10", dtype="uint8"),
                Column(variable="agbd_prediction_selected_mode_flag_a2_l4a", SDS_Name="agbd_prediction/selected_mode_flag_a2", dtype="uint8"),
                Column(variable="agbd_prediction_selected_mode_flag_a5_l4a", SDS_Name="agbd_prediction/selected_mode_flag_a5", dtype="uint8"),
                Column(variable="agbd_se_l4a", SDS_Name="agbd_se", dtype="float32"),
                Column(variable="agbd_t_l4a", SDS_Name="agbd_t", dtype="float32"),
                Column(variable="agbd_t_se_l4a", SDS_Name="agbd_t_se", dtype="float32"),
                Column(variable="degrade_include_flag_l4a", SDS_Name="degrade_include_flag", dtype="bool"),
                Column(variable="elev_highestreturn_outlier_flag_l4a", SDS_Name="elev_highestreturn_outlier_flag", dtype="uint8"),
                Column(variable="l4a_quality_flag_rel3_l4a", SDS_Name="l4a_quality_flag_rel3", dtype="uint8"),
                Column(variable="land_cover_data_pft_infilled_class_l4a", SDS_Name="land_cover_data/pft_infilled_class", dtype="uint8"),
                Column(variable="predict_stratum_l4a", SDS_Name="predict_stratum", dtype="str"),
                Column(variable="predictor_limit_flag_l4a", SDS_Name="predictor_limit_flag", dtype="uint8"),
                Column(variable="response_limit_flag_l4a", SDS_Name="response_limit_flag", dtype="uint8"),
                Column(variable="selected_mode_flag_l4a", SDS_Name="selected_mode_flag", dtype="uint8"),
                Column(variable="agbd_prediction_xvar_a1_l4a", SDS_Name="agbd_prediction/xvar_a1", is_profile=True, n_bins=4, dtype="float32"),
                Column(variable="agbd_prediction_xvar_a10_l4a", SDS_Name="agbd_prediction/xvar_a10", is_profile=True, n_bins=4, dtype="float32"),
                Column(variable="agbd_prediction_xvar_a2_l4a", SDS_Name="agbd_prediction/xvar_a2", is_profile=True, n_bins=4, dtype="float32"),
                Column(variable="agbd_prediction_xvar_a5_l4a", SDS_Name="agbd_prediction/xvar_a5", is_profile=True, n_bins=4, dtype="float32"),
                Column(variable="xvar_l4a", SDS_Name="xvar", is_profile=True, n_bins=4, dtype="float32"),
            ]
        ),
        Product(
            product_level=GediProduct.L4C,
            primary_key=PRIMARY_KEY,
            geometry=SHOT_GEOMETRY,
            variables=[
                Column(variable="algorithm_run_flag_l4c", SDS_Name="algorithm_run_flag", dtype="uint8"),
                Column(variable="l4c_quality_flag_rel2_l4c", SDS_Name="l4c_quality_flag_rel2", dtype="uint8"),
                Column(variable="l4c_quality_flag_rel3_l4c", SDS_Name="l4c_quality_flag_rel3", dtype="uint8"),
                Column(variable="wsci_l4c", SDS_Name="wsci", dtype="float32"),
                Column(variable="wsci_pi_lower_l4c", SDS_Name="wsci_pi_lower", dtype="float32"),
                Column(variable="wsci_pi_upper_l4c", SDS_Name="wsci_pi_upper", dtype="float32"),
                Column(variable="wsci_prediction_algorithm_run_flag_a1_l4c", SDS_Name="wsci_prediction/algorithm_run_flag_a1", dtype="uint8"),
                Column(variable="wsci_prediction_algorithm_run_flag_a10_l4c", SDS_Name="wsci_prediction/algorithm_run_flag_a10", dtype="uint8"),
                Column(variable="wsci_prediction_algorithm_run_flag_a2_l4c", SDS_Name="wsci_prediction/algorithm_run_flag_a2", dtype="uint8"),
                Column(variable="wsci_prediction_algorithm_run_flag_a5_l4c", SDS_Name="wsci_prediction/algorithm_run_flag_a5", dtype="uint8"),
                Column(variable="wsci_prediction_l4c_quality_flag_rel3_a1_l4c", SDS_Name="wsci_prediction/l4c_quality_flag_rel3_a1", dtype="uint8"),
                Column(variable="wsci_prediction_l4c_quality_flag_rel3_a10_l4c", SDS_Name="wsci_prediction/l4c_quality_flag_rel3_a10", dtype="uint8"),
                Column(variable="wsci_prediction_l4c_quality_flag_rel3_a2_l4c", SDS_Name="wsci_prediction/l4c_quality_flag_rel3_a2", dtype="uint8"),
                Column(variable="wsci_prediction_l4c_quality_flag_rel3_a5_l4c", SDS_Name="wsci_prediction/l4c_quality_flag_rel3_a5", dtype="uint8"),
                Column(variable="wsci_prediction_wsci_a1_l4c", SDS_Name="wsci_prediction/wsci_a1", dtype="float32"),
                Column(variable="wsci_prediction_wsci_a10_l4c", SDS_Name="wsci_prediction/wsci_a10", dtype="float32"),
                Column(variable="wsci_prediction_wsci_a2_l4c", SDS_Name="wsci_prediction/wsci_a2", dtype="float32"),
                Column(variable="wsci_prediction_wsci_a5_l4c", SDS_Name="wsci_prediction/wsci_a5", dtype="float32"),
                Column(variable="wsci_prediction_wsci_pi_lower_a1_l4c", SDS_Name="wsci_prediction/wsci_pi_lower_a1", dtype="float32"),
                Column(variable="wsci_prediction_wsci_pi_lower_a10_l4c", SDS_Name="wsci_prediction/wsci_pi_lower_a10", dtype="float32"),
                Column(variable="wsci_prediction_wsci_pi_lower_a2_l4c", SDS_Name="wsci_prediction/wsci_pi_lower_a2", dtype="float32"),
                Column(variable="wsci_prediction_wsci_pi_lower_a5_l4c", SDS_Name="wsci_prediction/wsci_pi_lower_a5", dtype="float32"),
                Column(variable="wsci_prediction_wsci_pi_upper_a1_l4c", SDS_Name="wsci_prediction/wsci_pi_upper_a1", dtype="float32"),
                Column(variable="wsci_prediction_wsci_pi_upper_a10_l4c", SDS_Name="wsci_prediction/wsci_pi_upper_a10", dtype="float32"),
                Column(variable="wsci_prediction_wsci_pi_upper_a2_l4c", SDS_Name="wsci_prediction/wsci_pi_upper_a2", dtype="float32"),
                Column(variable="wsci_prediction_wsci_pi_upper_a5_l4c", SDS_Name="wsci_prediction/wsci_pi_upper_a5", dtype="float32"),
                Column(variable="wsci_prediction_wsci_xy_a1_l4c", SDS_Name="wsci_prediction/wsci_xy_a1", dtype="float32"),
                Column(variable="wsci_prediction_wsci_xy_a10_l4c", SDS_Name="wsci_prediction/wsci_xy_a10", dtype="float32"),
                Column(variable="wsci_prediction_wsci_xy_a2_l4c", SDS_Name="wsci_prediction/wsci_xy_a2", dtype="float32"),
                Column(variable="wsci_prediction_wsci_xy_a5_l4c", SDS_Name="wsci_prediction/wsci_xy_a5", dtype="float32"),
                Column(variable="wsci_prediction_wsci_xy_pi_lower_a1_l4c", SDS_Name="wsci_prediction/wsci_xy_pi_lower_a1", dtype="float32"),
                Column(variable="wsci_prediction_wsci_xy_pi_lower_a10_l4c", SDS_Name="wsci_prediction/wsci_xy_pi_lower_a10", dtype="float32"),
                Column(variable="wsci_prediction_wsci_xy_pi_lower_a2_l4c", SDS_Name="wsci_prediction/wsci_xy_pi_lower_a2", dtype="float32"),
                Column(variable="wsci_prediction_wsci_xy_pi_lower_a5_l4c", SDS_Name="wsci_prediction/wsci_xy_pi_lower_a5", dtype="float32"),
                Column(variable="wsci_prediction_wsci_xy_pi_upper_a1_l4c", SDS_Name="wsci_prediction/wsci_xy_pi_upper_a1", dtype="float32"),
                Column(variable="wsci_prediction_wsci_xy_pi_upper_a10_l4c", SDS_Name="wsci_prediction/wsci_xy_pi_upper_a10", dtype="float32"),
                Column(variable="wsci_prediction_wsci_xy_pi_upper_a2_l4c", SDS_Name="wsci_prediction/wsci_xy_pi_upper_a2", dtype="float32"),
                Column(variable="wsci_prediction_wsci_xy_pi_upper_a5_l4c", SDS_Name="wsci_prediction/wsci_xy_pi_upper_a5", dtype="float32"),
                Column(variable="wsci_prediction_wsci_z_a1_l4c", SDS_Name="wsci_prediction/wsci_z_a1", dtype="float32"),
                Column(variable="wsci_prediction_wsci_z_a10_l4c", SDS_Name="wsci_prediction/wsci_z_a10", dtype="float32"),
                Column(variable="wsci_prediction_wsci_z_a2_l4c", SDS_Name="wsci_prediction/wsci_z_a2", dtype="float32"),
                Column(variable="wsci_prediction_wsci_z_a5_l4c", SDS_Name="wsci_prediction/wsci_z_a5", dtype="float32"),
                Column(variable="wsci_prediction_wsci_z_pi_lower_a1_l4c", SDS_Name="wsci_prediction/wsci_z_pi_lower_a1", dtype="float32"),
                Column(variable="wsci_prediction_wsci_z_pi_lower_a10_l4c", SDS_Name="wsci_prediction/wsci_z_pi_lower_a10", dtype="float32"),
                Column(variable="wsci_prediction_wsci_z_pi_lower_a2_l4c", SDS_Name="wsci_prediction/wsci_z_pi_lower_a2", dtype="float32"),
                Column(variable="wsci_prediction_wsci_z_pi_lower_a5_l4c", SDS_Name="wsci_prediction/wsci_z_pi_lower_a5", dtype="float32"),
                Column(variable="wsci_prediction_wsci_z_pi_upper_a1_l4c", SDS_Name="wsci_prediction/wsci_z_pi_upper_a1", dtype="float32"),
                Column(variable="wsci_prediction_wsci_z_pi_upper_a10_l4c", SDS_Name="wsci_prediction/wsci_z_pi_upper_a10", dtype="float32"),
                Column(variable="wsci_prediction_wsci_z_pi_upper_a2_l4c", SDS_Name="wsci_prediction/wsci_z_pi_upper_a2", dtype="float32"),
                Column(variable="wsci_prediction_wsci_z_pi_upper_a5_l4c", SDS_Name="wsci_prediction/wsci_z_pi_upper_a5", dtype="float32"),
                Column(variable="wsci_xy_l4c", SDS_Name="wsci_xy", dtype="float32"),
                Column(variable="wsci_xy_pi_lower_l4c", SDS_Name="wsci_xy_pi_lower", dtype="float32"),
                Column(variable="wsci_xy_pi_upper_l4c", SDS_Name="wsci_xy_pi_upper", dtype="float32"),
                Column(variable="wsci_z_l4c", SDS_Name="wsci_z", dtype="float32"),
                Column(variable="wsci_z_pi_lower_l4c", SDS_Name="wsci_z_pi_lower", dtype="float32"),
                Column(variable="wsci_z_pi_upper_l4c", SDS_Name="wsci_z_pi_upper", dtype="float32"),
            ]
        ),
    ],
    derived=[
        DerivedColumn(
            variable="absolute_time",
            SDS_Name="delta_time",
            description=(
                "Timestamp of GEDI footprint, derived from delta_time."
            ),
            product_level=[GediProduct.L2A, GediProduct.L2B, GediProduct.L4A, GediProduct.L4C],
            dtype="datetime64[ns, UTC]",
            unit="UTC timestamp",
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
            description=(
                "Point at the lon/lat of the GEDI footprint in WGS 84. Stored as "
                "OGC:CRS84 (EPSG:4326 with lon/lat axis order), which is the "
                "GeoParquet default and needs no axis-order override in DuckDB."
            ),
            product_level=[GediProduct.L2A],
            dtype="GEOMETRY('OGC:CRS84')",
            unit="degrees (x=lon, y=lat)"
        ),
        DerivedColumn(
            variable="geometry_6933",
            SDS_Name="lat_lowestmode/lon_lowestmode",
            description="Point at the GEDI footprint in EASE-Grid 2.0 Global (EPSG:6933)",
            product_level=[GediProduct.L2A],
            dtype="GEOMETRY('EPSG:6933')",
            unit="metres (x=easting, y=northing)"
        ),
        DerivedColumn(
            variable="ease_72km_x",
            SDS_Name="lat_lowestmode/lon_lowestmode",
            description=(
                "Column index of the footprint in the 72 km EASE-Grid 2.0 Global "
                "grid (72 x 1000.895 m cells), counted east from the grid's west edge"
            ),
            product_level=[GediProduct.L2A],
            dtype="int16",
            unit="N/A"
        ),
        DerivedColumn(
            variable="ease_72km_y",
            SDS_Name="lat_lowestmode/lon_lowestmode",
            description=(
                "Row index of the footprint in the 72 km EASE-Grid 2.0 Global "
                "grid (72 x 1000.895 m cells), counted south from the grid's north edge"
            ),
            product_level=[GediProduct.L2A],
            dtype="int16",
            unit="N/A"
        ),
        DerivedColumn(
            variable="h3_12",
            SDS_Name="lat_lowestmode/lon_lowestmode",
            description="H3 cell containing the footprint at resolution 12 (~307 m^2 cells)",
            product_level=[GediProduct.L2A],
            dtype="uint64",
            unit="N/A"
        ),
        DerivedColumn(
            variable="h3_03",
            SDS_Name="lat_lowestmode/lon_lowestmode",
            description="H3 cell containing the footprint at resolution 3 (~12,400 km^2 cells)",
            product_level=[GediProduct.L2A],
            dtype="uint64",
            unit="N/A"
        ),
        DerivedColumn(
            variable="beam_name",
            SDS_Name="N/A",
            description="Name of the GEDI beam (e.g. BEAM0001), derived from h5 groups",
            product_level=[GediProduct.L2A, GediProduct.L2B, GediProduct.L4A, GediProduct.L4C],
            dtype="string[7]",
            unit="N/A"
        ),
        DerivedColumn(
            variable="root_file_l2a",
            SDS_Name="N/A",
            description="Name of the L2A file for this GEDI shot",
            product_level=[GediProduct.L2A],
            dtype="string[57]",
            unit="N/A"
        ),
        DerivedColumn(
            variable="root_file_l2b",
            SDS_Name="N/A",
            description="Name of the L2B file for this GEDI shot",
            product_level=[GediProduct.L2B],
            dtype="string[57]",
            unit="N/A"
        ),
        DerivedColumn(
            variable="root_file_l4a",
            SDS_Name="N/A",
            description="Name of the L4A file for this GEDI shot",
            product_level=[GediProduct.L4A],
            dtype="string[57]",
            unit="N/A"
        ),
        DerivedColumn(
            variable="root_file_l4c",
            SDS_Name="N/A",
            description="Name of the L4C file for this GEDI shot",
            product_level=[GediProduct.L4C],
            dtype="string[57]",
            unit="N/A"
        ),
    ]
)
# fmt: on
