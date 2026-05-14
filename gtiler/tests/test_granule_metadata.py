"""Tests for gtiler.common.granule_metadata.get_granule_metadata.

CMR is mocked out: the per-product `cmr_query.query` outputs are pinned
fixtures captured by `fixtures/build_cmr_query_fixtures.py` against the
sabah_box shape over 2021 (re-run that script if you need to refresh
them).

The sabah_box / 2021 sample happens to have all 4 products present for
all 16 granules, which makes it a clean baseline. Tests that need to
exercise incomplete-granule behaviour synthesize that by dropping rows
from the fixture.
"""

import pathlib

import geopandas as gpd
import pandas as pd
import pytest
from unittest.mock import patch

from gtiler.common import cmr_query, granule_metadata
from gtiler.database.schema import GediProduct


FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "cmr_query"

ALL_PRODUCTS = [
    GediProduct.L2A,
    GediProduct.L2B,
    GediProduct.L4A,
    GediProduct.L4C,
]

EXPECTED_OUTPUT_COLUMNS = {
    "granule_key",
    "granule_size",
    "granule_names",
    "level2A_url",
    "level2B_url",
    "level4A_url",
    "level4C_url",
    "geometry",
    "granule_hash",
}

# Expected URL prefixes per product, from the pinned fixture.
URL_PREFIX_BY_PRODUCT = {
    "level2A_url": "s3://lp-prod-protected/GEDI02_A.002/",
    "level2B_url": "s3://lp-prod-protected/GEDI02_B.002/",
    "level4A_url": "s3://ornl-cumulus-prod-protected/gedi/GEDI_L4A_AGB_Density_V2_1/",
    "level4C_url": "s3://ornl-cumulus-prod-protected/gedi/GEDI_L4C_WSCI/",
}


def _load_fixture(product: GediProduct) -> gpd.GeoDataFrame:
    return pd.read_pickle(FIXTURES / f"{product.value}.pkl")


@pytest.fixture
def fixture_by_product():
    """{GediProduct: per-product cmr_query.query() output}"""
    return {p: _load_fixture(p) for p in ALL_PRODUCTS}


@pytest.fixture
def mock_cmr_query(fixture_by_product):
    """Patch cmr_query.query to return the pinned per-product fixture
    matching the requested product, ignoring all other arguments."""
    def _stub(product, **_kwargs):
        # Return a copy so the function under test can't mutate the
        # fixture for other tests sharing the same session.
        return fixture_by_product[product].copy()

    with patch.object(cmr_query, "query", side_effect=_stub) as m:
        yield m


@pytest.fixture
def md(mock_cmr_query):
    """get_granule_metadata called over all 4 products against the
    pinned sabah_box / 2021 fixture."""
    return granule_metadata.get_granule_metadata(
        shape=None,  # ignored by the mock
        products=ALL_PRODUCTS,
        start_year=2021,
        end_year=2021,
    )


class TestGetGranuleMetadataBaseline:
    """Baseline behaviour against the pinned sabah_box / 2021 fixture
    (16 granules, every granule has all 4 products)."""

    def test_one_query_per_product(self, md, mock_cmr_query):
        # One call to cmr_query.query for each requested product.
        called_products = [
            call.args[0] if call.args else call.kwargs.get("product")
            for call in mock_cmr_query.call_args_list
        ]
        assert called_products == ALL_PRODUCTS

    def test_one_row_per_granule(self, md):
        # 16 input granules, all complete → 16 output rows, one per key.
        assert len(md) == 16
        assert md["granule_key"].is_unique

    def test_output_columns_exact(self, md):
        assert set(md.columns) == EXPECTED_OUTPUT_COLUMNS

    def test_granule_key_format(self, md):
        # "<OrbitID>_<SubOrbitID>" — e.g. O13171_02
        assert md["granule_key"].str.match(r"^O\d+_\d+$").all()

    def test_all_product_urls_populated(self, md):
        for col in URL_PREFIX_BY_PRODUCT:
            assert md[col].notna().all(), f"{col} has nulls"

    def test_product_urls_point_at_right_bucket(self, md):
        for col, prefix in URL_PREFIX_BY_PRODUCT.items():
            assert md[col].str.startswith(prefix).all(), (
                f"{col} has unexpected URL prefix"
            )

    def test_granule_names_lists_have_one_entry_per_product(self, md):
        # After the groupby aggregation, granule_names is a list of the
        # per-product filenames for that granule_key.
        lengths = md["granule_names"].apply(len)
        assert (lengths == len(ALL_PRODUCTS)).all()

    def test_granule_hash_stable_and_nonempty(self, md):
        # 32-char md5 hex; same input list → same hash for that row.
        assert md["granule_hash"].str.match(r"^[0-9a-f]{32}$").all()
        # Distinct granules should have distinct hashes (16 unique).
        assert md["granule_hash"].nunique() == len(md)

    def test_geometry_is_set(self, md):
        assert isinstance(md, gpd.GeoDataFrame)
        assert md.geometry.notna().all()
        assert md.crs is not None and md.crs.to_string() == "EPSG:4326"


class TestGetGranuleMetadataFiltersIncompleteGranules:
    """When the queried products list is treated as the implicit
    required set (the default), granules missing any of them are
    dropped."""

    def test_granule_missing_l4c_is_excluded(
        self, fixture_by_product, caplog
    ):
        # Drop the first L4C row so its granule_key now has only
        # L2A + L2B + L4A. With required_products defaulting to all
        # four queried products, that granule must be excluded.
        l4c = fixture_by_product[GediProduct.L4C]
        dropped_name = l4c.iloc[0]["granule_name"]

        # Derive the expected dropped granule_key the same way the code
        # under test does.
        from gtiler.common.granule_metadata import get_granule_key_for_filename
        dropped_key = get_granule_key_for_filename(dropped_name)

        fixture_by_product[GediProduct.L4C] = l4c.iloc[1:].reset_index(
            drop=True
        )

        def _stub(product, **_kwargs):
            return fixture_by_product[product].copy()

        with patch.object(cmr_query, "query", side_effect=_stub):
            with caplog.at_level("INFO"):
                md = granule_metadata.get_granule_metadata(
                    shape=None,
                    products=ALL_PRODUCTS,
                    start_year=2021,
                    end_year=2021,
                )

        assert dropped_key not in md["granule_key"].values
        assert len(md) == 15  # one excluded
        # Verify the log mentions the exclusion.
        assert any(
            "Excluding" in r.message and "required products" in r.message
            for r in caplog.records
        )


class TestGetGranuleMetadataWithL4COptional:
    """When required_products excludes L4C, granules missing only L4C
    are kept with a NaN level4C_url; granules missing any required
    product (e.g. L2A) are still dropped."""

    @pytest.fixture
    def required_products(self):
        return [GediProduct.L2A, GediProduct.L2B, GediProduct.L4A]

    @pytest.fixture
    def fixture_with_one_l4c_missing(self, fixture_by_product):
        """Drop the first L4C row so one granule has L2A+L2B+L4A only."""
        from gtiler.common.granule_metadata import get_granule_key_for_filename
        l4c = fixture_by_product[GediProduct.L4C]
        dropped_key = get_granule_key_for_filename(l4c.iloc[0]["granule_name"])
        fixture_by_product[GediProduct.L4C] = l4c.iloc[1:].reset_index(
            drop=True
        )
        return fixture_by_product, dropped_key

    @pytest.fixture
    def md_l4c_optional(
        self, fixture_with_one_l4c_missing, required_products
    ):
        fixtures, _ = fixture_with_one_l4c_missing

        def _stub(product, **_kwargs):
            return fixtures[product].copy()

        with patch.object(cmr_query, "query", side_effect=_stub):
            return granule_metadata.get_granule_metadata(
                shape=None,
                products=ALL_PRODUCTS,
                start_year=2021,
                end_year=2021,
                required_products=required_products,
            )

    def test_all_16_granules_kept(self, md_l4c_optional):
        assert len(md_l4c_optional) == 16
        assert md_l4c_optional["granule_key"].is_unique

    def test_required_product_urls_all_populated(self, md_l4c_optional):
        for col in ("level2A_url", "level2B_url", "level4A_url"):
            assert md_l4c_optional[col].notna().all(), (
                f"{col} should be populated for every granule"
            )

    def test_l4c_url_null_only_for_dropped_granule(
        self, md_l4c_optional, fixture_with_one_l4c_missing
    ):
        _, dropped_key = fixture_with_one_l4c_missing
        null_keys = set(
            md_l4c_optional.loc[
                md_l4c_optional["level4C_url"].isna(), "granule_key"
            ]
        )
        assert null_keys == {dropped_key}
        non_null = md_l4c_optional[md_l4c_optional["level4C_url"].notna()]
        assert non_null["level4C_url"].str.startswith(
            URL_PREFIX_BY_PRODUCT["level4C_url"]
        ).all()

    def test_granule_missing_required_product_still_dropped(
        self, fixture_by_product, required_products
    ):
        """L4C is optional, but L2A is still required — a granule with
        no L2A entry must be excluded from the output."""
        from gtiler.common.granule_metadata import get_granule_key_for_filename
        l2a = fixture_by_product[GediProduct.L2A]
        dropped_key = get_granule_key_for_filename(l2a.iloc[0]["granule_name"])
        fixture_by_product[GediProduct.L2A] = l2a.iloc[1:].reset_index(
            drop=True
        )

        def _stub(product, **_kwargs):
            return fixture_by_product[product].copy()

        with patch.object(cmr_query, "query", side_effect=_stub):
            md = granule_metadata.get_granule_metadata(
                shape=None,
                products=ALL_PRODUCTS,
                start_year=2021,
                end_year=2021,
                required_products=required_products,
            )

        assert dropped_key not in md["granule_key"].values
        assert len(md) == 15

    def test_rejects_required_not_in_products(self, fixture_by_product):
        """required_products must be a subset of products."""
        def _stub(product, **_kwargs):
            return fixture_by_product[product].copy()

        with patch.object(cmr_query, "query", side_effect=_stub):
            with pytest.raises(ValueError, match="must be a subset"):
                granule_metadata.get_granule_metadata(
                    shape=None,
                    products=[GediProduct.L2A, GediProduct.L2B],
                    required_products=[GediProduct.L2A, GediProduct.L4C],
                )
