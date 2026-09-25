"""Unit tests for gtiler.common.cmr_query response parsing."""

from gtiler.common import cmr_query


def _entry(data_center, title, producer_granule_id, href):
    return {
        "online_access_flag": True,
        "data_center": data_center,
        "title": title,
        "producer_granule_id": producer_granule_id,
        "granule_size": "1.5",
        "polygons": [["-1 -50 -1 -49 0 -49 0 -50 -1 -50"]],
        "links": [{"href": href}],
        "time_start": "2021-09-04T11:57:44.000Z",
        "time_end": "2021-09-04T13:30:00.000Z",
    }


def test_parses_v3_lp_and_ornl_granule_names():
    # V003 ORNL titles are the bare granule name, with no "<collection>."
    # prefix as in V002.
    lp = _entry(
        "LPCLOUD",
        "GEDI02_A_2021247115744_O15457_04_T06217_02_004_02_V003",
        "GEDI02_A_2021247115744_O15457_04_T06217_02_004_02_V003.h5",
        "s3://lp-prod-protected/GEDI02_A.003/x.h5",
    )
    ornl = _entry(
        "ORNL_CLOUD",
        "GEDI04_A_2021247115744_O15457_04_T06217_02_004_01_V003",
        "GEDI04_A_2021247115744_O15457_04_T06217_02_004_01_V003",
        "s3://ornl-cumulus-prod-protected/gedi/GEDI_L4A_AGB_Density_V3/data/x.h5",
    )
    rows = cmr_query._parse_granules([lp, ornl], use_cloud=True)
    assert [r[0] for r in rows] == [
        "GEDI02_A_2021247115744_O15457_04_T06217_02_004_02_V003.h5",
        "GEDI04_A_2021247115744_O15457_04_T06217_02_004_01_V003",
    ]
    assert [r[1] for r in rows] == [lp["links"][0]["href"], ornl["links"][0]["href"]]
