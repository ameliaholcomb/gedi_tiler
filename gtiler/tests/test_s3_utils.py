"""Unit tests for the Earthdata credential handling in gtiler.common.s3_utils."""

from unittest.mock import patch

import pytest

from gtiler.common import s3_utils

LP_URL = "s3://lp-prod-protected/GEDI02_A.003/x/x.h5"
ORNL_URL = "s3://ornl-cumulus-prod-protected/gedi/GEDI_L4A_AGB_Density_V3/data/x.h5"


class _FakeMAAP:
    """Hands out numbered credentials."""

    def __init__(self, *args, **kwargs):
        self.aws = self
        self.calls = []

    def earthdata_s3_credentials(self, endpoint):
        self.calls.append(endpoint)
        n = len(self.calls)
        return {
            "accessKeyId": f"key{n}",
            "secretAccessKey": f"secret{n}",
            "sessionToken": f"token{n}",
            "expiration": "2026-09-28 21:00:00+00:00",
        }


@pytest.fixture
def daac_fs():
    with patch.object(s3_utils, "MAAP", _FakeMAAP):
        yield s3_utils.DaacFS()


def test_each_daac_gets_its_own_endpoint_and_filesystem(daac_fs):
    lp = daac_fs.get_fs(LP_URL)
    ornl = daac_fs.get_fs(ORNL_URL)
    assert daac_fs.maap.calls == [
        s3_utils.DAAC_CREDENTIALS_ENDPOINTS["lp-prod-protected"],
        s3_utils.DAAC_CREDENTIALS_ENDPOINTS["ornl-cumulus-prod-protected"],
    ]
    assert lp is not ornl
    assert (lp.key, ornl.key) == ("key1", "key2")


def test_credentials_are_reused_until_refresh(daac_fs):
    first = daac_fs.get_fs(LP_URL)
    assert daac_fs.get_fs(LP_URL) is first
    assert len(daac_fs.maap.calls) == 1


def test_refresh_replaces_only_that_daac(daac_fs):
    lp = daac_fs.get_fs(LP_URL)
    ornl = daac_fs.get_fs(ORNL_URL)
    daac_fs.refresh(ORNL_URL)
    assert daac_fs.get_fs(LP_URL) is lp
    assert daac_fs.get_fs(ORNL_URL) is not ornl


def test_unknown_bucket_fails(daac_fs):
    with pytest.raises(KeyError):
        daac_fs.get_fs("s3://some-other-bucket/x.h5")


class TestCallMaapApi:
    @pytest.fixture(autouse=True)
    def no_sleep(self):
        with patch.object(s3_utils.time, "sleep") as sleep:
            yield sleep

    def _flaky(self, failures, error):
        calls = []

        def f(x):
            calls.append(x)
            if len(calls) <= failures:
                raise error
            return x * 2

        return f, calls

    def test_retries_timeouts_until_success(self, no_sleep):
        f, calls = self._flaky(2, s3_utils.requests.ConnectTimeout("slow"))
        assert s3_utils.call_maap_api(f, 3) == 6
        assert len(calls) == 3
        assert [c.args[0] for c in no_sleep.call_args_list] == [15, 30]

    def test_raises_once_the_retries_run_out(self):
        f, calls = self._flaky(99, s3_utils.requests.ConnectionError("down"))
        with pytest.raises(s3_utils.requests.ConnectionError):
            s3_utils.call_maap_api(f, 3)
        assert len(calls) == len(s3_utils.MAAP_API_WAITS) + 1

    def test_other_errors_are_not_retried(self):
        f, calls = self._flaky(1, ValueError("bad"))
        with pytest.raises(ValueError):
            s3_utils.call_maap_api(f, 3)
        assert len(calls) == 1
