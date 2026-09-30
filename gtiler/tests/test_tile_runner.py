"""Unit tests for the job planning helpers in scripts/runners/tile_runner.py.

Run with:
    conda run -n pyduck python -m pytest gtiler/tests/test_tile_runner.py -v
"""

import importlib.util
import pathlib
import sys

import pandas as pd
import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def tile_runner():
    path = REPO_ROOT / "scripts" / "runners" / "tile_runner.py"
    spec = importlib.util.spec_from_file_location("tile_runner", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["tile_runner"] = module
    spec.loader.exec_module(module)
    return module


class TestChooseQueue:
    def test_small_tile_year_stays_on_8gb(self, tile_runner):
        assert tile_runner.choose_queue("S03_W060", 54, 55) == "maap-dps-worker-8gb"

    def test_large_tile_year_goes_to_16gb(self, tile_runner):
        assert tile_runner.choose_queue("S03_W060", 55, 55) == "maap-dps-worker-16gb"

    def test_latitude_queue_is_not_lowered(self, tile_runner):
        assert tile_runner.choose_queue("N52_W100", 10, 55) == "maap-dps-worker-32gb"
        assert tile_runner.choose_queue("N52_W100", 90, 55) == "maap-dps-worker-32gb"


class TestRequiredTileYears:
    def test_counts_granules_per_tile_year(self, tile_runner):
        granules = pd.DataFrame({
            "tile_id": ["A", "A", "A", "B"],
            "time_start": pd.to_datetime(
                ["2020-03-01T00:00", "2020-06-01T00:00", "2020-12-31T23:00", "2021-05-01T00:00"], utc=True
            ),
            "time_end": pd.to_datetime(
                ["2020-03-01T01:00", "2020-06-01T01:00", "2021-01-01T00:30", "2021-05-01T01:00"], utc=True
            ),
        })
        counts = tile_runner.required_tile_years(granules, None, None)
        # The granule spanning New Year counts toward both years.
        assert counts == {("A", 2020): 3, ("A", 2021): 1, ("B", 2021): 1}

    def test_years_outside_the_range_are_left_out(self, tile_runner):
        granules = pd.DataFrame({
            "tile_id": ["A"],
            "time_start": pd.to_datetime(["2020-12-31T23:00"], utc=True),
            "time_end": pd.to_datetime(["2021-01-01T00:30"], utc=True),
        })
        counts = tile_runner.required_tile_years(granules, 2021, None)
        assert counts == {("A", 2021): 1}
