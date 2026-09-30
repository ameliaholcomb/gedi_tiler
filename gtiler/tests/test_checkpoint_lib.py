"""Tests for gtiler.common.checkpoint_lib, against moto's S3.

moto enforces If-Match/If-None-Match on PutObject but not on multipart
uploads, so uploads here go through a single conditional PutObject. Real
S3 enforces both conditions on multipart uploads too.
"""

import io
import json

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws
from unittest.mock import patch

from gtiler.common import s3_utils
from gtiler.common.checkpoint_lib import (
    DONE,
    RUNNING,
    CheckpointConflict,
    Checkpointer,
)

BUCKET = "test-bucket"
PREFIX = "test/prefix"
TILE_ID = "N00_W050"
YEAR = 2020
CHECKPOINT_PREFIX = f"{PREFIX}/checkpoints/{TILE_ID}/{YEAR}/"
MANIFEST_KEY = f"{CHECKPOINT_PREFIX}manifest.json"
OUTPUT_KEY = f"{PREFIX}/data/tile_id={TILE_ID}/year={YEAR}/data_0.parquet"
GRANULES = ["g1", "g2", "g3", "g4"]


def _single_put(bucket, key, body, *, if_match=None, if_none_match=None):
    return s3_utils.conditional_put(
        bucket, key, body.read(), if_match=if_match, if_none_match=if_none_match
    )


@pytest.fixture
def s3():
    with mock_aws(), patch.object(
        s3_utils, "conditional_multipart_put", _single_put
    ):
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def job(generation, job_id):
    return Checkpointer(BUCKET, PREFIX, TILE_ID, YEAR, generation, job_id=job_id)


def manifest(s3):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read())


def keys(s3):
    return sorted(
        o["Key"]
        for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get("Contents", [])
    )


def body(s3, key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


@pytest.fixture
def local(tmp_path):
    """Returns a function writing bytes to a local file, returning its path."""

    def _write(name, content):
        path = tmp_path / name
        path.write_bytes(content)
        return str(path)

    return _write


def run_batch(cp, local, name, remaining):
    cp.add_batch(local(name, name.encode()), remaining)


class TestFreshStart:
    def test_claims_a_manifest_listing_every_granule(self, s3):
        m = job(0, "a").initialize(GRANULES, [OUTPUT_KEY])
        assert (m.generation, m.job_id, m.remaining, m.parts, m.state) == (
            0, "a", GRANULES, [], RUNNING,
        )
        assert manifest(s3)["job_id"] == "a"

    def test_batch_uploads_a_part_named_for_the_job(self, s3, local):
        cp = job(3, "a")
        cp.initialize(GRANULES, [OUTPUT_KEY])
        run_batch(cp, local, "p0", GRANULES[2:])
        part = f"{CHECKPOINT_PREFIX}part-g3-a-0000.parquet"
        assert manifest(s3)["parts"] == [part]
        assert manifest(s3)["remaining"] == GRANULES[2:]
        assert body(s3, part) == b"p0"

    def test_batch_without_shots_records_progress_only(self, s3):
        cp = job(0, "a")
        cp.initialize(GRANULES, [OUTPUT_KEY])
        cp.add_batch(None, GRANULES[2:])
        assert manifest(s3)["parts"] == []
        assert manifest(s3)["remaining"] == GRANULES[2:]


class TestResume:
    def test_resumes_progress_and_downloads_parts(self, s3, local, tmp_path):
        a = job(0, "a")
        a.initialize(GRANULES, [OUTPUT_KEY])
        run_batch(a, local, "p0", GRANULES[2:])
        b = job(0, "b")
        m = b.initialize(GRANULES, [OUTPUT_KEY])
        assert (m.job_id, m.remaining) == ("b", GRANULES[2:])
        (dl := tmp_path / "dl").mkdir()
        paths = b.download_parts(str(dl))
        assert [open(p, "rb").read() for p in paths] == [b"p0"]

    def test_newest_claim_of_the_same_generation_wins(self, s3, local):
        a = job(0, "a")
        a.initialize(GRANULES, [OUTPUT_KEY])
        job(0, "b").initialize(GRANULES, [OUTPUT_KEY])
        with pytest.raises(CheckpointConflict):
            run_batch(a, local, "p0", GRANULES[2:])
        assert manifest(s3)["job_id"] == "b"

    def test_higher_generation_takes_over_the_progress(self, s3, local):
        old = job(0, "old")
        old.initialize(GRANULES, [OUTPUT_KEY])
        run_batch(old, local, "p0", GRANULES[2:])
        m = job(1, "new").initialize(GRANULES, [OUTPUT_KEY])
        assert (m.generation, m.remaining, len(m.parts)) == (1, GRANULES[2:], 1)
        with pytest.raises(CheckpointConflict):
            run_batch(old, local, "p1", [])

    def test_lower_generation_stops_at_once(self, s3):
        job(2, "new").initialize(GRANULES, [OUTPUT_KEY])
        with pytest.raises(CheckpointConflict):
            job(1, "old").initialize(GRANULES, [OUTPUT_KEY])
        assert manifest(s3)["job_id"] == "new"

    def test_a_lost_claim_retries_from_the_newer_progress(self, s3, local):
        """If another job writes between this job's read and its claim,
        the claim is made again from what that job wrote, so the newer
        progress is kept."""
        a = job(0, "a")
        a.initialize(GRANULES, [OUTPUT_KEY])
        b = job(1, "b")
        read = b._read
        calls = []

        def read_then_race():
            result = read()
            if not calls:
                run_batch(a, local, "p0", GRANULES[2:])
            calls.append(1)
            return result

        with patch.object(b, "_read", read_then_race):
            m = b.initialize(GRANULES, [OUTPUT_KEY])
        assert len(calls) == 2
        assert (m.remaining, len(m.parts)) == (GRANULES[2:], 1)
        assert manifest(s3)["job_id"] == "b"


class TestCommit:
    def _finish(self, cp, local, content=b"out"):
        cp.add_batch(local("p", b"p"), [])
        cp.commit(local("out", content), OUTPUT_KEY)

    def test_writes_output_marks_done_and_deletes_parts(self, s3, local):
        cp = job(0, "a")
        cp.initialize(GRANULES, [OUTPUT_KEY])
        self._finish(cp, local)
        assert body(s3, OUTPUT_KEY) == b"out"
        assert (manifest(s3)["state"], manifest(s3)["parts"]) == (DONE, [])
        assert keys(s3) == sorted([MANIFEST_KEY, OUTPUT_KEY])

    def test_deletes_unlisted_parts_of_lost_jobs(self, s3, local):
        a = job(0, "a")
        a.initialize(GRANULES, [OUTPUT_KEY])
        b = job(0, "b")
        b.initialize(GRANULES, [OUTPUT_KEY])
        # a's part was uploaded, but its manifest update lost to b.
        with pytest.raises(CheckpointConflict):
            run_batch(a, local, "orphan", GRANULES[2:])
        assert f"{CHECKPOINT_PREFIX}part-g0-a-0000.parquet" in keys(s3)
        self._finish(b, local)
        assert keys(s3) == sorted([MANIFEST_KEY, OUTPUT_KEY])

    def test_empty_tile_year_writes_a_marker(self, s3):
        marker = f"{PREFIX}/data/tile_id={TILE_ID}/year={YEAR}/_EMPTY"
        cp = job(0, "a")
        cp.initialize(GRANULES, [OUTPUT_KEY, marker])
        cp.add_batch(None, [])
        cp.commit(None, marker)
        assert body(s3, marker) == b""
        assert manifest(s3)["state"] == DONE

    def test_keeps_a_higher_generations_parts(self, s3, local):
        a = job(0, "a")
        a.initialize(GRANULES, [OUTPUT_KEY])
        newer = f"{CHECKPOINT_PREFIX}part-g1-z-0000.parquet"
        s3.put_object(Bucket=BUCKET, Key=newer, Body=b"z")
        self._finish(a, local)
        assert newer in keys(s3)

    def test_job_that_lost_the_checkpoint_writes_no_output(self, s3, local):
        old = job(0, "old")
        old.initialize(GRANULES, [OUTPUT_KEY])
        old.add_batch(local("p", b"p"), [])
        job(1, "new").initialize(GRANULES, [OUTPUT_KEY])
        with pytest.raises(CheckpointConflict):
            old.commit(local("old", b"old"), OUTPUT_KEY)
        assert OUTPUT_KEY not in keys(s3)


class TestOutputRace:
    """An old job that confirmed it held the checkpoint just before a newer
    job claimed it may still upload its output. The newer job's output is
    the one left, in either order."""

    def _race(self, local):
        old = job(0, "old")
        old.initialize(GRANULES, [OUTPUT_KEY])
        old.add_batch(local("p", b"p"), [])
        old._update(old.manifest)  # old confirms, as commit() does first
        new = job(1, "new")
        new.initialize(GRANULES, [OUTPUT_KEY])
        return old, new

    def test_old_upload_first_is_replaced(self, s3, local):
        old, new = self._race(local)
        old._upload(local("old", b"old"), OUTPUT_KEY, old.output_etags[OUTPUT_KEY])
        new.commit(local("new", b"new"), OUTPUT_KEY)
        assert body(s3, OUTPUT_KEY) == b"new"
        assert manifest(s3)["state"] == DONE

    def test_old_upload_second_is_refused(self, s3, local):
        old, new = self._race(local)
        new.commit(local("new", b"new"), OUTPUT_KEY)
        with pytest.raises(ClientError):
            old._upload(local("old", b"old"), OUTPUT_KEY, old.output_etags[OUTPUT_KEY])
        assert body(s3, OUTPUT_KEY) == b"new"

    def test_rebuild_over_an_existing_output(self, s3, local):
        s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=b"previous")
        cp = job(0, "a")
        cp.initialize(GRANULES, [OUTPUT_KEY])
        TestCommit()._finish(cp, local, b"rebuilt")
        assert body(s3, OUTPUT_KEY) == b"rebuilt"


class TestAfterDone:
    @pytest.fixture
    def done(self, s3, local):
        cp = job(1, "a")
        cp.initialize(GRANULES, [OUTPUT_KEY])
        cp.add_batch(local("p", b"p"), [])
        cp.commit(local("out", b"out"), OUTPUT_KEY)

    def test_same_generation_finds_it_built(self, s3, done):
        m = job(1, "b").initialize(GRANULES, [OUTPUT_KEY])
        assert m.state == DONE
        assert manifest(s3)["job_id"] == "a"

    def test_lower_generation_stops(self, s3, done):
        with pytest.raises(CheckpointConflict):
            job(0, "old").initialize(GRANULES, [OUTPUT_KEY])

    def test_higher_generation_rebuilds_from_scratch(self, s3, done):
        m = job(2, "c").initialize(GRANULES, [OUTPUT_KEY])
        assert (m.state, m.remaining, m.parts) == (RUNNING, GRANULES, [])
