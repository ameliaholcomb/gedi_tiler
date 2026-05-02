import boto3
import pickle
import pytest
import pandas as pd
from botocore.exceptions import ClientError
from moto import mock_aws
from unittest.mock import patch

from gtiler.common.checkpoint_lib import CheckpointData, CheckpointConflict, Checkpointer


BUCKET = "test-bucket"
PREFIX = "test/prefix"
TILE_ID = "tile-001"
GENERATION = 5
CHECKPOINT_KEY = f"{PREFIX}/checkpoints/{TILE_ID}/checkpoint.pkl"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def checkpointer():
    return Checkpointer(bucket=BUCKET, prefix=PREFIX, tile_id=TILE_ID, generation=GENERATION)


@pytest.fixture
def sample_data():
    return (
        ["granule_a", "granule_b"],
        pd.DataFrame({"shot_id": [1, 2, 3]}),
    )


@pytest.fixture
def sample_checkpoint(sample_data):
    granules, processed = sample_data
    return CheckpointData(
        granules_to_process=granules,
        processed_data=processed,
        generation=GENERATION,
    )


def _put(s3_client, checkpoint: CheckpointData) -> str:
    """Write a checkpoint directly to S3, return ETag."""
    return s3_client.put_object(
        Bucket=BUCKET, Key=CHECKPOINT_KEY, Body=pickle.dumps(checkpoint)
    )["ETag"]


def _get(s3_client) -> CheckpointData:
    """Read and deserialize the checkpoint currently in S3."""
    return pickle.loads(
        s3_client.get_object(Bucket=BUCKET, Key=CHECKPOINT_KEY)["Body"].read()
    )


# ---------------------------------------------------------------------------
# CheckpointData
# ---------------------------------------------------------------------------

class TestCheckpointData:
    def test_str_shows_generation_remaining_processed(self):
        cp = CheckpointData(
            generation=3,
            granules_to_process=["a", "b"],
            processed_data=pd.DataFrame({"x": range(7)}),
        )
        s = str(cp)
        assert "gen=3" in s
        assert "remaining=2" in s
        assert "processed=7" in s

    def test_default_generation_is_zero(self):
        cp = CheckpointData(granules_to_process=[], processed_data=pd.DataFrame())
        assert cp.generation == 0


# ---------------------------------------------------------------------------
# Checkpointer construction
# ---------------------------------------------------------------------------

class TestCheckpointerConstruction:
    def test_checkpoint_key_format(self):
        cp = Checkpointer(BUCKET, PREFIX, TILE_ID, GENERATION)
        assert cp.checkpoint_key == CHECKPOINT_KEY

    def test_initial_etag_is_none(self):
        assert Checkpointer(BUCKET, PREFIX, TILE_ID, GENERATION).etag is None


# ---------------------------------------------------------------------------
# Checkpointer.initialize
# ---------------------------------------------------------------------------

class TestInitialize:
    @patch("gtiler.common.s3_utils.s3_prefix_exists", return_value=False)
    def test_returns_none_when_no_checkpoint_exists(self, _mock, checkpointer):
        assert checkpointer.initialize() is None

    @patch("gtiler.common.s3_utils.s3_prefix_exists", return_value=False)
    def test_checks_correct_s3_url(self, mock_exists, checkpointer):
        checkpointer.initialize()
        mock_exists.assert_called_once_with(f"s3://{BUCKET}/{CHECKPOINT_KEY}")

    @patch("gtiler.common.s3_utils.s3_prefix_exists", return_value=True)
    def test_restores_checkpoint_and_claims_ownership(
        self, _mock, s3, checkpointer, sample_checkpoint, sample_data
    ):
        granules, processed = sample_data
        _put(s3, sample_checkpoint)

        result = checkpointer.initialize()

        assert result[0] == granules
        assert result[1].equals(processed)
        assert checkpointer.etag is not None
        assert _get(s3).generation == GENERATION


# ---------------------------------------------------------------------------
# Checkpointer.read_checkpoint
# ---------------------------------------------------------------------------

class TestReadCheckpoint:
    def test_returns_checkpoint_data_and_stores_etag(
        self, s3, checkpointer, sample_checkpoint
    ):
        etag = _put(s3, sample_checkpoint)

        result = checkpointer.read_checkpoint()

        assert result.generation == GENERATION
        assert result.granules_to_process == sample_checkpoint.granules_to_process
        assert checkpointer.etag == etag

    def test_parses_legacy_tuple_format(self, s3, checkpointer):
        granules = ["g1", "g2"]
        processed = pd.DataFrame({"shot_id": [10, 20]})
        s3.put_object(
            Bucket=BUCKET, Key=CHECKPOINT_KEY, Body=pickle.dumps((granules, processed))
        )

        result = checkpointer.read_checkpoint()

        assert result.granules_to_process == granules
        assert result.processed_data.equals(processed)
        assert result.generation == 0

    def test_raises_conflict_for_higher_stored_generation(self, s3, checkpointer):
        _put(s3, CheckpointData(
            granules_to_process=[],
            processed_data=pd.DataFrame(),
            generation=GENERATION + 1,
        ))
        with pytest.raises(CheckpointConflict):
            checkpointer.read_checkpoint()

    def test_does_not_raise_for_equal_generation(self, s3, checkpointer, sample_checkpoint):
        _put(s3, sample_checkpoint)
        result = checkpointer.read_checkpoint()
        assert result.generation == GENERATION

    def test_does_not_raise_for_lower_stored_generation(self, s3, checkpointer):
        _put(s3, CheckpointData(
            granules_to_process=["g"],
            processed_data=pd.DataFrame(),
            generation=GENERATION - 1,
        ))
        result = checkpointer.read_checkpoint()
        assert result.generation == GENERATION - 1


# ---------------------------------------------------------------------------
# Checkpointer.write_checkpoint
# ---------------------------------------------------------------------------

class TestWriteCheckpoint:
    def test_first_write_creates_object_with_correct_generation(
        self, s3, checkpointer, sample_data
    ):
        granules, processed = sample_data
        checkpointer.write_checkpoint(granules, processed)

        written = _get(s3)
        assert written.generation == GENERATION
        assert written.granules_to_process == granules
        assert checkpointer.etag is not None

    def test_sequential_writes_succeed(self, s3, checkpointer, sample_data):
        granules, processed = sample_data
        checkpointer.write_checkpoint(granules, processed)

        new_granules = ["granule_c"]
        checkpointer.write_checkpoint(new_granules, processed)

        assert _get(s3).granules_to_process == new_granules

    def test_first_write_conflicts_if_object_already_exists(
        self, s3, checkpointer, sample_checkpoint, sample_data
    ):
        _put(s3, sample_checkpoint)
        granules, processed = sample_data

        with pytest.raises(CheckpointConflict):
            checkpointer.write_checkpoint(granules, processed)

    def test_stale_etag_raises_conflict_for_same_generation(
        self, s3, checkpointer, sample_data
    ):
        # moto 5.1.22 does not enforce IfMatch on CompleteMultipartUpload
        # (put_object IfMatch works; multipart IfMatch is silently ignored).
        # Patch conditional_multipart_put to inject PreconditionFailed so that
        # read_checkpoint (in the error handler) still hits the real moto state.
        granules, processed = sample_data
        _put(s3, CheckpointData(
            granules_to_process=granules,
            processed_data=processed,
            generation=GENERATION,
        ))
        checkpointer.etag = '"stale-etag"'

        precondition_failed = ClientError(
            {"Error": {"Code": "PreconditionFailed", "Message": ""}},
            "CompleteMultipartUpload",
        )
        with patch("gtiler.common.s3_utils.conditional_multipart_put", side_effect=precondition_failed):
            with pytest.raises(CheckpointConflict):
                checkpointer.write_checkpoint(granules, processed)

    def test_stale_etag_retries_when_stored_generation_is_lower(
        self, s3, checkpointer, sample_data
    ):
        # Same moto IfMatch caveat — see test above.
        # The second call in side_effect lets the retry succeed so we can verify
        # the retry logic without having to make the write actually hit S3.
        granules, processed = sample_data
        _put(s3, CheckpointData(
            granules_to_process=granules,
            processed_data=processed,
            generation=GENERATION - 1,
        ))
        checkpointer.etag = '"stale-etag"'

        precondition_failed = ClientError(
            {"Error": {"Code": "PreconditionFailed", "Message": ""}},
            "CompleteMultipartUpload",
        )
        with patch(
            "gtiler.common.s3_utils.conditional_multipart_put",
            side_effect=[precondition_failed, '"new-etag"'],
        ):
            checkpointer.write_checkpoint(granules, processed)

        assert checkpointer.etag == '"new-etag"'
