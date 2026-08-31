import boto3
from botocore.exceptions import ClientError
from dataclasses import dataclass
import logging
import pandas as pd
import pickle
import tempfile

from gtiler.common import s3_utils

logger = logging.getLogger(__name__)


@dataclass
class CheckpointData:
    granules_to_process: list
    processed_data: pd.DataFrame
    quality_filter: bool = True
    generation: int = 0

    def __str__(self):
        return f"""
            CheckpointData(\
                gen={self.generation}, \
                quality_filter={self.quality_filter}, \
                remaining={len(self.granules_to_process)} granules, \
                processed={len(self.processed_data)} shots)"""


class CheckpointConflict(Exception):
    """Another job wrote a checkpoint with equal or higher generation."""

    pass


class Checkpointer:
    def __init__(
        self,
        bucket: str,
        prefix: str,
        tile_id: str,
        year: int,
        generation: int,
    ):
        self.bucket = bucket
        self.checkpoint_key = (
            f"{prefix}/checkpoints/{tile_id}/{year}/checkpoint.pkl"
        )
        self.tile_id = tile_id
        self.year = year
        self.generation = generation
        self.etag = None

    def initialize(self):
        """Load initial checkpoint state from S3, if it exists.
        This function is intended to be called at the start of a job
        to resume from existing checkpoints. If the existing checkpoint
        is of a lower generation, it will be immediately overwritten
        to signal other jobs to terminate.
        """
        checkpoint_url = f"s3://{self.bucket}/{self.checkpoint_key}"
        if s3_utils.s3_prefix_exists(checkpoint_url):
            logger.info("Restoring from checkpoint ...")
            checkpoint = self.read_checkpoint()
            logger.info("%s", checkpoint)
            # immediately try to write the checkpoint back to claim ownership
            # of this generation
            self.write_checkpoint(
                checkpoint.granules_to_process,
                checkpoint.processed_data,
                checkpoint.quality_filter,
            )
            return (
                checkpoint.granules_to_process,
                checkpoint.processed_data,
                checkpoint.quality_filter,
            )
        else:
            return None

    def read_checkpoint(self) -> CheckpointData:
        """Read the checkpoint from S3 and return the CheckpointData."""
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=self.bucket, Key=self.checkpoint_key)
        self.etag = response["ETag"]
        checkpoint = pickle.loads(response["Body"].read())
        if checkpoint.generation > self.generation:
            raise CheckpointConflict(f"Read gen {checkpoint.generation} > {self.generation}")
        return checkpoint

    def write_checkpoint(
        self,
        granules_to_process: list,
        processed_data: pd.DataFrame,
        quality_filter: bool = True,
    ):
        """
        Write the checkpoint to S3 using multipart upload
        with optimistic concurrency control.

        The checkpoint is pickled to a temporary file on disk and then
        streamed to S3 in chunks, so peak memory stays at roughly the
        size of `processed_data` rather than 2x.
        """
        checkpoint = CheckpointData(
            generation=self.generation,
            granules_to_process=granules_to_process,
            processed_data=processed_data,
            quality_filter=quality_filter,
        )
        logger.info("Writing checkpoint: %s", checkpoint)
        while True:
            with tempfile.TemporaryFile() as tmp:
                pickle.dump(checkpoint, tmp, protocol=pickle.HIGHEST_PROTOCOL)
                tmp.seek(0)
                try:
                    if self.etag is None:  # First write, no existing checkpoint
                        self.etag = s3_utils.conditional_multipart_put(
                            bucket=self.bucket,
                            key=self.checkpoint_key,
                            body=tmp,
                            if_none_match="*",
                        )
                    else:
                        self.etag = s3_utils.conditional_multipart_put(
                            bucket=self.bucket,
                            key=self.checkpoint_key,
                            body=tmp,
                            if_match=self.etag,
                        )
                    return
                except ClientError as e:
                    if e.response["Error"]["Code"] != "PreconditionFailed":
                        raise
                    existing_checkpoint = self.read_checkpoint()
                    if existing_checkpoint.generation >= self.generation:
                        raise CheckpointConflict(
                            f"Read gen {existing_checkpoint.generation} > {self.generation}"
                        )
                    logger.info(
                        "Checkpoint generation %d lower than job generation %d, retrying write ...",
                        existing_checkpoint.generation,
                        self.generation,
                    )
                    # loop and re-pickle into a fresh tempfile
