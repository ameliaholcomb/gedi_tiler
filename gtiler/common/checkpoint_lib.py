import boto3
from botocore.exceptions import ClientError
from dataclasses import dataclass
import logging
import pandas as pd
import pickle

from gtiler.common import s3_utils

logger = logging.getLogger(__name__)


@dataclass
class CheckpointData:
    granules_to_process: list
    processed_data: pd.DataFrame
    generation: int = 0

    def __str__(self):
        return f"""
            CheckpointData(\
                gen={self.generation}, \
                remaining={len(self.granules_to_process)} granules, \
                processed={len(self.processed_data)} shots)"""


class CheckpointConflict(Exception):
    """Another job wrote a checkpoint with equal or higher generation."""

    pass


class Checkpointer:
    def __init__(self, bucket: str, prefix: str, tile_id: str, generation: int):
        self.bucket = bucket
        self.checkpoint_key = f"{prefix}/checkpoints/{tile_id}/checkpoint.pkl"
        self.tile_id = tile_id
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
                checkpoint.granules_to_process, checkpoint.processed_data
            )
            return checkpoint.granules_to_process, checkpoint.processed_data
        else:
            return None

    def _legacy_parse_checkpoint(self, checkpoint_tuple):
        """Parse checkpoints from before the CheckpointData class."""
        # TODO: Remove this function after all existing checkpoints
        # have been updated or made obsolete
        return CheckpointData(
            granules_to_process=checkpoint_tuple[0],
            processed_data=checkpoint_tuple[1],
        )

    def read_checkpoint(self) -> CheckpointData:
        """Read the checkpoint from S3 and return the CheckpointData."""
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=self.bucket, Key=self.checkpoint_key)
        self.etag = response["ETag"]
        checkpoint = pickle.loads(response["Body"].read())
        if not isinstance(checkpoint, CheckpointData):
            checkpoint = self._legacy_parse_checkpoint(checkpoint)
        if checkpoint.generation > self.generation:
            raise CheckpointConflict(f"Read gen {checkpoint.generation} > {self.generation}")
        return checkpoint

    def write_checkpoint(
        self,
        granules_to_process: list,
        processed_data: pd.DataFrame,
    ):
        """
        Write the checkpoint to S3 using multipart upload
        with optimistic concurrency control.
        """
        checkpoint = CheckpointData(
            generation=self.generation,
            granules_to_process=granules_to_process,
            processed_data=processed_data,
        )
        logger.info("Writing checkpoint: %s", checkpoint)
        try:
            if self.etag is None:  # First write, no existing checkpoint
                self.etag = s3_utils.conditional_multipart_put(
                    bucket=self.bucket,
                    key=self.checkpoint_key,
                    data=pickle.dumps(checkpoint),
                    if_none_match="*",
                )
            else:
                self.etag = s3_utils.conditional_multipart_put(
                    bucket=self.bucket,
                    key=self.checkpoint_key,
                    data=pickle.dumps(checkpoint),
                    if_match=self.etag,
                )
        except ClientError as e:
            if e.response["Error"]["Code"] != "PreconditionFailed":
                raise
            existing_checkpoint = self.read_checkpoint()
            if existing_checkpoint.generation >= self.generation:
                raise CheckpointConflict(f"Read gen {existing_checkpoint.generation} > {self.generation}")
            else:
                logger.info(
                    "Checkpoint generation %d lower than job generation %d, retrying write ...",
                    existing_checkpoint.generation,
                    self.generation,
                )
                return self.write_checkpoint(
                    granules_to_process, processed_data
                )
