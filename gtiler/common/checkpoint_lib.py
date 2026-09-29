"""Resumable, generation-fenced checkpoints for a tile-year build.

A tile-year's checkpoint lives under {prefix}/checkpoints/{tile_id}/{year}/:
a small JSON manifest, plus one parquet part per batch of granules holding
that batch's shots. The manifest lists the granules still to process and
the parts that hold the rest; a part counts only once the manifest lists
it.

Concurrency. Every manifest write is conditional on the ETag this job last
saw, so a job that loses a race fails its next write and stops with
CheckpointConflict:
- A job claims the manifest when it starts, taking over the progress
  already recorded there, including another generation's. The newest claim
  wins; a job of a lower generation than the manifest's stops at once.
- Parts are named with the generation and a per-job ID, so two jobs never
  write the same part.
- The output is uploaded on condition that it is still as this job found
  it when claiming. If another job uploaded in between, this job confirms
  it still holds the manifest and uploads over it; a job that has lost the
  manifest stops without writing. So the newest claim's output is the one
  left, whatever order the uploads finish in.
- After the output, the manifest is marked done, and only then are the
  parts deleted. The done manifest stays, so a job that turns up later
  stops (lower generation) or finds nothing to do (same generation). A
  higher generation rebuilds from scratch.
"""

import boto3
from botocore.exceptions import ClientError
from dataclasses import asdict, dataclass, field, replace
import datetime
import json
import logging
import os
import uuid

from gtiler.common import s3_utils

logger = logging.getLogger(__name__)

RUNNING = "running"
DONE = "done"


@dataclass
class Manifest:
    generation: int
    job_id: str
    remaining: list
    parts: list = field(default_factory=list)
    state: str = RUNNING

    def __str__(self):
        return (
            f"Manifest(gen={self.generation}, job={self.job_id}, "
            f"state={self.state}, remaining={len(self.remaining)} granules, "
            f"parts={len(self.parts)})"
        )


class CheckpointConflict(Exception):
    """Another job holds the checkpoint: one of an equal or higher
    generation claimed it after this job did."""

    pass


def _precondition_failed(e: ClientError) -> bool:
    # S3 answers a failed If-None-Match on an existing object with 412
    # PreconditionFailed, and a concurrent conditional write with 409.
    return e.response["Error"]["Code"] in (
        "PreconditionFailed",
        "ConditionalRequestConflict",
    )


class Checkpointer:
    def __init__(
        self,
        bucket: str,
        prefix: str,
        tile_id: str,
        year: int,
        generation: int,
        job_id: str = None,
    ):
        self.bucket = bucket
        self.checkpoint_prefix = f"{prefix}/checkpoints/{tile_id}/{year}/"
        self.manifest_key = f"{self.checkpoint_prefix}manifest.json"
        self.generation = generation
        self.job_id = job_id or uuid.uuid4().hex[:12]
        self.s3 = boto3.client("s3")
        self.manifest = None
        self.etag = None

    def _read(self):
        """The manifest in S3 and its ETag, or (None, None)."""
        try:
            response = self.s3.get_object(
                Bucket=self.bucket, Key=self.manifest_key
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None, None
            raise
        return Manifest(**json.loads(response["Body"].read())), response["ETag"]

    def _put(self, manifest: Manifest, etag: str):
        """Write the manifest if S3 still holds the one with this ETag (or
        none, for None). Returns the new ETag."""
        kw = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
        return self.s3.put_object(
            Bucket=self.bucket,
            Key=self.manifest_key,
            Body=json.dumps(asdict(manifest)).encode(),
            **kw,
        )["ETag"]

    def _update(self, manifest: Manifest):
        """Write the manifest over the one this job last wrote, or stop if
        another job has claimed it since."""
        try:
            self.etag = self._put(manifest, self.etag)
        except ClientError as e:
            if not _precondition_failed(e):
                raise
            current, _ = self._read()
            raise CheckpointConflict(
                f"Job {self.job_id} (gen {self.generation}) lost the "
                f"checkpoint to {current}"
            )
        self.manifest = manifest

    def initialize(self, granules: list, output_keys: list) -> Manifest:
        """Claim the tile-year's checkpoint and return the manifest to work
        from: a new one listing every granule, or the recorded progress to
        resume. A manifest in state DONE means the tile-year is already
        built for this generation.

        Args:
            granules: Keys of all granules the tile-year needs, used when
                there is no progress to resume.
            output_keys: Keys the tile-year's output may go to (data
                file, empty marker), whose current state conditions the
                final upload.
        """
        while True:
            current, etag = self._read()
            if current is None or (
                current.state == DONE and current.generation < self.generation
            ):
                claimed = Manifest(self.generation, self.job_id, list(granules))
            elif current.generation > self.generation:
                raise CheckpointConflict(
                    f"Job gen {self.generation} found {current}"
                )
            elif current.state == DONE:
                logger.info("Already built: %s", current)
                return current
            else:
                claimed = replace(
                    current, generation=self.generation, job_id=self.job_id
                )
            # Read before the claim, so an upload by a job that claimed
            # earlier changes it and fails this job's condition.
            self.output_etags = {
                key: s3_utils.object_etag(self.bucket, key) for key in output_keys
            }
            try:
                self.etag = self._put(claimed, etag)
            except ClientError as e:
                if not _precondition_failed(e):
                    raise
                # Another job wrote in between; claim from what it wrote.
                continue
            self.manifest = claimed
            self.claimed_at = datetime.datetime.now(datetime.timezone.utc)
            logger.info("Claimed checkpoint: %s", claimed)
            return claimed

    def download_parts(self, work_dir: str) -> list:
        """Download the manifest's parts into work_dir, returning the
        local paths."""
        paths = []
        for key in self.manifest.parts:
            path = os.path.join(work_dir, key.rsplit("/", 1)[1])
            self.s3.download_file(self.bucket, key, path)
            paths.append(path)
        return paths

    def add_batch(self, local_part: str, remaining: list):
        """Record a finished batch: upload its part (None if the batch had
        no shots), then list it in the manifest with the granules left."""
        parts = list(self.manifest.parts)
        if local_part is not None:
            key = (
                f"{self.checkpoint_prefix}part-g{self.generation}-"
                f"{self.job_id}-{len(parts):04d}.parquet"
            )
            with open(local_part, "rb") as f:
                s3_utils.conditional_multipart_put(
                    self.bucket, key, f, if_none_match="*"
                )
            parts.append(key)
        self._update(replace(self.manifest, parts=parts, remaining=list(remaining)))
        logger.info("Wrote checkpoint: %s", self.manifest)

    def commit(self, local_path: str, output_key: str):
        """Upload the tile-year's output from local_path (None for an empty
        marker), mark the checkpoint done, and delete the parts."""
        assert not self.manifest.remaining, self.manifest
        # Confirm this job still holds the checkpoint before writing.
        self._update(self.manifest)
        expected = self.output_etags[output_key]
        while True:
            try:
                self._upload(local_path, output_key, expected)
                break
            except ClientError as e:
                if not _precondition_failed(e):
                    raise
                # A job that claimed earlier wrote the output after this
                # job claimed. Write over it, if this job still holds the
                # checkpoint.
                self._update(self.manifest)
                expected = s3_utils.object_etag(self.bucket, output_key)
                logger.info("Output changed since the claim; replacing it.")
        self._update(replace(self.manifest, state=DONE, parts=[]))
        logger.info("Committed %s: %s", output_key, self.manifest)
        self._clean_up(output_key)

    def _upload(self, local_path, output_key, expected):
        kw = {"if_match": expected} if expected else {"if_none_match": "*"}
        if local_path is None:
            s3_utils.conditional_put(self.bucket, output_key, b"", **kw)
        else:
            with open(local_path, "rb") as f:
                s3_utils.conditional_multipart_put(
                    self.bucket, output_key, f, **kw
                )

    def _clean_up(self, output_key):
        """Delete the parts under the checkpoint prefix, including other
        jobs' unlisted ones, and abort uploads that jobs killed mid-write
        left behind. Leaves alone anything a higher generation, which may
        have started since, could be writing: its parts, and uploads begun
        after this job's claim."""
        keys = [
            obj["Key"]
            for page in self.s3.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=self.checkpoint_prefix
            )
            for obj in page.get("Contents", [])
            if obj["Key"] != self.manifest_key
            and _part_generation(obj["Key"]) <= self.generation
        ]
        for i in range(0, len(keys), 1000):
            self.s3.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]},
            )
        for prefix in (self.checkpoint_prefix, output_key):
            uploads = self.s3.list_multipart_uploads(
                Bucket=self.bucket, Prefix=prefix
            ).get("Uploads", [])
            for u in uploads:
                if u["Initiated"] >= self.claimed_at:
                    continue
                self.s3.abort_multipart_upload(
                    Bucket=self.bucket, Key=u["Key"], UploadId=u["UploadId"]
                )
        logger.info("Deleted %d checkpoint parts.", len(keys))


def _part_generation(key: str) -> int:
    """The generation in a part's name, part-g{generation}-{job}-{n}."""
    return int(key.rsplit("/", 1)[1].split("-")[1].removeprefix("g"))
