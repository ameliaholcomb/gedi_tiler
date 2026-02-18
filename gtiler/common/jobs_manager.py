from maap.maap import MAAP
import enum
import pandas as pd
import time
from tqdm import tqdm
import requests


class JobStatus(enum.Enum):
    RUNNING = "Running"
    FAILED = "Failed"
    ACCEPTED = "Accepted"
    SUCCEEDED = "Succeeded"
    OFFLINE = "Offline"
    DELETED = "Deleted"  # often used before a job starts


def _tabify_jobs(jobs_list):
    rows = []
    for j in jobs_list:
        job_id = list(j.keys())[0]
        j = list(j.values())[0]
        tag = j["context"].get("tag")
        tile_id = j["context"].get("tile_id")
        status = j.get("status")
        error = j.get("short_error")
        error_msg = j.get("error")
        rows.append(
            {
                "tile_id": tile_id,
                "job_id": job_id,
                "tag": tag,
                "status": status,
                "error": error,
                "error_message": error_msg,
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "tile_id",
            "job_id",
            "tag",
            "status",
            "error",
            "error_message",
        ],
    ).set_index("tile_id")


class JobsManager:
    def __init__(
        self,
        job_code: str,
        job_iteration: int,
        s3_bucket: str,
        s3_prefix: str,
        algorithm_id: str,
        algorithm_version: str,
        tile_ids: list[str],
    ):
        self.maap = MAAP(maap_host="api.maap-project.org")
        self.jobs_prefix = f"tiler_{job_code}"  # tag prefix to identify jobs
        self.jobs_name = f"tiler_{job_code}_{job_iteration}"  # unique run name
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.algorithm_id = algorithm_id
        self.algorithm_version = algorithm_version
        self.tiles = list(set(tile_ids))

    def manage(self):
        tqdm.write("Checking for existing and completed jobs ...")
        succeeded_jobs = _tabify_jobs(self._fetch_jobs(JobStatus.SUCCEEDED))
        tqdm.write(f"Total succeeded jobs for this region: {len(succeeded_jobs)}")
        succeeded_subset = succeeded_jobs.loc[succeeded_jobs.index.intersection(self.tiles)]
        tqdm.write(f"Completion of remaining tiles: {len(succeeded_subset)}/{len(self.tiles)}")

        # TODO: this could be a while loop that keeps track of jobs
        # and issues new ones until they have all completed.
        if len(succeeded_subset) < len(self.tiles):
            self.submit_new_jobs()
            exit(0)

    def get_unstarted_tiles(self):
        running_jobs = _tabify_jobs(self._fetch_jobs(JobStatus.RUNNING))
        accepted_jobs = _tabify_jobs(self._fetch_jobs(JobStatus.ACCEPTED))
        succeeded_jobs = _tabify_jobs(self._fetch_jobs(JobStatus.SUCCEEDED))
        offline_jobs = _tabify_jobs(self._fetch_jobs(JobStatus.OFFLINE))
        started_tiles = (
            set(running_jobs.index)
            | set(accepted_jobs.index)
            | set(succeeded_jobs.index)
            | set(offline_jobs.index)
        )
        for tile_id in self.tiles:
            if tile_id not in started_tiles:
                yield tile_id

    def submit_new_jobs(self):
        for i, tile_id in enumerate(self.get_unstarted_tiles()):
            print(f"Submitting job for tile {tile_id}...")
            job_name = f"{self.jobs_name}_{tile_id}"
            if (
                "N50" in tile_id
                or "S50" in tile_id
                or "S51" in tile_id
                or "N51" in tile_id
            ):
                queue = "maap-dps-worker-16gb"
            else:
                queue = "maap-dps-worker-8gb"
            self.maap.submitJob(
                identifier=job_name,
                algo_id=self.algorithm_id,
                version=self.algorithm_version,
                queue=queue,
                bucket=self.s3_bucket,
                prefix=self.s3_prefix,
                tile_id=tile_id,
                checkpoint_interval=25,
                quality="quality",
            )
            if i % 50 == 0:
                time.sleep(60)

    def _fetch_jobs(self, status: JobStatus):
        kwargs = dict(
            version=self.algorithm_version,
            page_size=500,
            algo_id=f"job-{self.algorithm_id}",
            status=status.value,
        )
        offset = 0
        all_jobs = []
        while True:
            ret = self.maap.listJobs(offset=offset, **kwargs)
            ret.raise_for_status()
            jobs = ret.json()
            if not jobs.get("jobs"):
                return all_jobs
            jobs_list = jobs["jobs"]
            offset += len(jobs_list)
            jobs_list_filt = [
                j
                for j in jobs_list
                if list(j.values())[0]["context"]
                .get("tag", "")
                .startswith(self.jobs_prefix)
            ]
            all_jobs.extend(jobs_list_filt)
