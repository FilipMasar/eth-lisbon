"""workflow.py.

run similar bacalau jobs for a list of input hashes (representing vertically split input datasets on IPFS),
check the status of the jobs in parallel,
if the results are all succesful, start the aggregation
"""
import glob
import json
import multiprocessing
import os
import shutil
import subprocess
import tempfile
import time
from enum import Enum
from functools import partial
from pprint import pprint
from typing import Final, List, Optional, Tuple

fileHashesTrain: Final[str] = "hashes_train.txt"
# imageName: Final[str] = "filipmasar/eth-lisbon:latest"
imageName: Final[str] = "filipmasar/eth-lisbon:ml7"
NTRY_MAX: Final[int] = 5


class JobType(Enum):
    train = 1
    aggregate = 2


def checkStatusOfJob(job_id: str) -> Tuple[str, Optional[str]]:
    """Check the status of a Bacalhau job."""
    assert len(job_id) > 0
    p = subprocess.run(
        ["bacalhau", "list", "--output", "json", "--id-filter", job_id],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    r, optional_cid = parseJobStatus(p.stdout)

    if r == "":
        print("job status is empty! %s" % job_id)
    elif r == "Completed":
        print("job completed: %s" % job_id)
    elif r == "Error":
        print("job returns error: %s" % job_id)
        pprint(json.loads(p.stdout))
    else:
        print("job not completed: %s - %s" % (job_id, r))

    return r, optional_cid


def submitJob(jobType: JobType, cid: str) -> str:
    """Submit a job to the Bacalhau network."""
    assert len(cid) > 0
    p = subprocess.run(
        [
            "bacalhau",
            "docker",
            "run",
            "--id-only",
            "--wait=false",
            "--input",
            "ipfs://" + cid + ":/inputs/",
            imageName,
            "--",
            "python",
            "main.py",
            f"--{jobType.name}",
            "--input=/inputs",
            "--output=/outputs",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if p.returncode != 0:
        print("failed (%d) job: %s" % (p.returncode, p.stdout))

    job_id = p.stdout.strip()
    print("%s job submitted: %s" % (jobType, job_id))

    return job_id


def getResultsFromJob(job_id: str) -> str:
    """Get results from a Bacalhau job."""
    assert len(job_id) > 0
    temp_dir = tempfile.mkdtemp()
    print("getting results for job: %s" % job_id)
    # try max NTRY_MAX times
    for i in range(0, NTRY_MAX):
        p = subprocess.run(
            [
                "bacalhau",
                "get",
                "--output-dir",
                temp_dir,
                job_id,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode == 0:
            break
        else:
            print("failed (exit %d) to get job: %s" % (p.returncode, p.stdout))

    return temp_dir


def getOutputCidForJob(job_id: str) -> str:
    raise NotImplementedError


def parseJobStatus(result: str) -> Tuple[str, Optional[str]]:
    """Parse the status and CID of a Bacalhau job."""
    output_cid: Optional[str] = None
    if len(result) == 0:
        return "", output_cid
    r = json.loads(result)
    if len(r) > 0:
        # print("r[0]: " + json.dumps(r[0]))
        # uncomment to see full error spec
        # pprint(r[0])
        state: str = r[0]["State"]["State"]

        if state == "Completed":
            # print("published results: ")
            # pprint(r[0]["State"])
            res = [
                execRes
                for execRes in r[0]["State"]["Executions"]
                if len(execRes["PublishedResults"]) > 0
            ]
            output_cid = res[0]["PublishedResults"]["CID"]

        return state, output_cid

    return "", output_cid


def parseHashes(filename: str) -> list:
    """Split lines from a text file into a list."""
    assert os.path.exists(filename)
    with open(filename, "r") as f:
        hashes = f.read().splitlines()
    return hashes


def main(file: str = fileHashesTrain, num_files: int = -1):
    # Use multiprocessing to work in parallel
    count = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=count) as pool:
        hashes = parseHashes(file)[:num_files]
        print("submitting %d jobs" % len(hashes))
        submitTrainingJob = partial(submitJob, JobType.train)
        job_ids = pool.map(submitTrainingJob, hashes)
        assert len(job_ids) == len(hashes)

        print("waiting for training jobs to complete...")
        while True:
            training_job_statuses = pool.map(checkStatusOfJob, job_ids)
            total_finished = sum(
                map(lambda x: x[0] == "Completed", training_job_statuses)
            )
            if total_finished >= len(job_ids):
                break
            print("%d/%d jobs completed" % (total_finished, len(job_ids)))
            time.sleep(2)

        print("all training jobs completed, saving results...")
        results = pool.map(getResultsFromJob, job_ids)
        # print(results)
        print("finished saving results / training")

        # Do something with the results
        # shutil.rmtree("results", ignore_errors=True)
        # os.makedirs("results", exist_ok=True)
        # for r in results:
        #     path = os.path.join(r, "outputs", "*.csv")
        #     csv_file = glob.glob(path)
        #     for f in csv_file:
        #         print("moving %s to results" % f)
        #         shutil.move(f, "results")

        # run the aggregation
        print("----------------------------------")
        print("running aggregation")
        print("----------------------------------")
        print("job_statuses: ")
        print(training_job_statuses)
        output_train_hashes: List[str] = [r[0] for r in training_job_statuses]
        submitAggregationJob = partial(submitJob, JobType.aggregate)
        aggregate_job_ids = pool.map(submitAggregationJob, output_train_hashes)

        assert len(aggregate_job_ids) == len(output_train_hashes)

        print("waiting for aggregation jobs to complete...")
        while True:
            aggregation_job_statuses = pool.map(checkStatusOfJob, aggregate_job_ids)
            total_finished = sum(
                map(lambda x: x[0] == "Completed", aggregation_job_statuses)
            )
            if total_finished >= len(aggregate_job_ids):
                break
            print(
                "%d/%d aggregation jobs completed"
                % (total_finished, len(aggregate_job_ids))
            )
            time.sleep(2)

        print("all aggregation jobs completed, saving results...")
        results = pool.map(getResultsFromJob, aggregate_job_ids)


if __name__ == "__main__":
    main("hashes_train.txt", 2)
