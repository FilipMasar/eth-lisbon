"""workflow.py.

run similar bacalau jobs for a list of input hashes (representing vertically split input datasets on IPFS),
check the status of the jobs in parallel,
if the results are all succesful, start the aggregation
"""
import json
import multiprocessing
import os
import subprocess
import tempfile
import time
from enum import Enum
from functools import partial
from pprint import pprint
from typing import Final, List, Optional, Tuple

fileHashesTrain: Final[str] = "hashes_train.txt"
imageName: Final[str] = "filipmasar/eth-lisbon:ml9"
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


def submitJob(jobType: JobType, input_cids: List[str]) -> str:
    """Submit a job to the Bacalhau network."""
    # ugly
    if isinstance(input_cids, str):
        input_cids = [input_cids]

    assert isinstance(
        input_cids, list
    ), f"should be passing list of cids. {type(input_cids)=}"

    for cid in input_cids:
        assert len(cid) > 0

    base_command: List[str] = [
        "bacalhau",
        "docker",
        "run",
        "--id-only",
        "--wait=false",
    ]

    # we use subdir with aggregate jobs, not with training jobs?
    withSubDir: bool = len(input_cids) > 1
    for count, cid in enumerate(input_cids, start=1):
        inputsMount: str = f":/inputs/{count}" if withSubDir else ":/inputs"
        base_command += ["--input", "ipfs://" + cid + inputsMount]

    command: List[str] = base_command + [
        imageName,
        "--",
        "python",
        "main.py",
        f"--{jobType.name}",
        "--input=/inputs",
        "--output=/outputs",
    ]

    p = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if p.returncode != 0:
        print("failed %s (%d) job: %s" % (jobType.name, p.returncode, p.stdout))

    job_id = p.stdout.strip()
    print("%s job submitted: %s" % (jobType.name, job_id))

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
        cids = parseHashes(file)[:num_files]
        print("----------------------------------")
        print("running local training on %d files" % len(cids))
        print("----------------------------------")
       
        submitTrainingJob = partial(submitJob, JobType.train)
        job_ids = pool.map(submitTrainingJob, cids)
        assert len(job_ids) == len(cids)

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

        print(f"{training_job_statuses=}")

        # run the aggregation
        print("----------------------------------")
        print("running aggregation")
        print("----------------------------------")
        output_train_hashes: List[str] = [r[-1] for r in training_job_statuses]
        print("local training results: ", output_train_hashes)
        submitAggregationJob = partial(submitJob, JobType.aggregate)
        aggregation_job_id = submitAggregationJob(output_train_hashes)

        print("waiting for aggregation job to complete...")
        while True:
            aggregation_job_status: Tuple[str, Optional[str]] = checkStatusOfJob(
                aggregation_job_id
            )
            if aggregation_job_status[0] == "Completed":
                break

            print("aggregation job completed")
            time.sleep(2)

        print("aggregation job completed, saving results...")
        subprocess.run(["bacalhau", "get", aggregation_job_id])


if __name__ == "__main__":
    main("hashes_train.txt", 2)
