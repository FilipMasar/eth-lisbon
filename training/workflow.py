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
from pprint import pprint
from typing import Final, List

fileHashesTrain: Final[str] = "hashes_train.txt"
# imageName: Final[str] = "filipmasar/eth-lisbon:latest"
imageName: Final[str] = "filipmasar/eth-lisbon:ml7"


def checkStatusOfJob(job_id: str) -> str:
    """Check the status of a Bacalhau job."""
    assert len(job_id) > 0
    p = subprocess.run(
        ["bacalhau", "list", "--output", "json", "--id-filter", job_id],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    r = parseJobStatus(p.stdout)
    if r == "":
        print("job status is empty! %s" % job_id)
    elif r == "Completed":
        print("job completed: %s" % job_id)
    else:
        print("job not completed: %s - %s" % (job_id, r))

    return r


def submitJob(cid: str) -> str:
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
            "--train",
            "--input='/inputs'",
            "--output='/outputs'",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if p.returncode != 0:
        print("failed (%d) job: %s" % (p.returncode, p.stdout))

    job_id = p.stdout.strip()
    print("job submitted: %s" % job_id)

    return job_id


def getResultsFromJob(job_id: str) -> str:
    """Get results from a Bacalhau job."""
    assert len(job_id) > 0
    temp_dir = tempfile.mkdtemp()
    print("getting results for job: %s" % job_id)
    for i in range(0, 5):  # try 5 times
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


def parseJobStatus(result: str) -> str:
    """Parse the status of a Bacalhau job."""
    if len(result) == 0:
        return ""
    r = json.loads(result)
    if len(r) > 0:
        # print("r[0]: " + json.dumps(r[0]))
        # uncomment to see full error spec
        # pprint(r[0])
        return r[0]["State"]["State"]
    return ""


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
        job_ids = pool.map(submitJob, hashes)
        assert len(job_ids) == len(hashes)

        print("waiting for jobs to complete...")
        while True:
            job_statuses = pool.map(checkStatusOfJob, job_ids)
            total_finished = sum(map(lambda x: x == "Completed", job_statuses))
            if total_finished >= len(job_ids):
                break
            print("%d/%d jobs completed" % (total_finished, len(job_ids)))
            time.sleep(2)

        print("all jobs completed, saving results...")
        results = pool.map(getResultsFromJob, job_ids)
        print("finished saving results")

        # TODO: later
        # Do something with the results

        # shutil.rmtree("results", ignore_errors=True)
        # os.makedirs("results", exist_ok=True)
        # for r in results:
        #     path = os.path.join(r, "outputs", "*.csv")
        #     csv_file = glob.glob(path)
        #     for f in csv_file:
        #         print("moving %s to results" % f)
        #         shutil.move(f, "results")


if __name__ == "__main__":
    main("hashes_train.txt", 2)
