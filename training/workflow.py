import json
import subprocess


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
            "ipfs://" + cid + ":/inputs/data.tar.gz",
            "https://hub.docker.com/r/filipmasar/eth-lisbon:latest",
            # "https://hub.docker.com/r/filipmasar/eth-lisbon:ml7",
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
        return r[0]["State"]["State"]
    return ""
