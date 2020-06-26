"Examples of utility functions that use the Kive API."
import time

from kiveapi import KiveMalformedDataException


def await_containerrun(session, containerrun):
    """Given a `KiveAPI instance and a container run, monitor the run
    for completion and return the completed run.
    """
    ACTIVE_STATES = "NSLR"
    INTERVAL = 1.0
    MAX_WAIT = 300.0

    starttime = time.time()
    elapsed = 0.0

    runid = containerrun["id"]
    print(f"Waiting for run {runid} to finish.")

    while elapsed < MAX_WAIT:
        containerrun = session.endpoints.containerruns.get(runid)
        state = containerrun["state"]
        elapsed = round(time.time() - starttime, 2)
        if state in ACTIVE_STATES:
            print(f"Run in progress (state={state}, {elapsed}s elapsed)")
            time.sleep(INTERVAL)
        elif state == "C":
            print(f"Run {runid} finished after {elapsed}s; fetching results")
            break
        else:
            import pprint
            print(f"Run {runid} failed after {elapsed}s; exiting")
            pprint.pprint(containerrun)
            exit(1)
    else:
        exit(f"RUn {runid} timed out after {elapsed}s")

    return containerrun


# This function is mostly useful in the API examples: when the example is re-run, this function
# retrieves an existing dataset instead of creating a new one. Note that in production, we
# also compare the MD5 hashes of the files to give us more confidence that they're actually
# identical.
def upload_or_retrieve_dataset(session, name, inputfile, users=None, groups=None):
    "Create a dataset by uploading a file to Kive."
    if users is None and groups is None:
        raise ValueError("A list of users or a list of groups is required")
    try:
        dataset = session.add_dataset(name, 'None', inputfile, None, users, groups)
    except KiveMalformedDataException:
        dataset = session.find_dataset(name=name)[0]
    return dataset
