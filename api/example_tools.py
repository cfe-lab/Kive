"Tools to reduce boilerplate in the API examples."
import time


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
