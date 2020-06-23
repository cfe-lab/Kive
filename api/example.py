import time

import kiveapi

# Use HTTPS on a real server, so your password is encrypted.
# Don't put your real password in source code, store it in a text file
# that is only readable by your user account or some more secure storage.
session = kiveapi.KiveAPI("http://localhost:8080")
session.login('kive', 'kive')

# Get the data by ID
dataset1 = session.get_dataset(1)
# or get the data by name.
example_names = session.find_datasets(name="example_names.csv")[0]

# Get app information
app = session.endpoints.containerapps.get(1)
# and app arguments information.
appargs = session.endpoints.containerapps.get(f"{app['id']}/argument_list")

# Start a run of the app, giving the dataset as an argument.
inputarg = next(a for a in appargs if a['type'] == 'I')
runspec = {
    "name": "example",
    "app": app["url"],
    "datasets": [
        {
            "argument": inputarg["url"],
            "dataset": example_names.raw["url"],
        },
    ],
}
print("Starting example run... ", end="")
created_containerrun = session.endpoints.containerruns.post(json=runspec)

# Monitor the run for completion
ACTIVE_STATES = "NSLR"
INTERVAL = 1.0
MAX_WAIT = 300.0
starttime = time.time()
elapsed = 0.0

runid = created_containerrun["id"]
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
    exit("Example timed out")

# Retrieve the output and save to a file
run_datasets = session.get(containerrun["dataset_list"]).json()
for run_dataset in run_datasets:
    if run_dataset.get("argument_type") == "O":
        dataset = session.get(run_dataset["dataset"]).json()
        filename = dataset["name"]
        print(f"  downloading {filename}")
        with open(filename, "wb") as outf:
            session.download_file(outf, dataset["download_url"])

print("Example run finished.")
