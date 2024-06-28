"Create a batch containing several container runs."
import time

import kiveapi

import example_tools

# Use HTTPS on a real server, so your password is encrypted.
# Don't put your real password in source code, store it in a text file
# that is only readable by your user account or some more secure storage.
session = kiveapi.KiveAPI("http://localhost:8000")
session.login('kive', 'kive')

# Upload data
names1 = example_tools.upload_or_retrieve_dataset(
    session,
    "API Example 4 Names 1",
    open("names1.csv"),
    None,
    ["Everyone"],
)
names2 = example_tools.upload_or_retrieve_dataset(
    session,
    "API Example 4 Names 2",
    open("names2.csv"),
    None,
    ["Everyone"],
)

# Get the app
containerfamily = session.endpoints.containerfamilies.filter(
    "name", "samplecode")[0]
container = session.get(containerfamily["containers"]).json()[0]
containerapps = session.get(container["app_list"]).json()
# Retrieve the default app for the samplecode container.
app = next(c for c in containerapps if c["name"] == "")
appargs = session.get(app["argument_list"]).json()
inputarg = next(a for a in appargs if a["type"] == "I")

# Create the batch
batchspec = {
    "name": "API Example 4 Batch",
    "groups_allowed": ["Everyone"],
}
containerbatch = session.endpoints.batches.post(json=batchspec)

# Run apps in the batch
runspec1 = {
    "name": "API Example 4 App 1",
    "app": app["url"],
    "batch": containerbatch["url"],
    "dataset_list": [
        {
            "argument": inputarg["url"],
            "dataset": names1.raw["url"],
        },
    ],
}
runspec2 = {
    "name": "API Example 4 App 2",
    "app": app["url"],
    "batch": containerbatch["url"],
    "dataset_list": [
        {
            "argument": inputarg["url"],
            "dataset": names2.raw["url"],
        },
    ],
}
run1 = session.endpoints.containerruns.post(json=runspec1)
run2 = session.endpoints.containerruns.post(json=runspec2)

# Wait for all the runs in this batch to be finished
ACTIVE_STATES = "NSRI"
batchid = containerbatch["id"]
print(f"Waiting for batch {batchid} to finish...")
while True:
    containerbatch = session.get(containerbatch["url"]).json()
    if any(r["state"] in ACTIVE_STATES for r in containerbatch["runs"]):
        time.sleep(1)
        continue
    else:
        print("done.")
        break

# Check outputs and retrieve results
for run in containerbatch["runs"]:
    if run["state"] != "C":
        print(f"Run {run['id']} failed (state: {run['state']})")
        continue
    rundatasets = session.get(run["dataset_list"]).json()
    outputdataseturls = (r["dataset"] for r in rundatasets if r["argument_type"] == "O")
    print(f"Retrieving results from run {run['id']}")
    for url in outputdataseturls:
        dataset = session.get(url).json()
        name = dataset["name"]
        print(f"  downloading {name}")
        with open(name, "wb") as outf:
            session.download_file(outf, dataset["download_url"])

print("done.")
