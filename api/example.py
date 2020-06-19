from pprint import pprint
import time

from kiveapi import KiveAPI

RULE = " " + 66 * "-"
ACTIVE_STATES = "NRLS"


# Use HTTPS on a real server, so your password is encrypted.
KiveAPI.SERVER_URL = 'http://localhost:8080'
# Don't put your real password in source code, store it in a text file
# that is only readable by your user account or some more secure storage.
session = KiveAPI()
session.login('kive', 'kive')

# Get the data by ID
dataset1 = session.get_dataset(1)
dataset2 = session.get_dataset(2)

# or get the data by name
dataset1 = session.find_datasets(name='example_pairs.csv_20200615170532257697')[0]
dataset2 = session.find_datasets(name='example_names.csv_20200615170519866442')[0]


# Get app information
app = session.endpoints.containerapps.get(1)
appargs = session.endpoints.containerapps.get(f"{app['id']}/argument_list")
print("App" + RULE)
pprint(app)
print("App Args" + RULE)
pprint(appargs)


# Start a pipeline run
inputarg = next(a for a in appargs if a['type'] == 'I')
kiveuser = session.endpoints.users.get(1)
dataset = session.endpoints.datasets.get(1)
runspec = {
    "name": "example",

    "app": app["url"],
    "datasets": [
        {
            "argument": inputarg["url"],
            "dataset": dataset["url"],
        },
    ],
}
created_containerrun = session.endpoints.containerruns.post(json=runspec)
print("Created a Run" + RULE)
pprint(created_containerrun)


# Monitor for completion
runid = created_containerrun["id"]
starttime = time.time()
while True:
    containerrun = session.endpoints.containerruns.get(runid)
    state = containerrun["state"]
    if state in ACTIVE_STATES:
        elapsed = round(time.time() - starttime, 2)
        print(f"Run in progress (state={state}, {elapsed}s elapsed))\r", end="")
        time.sleep(4)
    elif state == "C":
        print("Run finished" + RULE)
        pprint(containerrun)
        break
    else:
        print("Run failed" + RULE)
        pprint(containerrun)
        exit(1)


# Retrieve the output and save to a file

# TODO(nknight)
