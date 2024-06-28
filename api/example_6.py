"Run the example collation pipeline, which has multi-valued inputs."
import kiveapi
import example_tools

# Use HTTPS on a real server, so your password is encrypted.
# Don't put your real password in source code, store it in a text file
# that is only readable by your user account or some more secure storage.
session = kiveapi.KiveAPI("http://localhost:8000")
session.login('kive', 'kive')

# Get datasets to collate
names_dataset = session.find_datasets(name="example_names.csv")[0]
pairs_dataset = session.find_datasets(name="example_pairs.csv")[0]

# Get the collation app from the samplecode container
collationapp = session.endpoints.containerapps.filter("name", "collation")[0]
appargs = session.get(collationapp["argument_list"]).json()

# Start a run of the app providing the datasets as arguments
inputarg = next(a for a in appargs if a["type"] == "I")
runspec = {
    "name": "API Example 6",
    "app": collationapp["url"],
    "dataset_list": [
        {
            "argument": inputarg["url"],
            "dataset": names_dataset.raw["url"],
            "multi_position": 0,
        },
        {
            "argument": inputarg["url"],
            "dataset": pairs_dataset.raw["url"],
            "multi_position": 1,
        },
    ]
}

print("Starting example run...")
containerrun = session.endpoints.containerruns.post(json=runspec)

# Monitor the run for completion
containerrun = example_tools.await_containerrun(session, containerrun)

# Retrieve the output and save it to a file
run_datasets = session.get(containerrun["dataset_list"]).json()
for run_dataset in run_datasets:
    if run_dataset.get("argument_type") == "O":
        dataset = session.get(run_dataset["dataset"]).json()
        filename = dataset["name"]
        print(f"  downloading {filename}")
        with open(filename, "wb") as outf:
            session.download_file(outf, dataset["download_url"])

print("Example run finished")
