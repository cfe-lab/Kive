"Run the keyword-names example pipeline, which has keyword-style inputs."
import kiveapi
import example_tools

# Use HTTPS on a real server, so your password is encrypted.
# Don't put your real password in source code, store it in a text file
# that is only readable by your user account or some more secure storage.
session = kiveapi.KiveAPI("http://localhost:8000")
session.login('kive', 'kive')

# Get datasets to collate
names_dataset = session.find_datasets(name="example_names.csv")[0]
salutations_dataset = session.find_datasets(name="salutations.csv")[0]

# Get the collation app from the samplecode container
kwsalutationsapp = session.endpoints.containerapps.filter("name", "kw_salutations")[0]
appargs = session.get(kwsalutationsapp["argument_list"]).json()

# Start a run of the app providing the datasets as arguments
inputargs = {a["name"]: a["url"] for a in appargs if a["type"] == "I"}


runspec = {
    "name": "API Example 5",
    "app": kwsalutationsapp["url"],
    "dataset_list": [
        {
            "argument": inputargs["names"],
            "dataset": names_dataset.raw["url"],
        },
        {
            "argument": inputargs["salutations"],
            "dataset": salutations_dataset.raw["url"],
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
