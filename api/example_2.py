"""Upload (or find, if it's already been uploaded) a dataset and use it
with an app from a container family.
"""
import example_tools
from kiveapi import KiveAPI, KiveMalformedDataException

# Use HTTPS on a real server, so your password is encrypted.
# Don't put your real password in source code, store it in a text file
# that is only readable by your user account or some more secure storage.
kive = KiveAPI('http://localhost:8000')
kive.login('kive', 'kive')

# Upload (or retrieve) an input file
dataset = example_tools.upload_or_retrieve_dataset(kive,
                                                   "API Example 2 Names File",
                                                   open("names.csv", "r"),
                                                   groups=["Everyone"])

# Get the app from a container family.
containerfamily = kive.filter("/api/containerfamilies/", "name",
                              "samplecode").json()[0]
container = kive.get(containerfamily["containers"]).json()[0]
app = kive.filter(
    container["app_list"], "smart",
    "Minimal example that can run simple Python scripts").json()[0]

# Create a run of this app using the file we uploaded
appargs = kive.get(app["argument_list"]).json()
inputarg = next(a for a in appargs if a["type"] == "I")

runspec = {
    "name": "uploaded-file-example",
    "app": app["url"],
    "dataset_list": [{
        "argument": inputarg["url"],
        "dataset": dataset.raw["url"],
    }],
}

print("Starting uploaded file example...", end="", flush=True)
containerrun = kive.endpoints.containerruns.post(json=runspec)

# Wait for the pipeline to finish
containerrun = example_tools.await_containerrun(kive, containerrun)

# Retrieve files
run_datasets = kive.get(containerrun["dataset_list"]).json()
for run_dataset in run_datasets:
    if run_dataset.get("argument_type") == "O":
        dataset = kive.get(run_dataset["dataset"]).json()
        filename = dataset["name"]
        print(f"  downloading {filename}")
        with open(filename, "wb") as outf:
            kive.download_file(outf, dataset["download_url"])
