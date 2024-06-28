"Run the example Scraper pipeline, demonstrating directory outputs."

import kiveapi
import example_tools

# Use HTTPS on a real server, so your password is encrypted.
# Don't put your real password in source code, store it in a text file
# that is only readable by your user account or some more secure storage.
session = kiveapi.KiveAPI("http://localhost:8000")
session.login('kive', 'kive')

# Upload (or retrieve) input dataset.
input_dataset = example_tools.upload_or_retrieve_dataset(
    session,
    "API Example 7 Scraper URLs",
    open("example_urls.csv"),
    None,
    ["Everyone"],
)

# Get the app
containerfamily = session.endpoints.containerfamilies.filter(
    "name", "samplecode")[0]
container = session.get(containerfamily["containers"]).json()[0]
containerapps = session.get(container["app_list"]).json()
# Retrieve the scraper app for the samplecode container.
app = next(c for c in containerapps if c["name"] == "scraper")
appargs = session.get(app["argument_list"]).json()
inputarg = next(a for a in appargs if a["type"] == "I")

runspec = {
    "name": "directory-output-example",
    "app": app["url"],
    "dataset_list": [{
        "argument": inputarg["url"],
        "dataset": input_dataset.raw["url"],
    }],
    "groups_allowed": ["Everyone"],
}

print("Starting directory output container run...")
containerrun = session.endpoints.containerruns.post(json=runspec)

# Monitor for completion
containerrun = example_tools.await_containerrun(session, containerrun)


# Retrieve and download output
run_datasets = session.get(containerrun["dataset_list"]).json()
for run_dataset in run_datasets:
    if run_dataset.get("argument_type") == "O":
        dataset = session.get(run_dataset["dataset"]).json()
        filename = dataset["name"]
        print(f"  downloading {filename}")
        with open(filename, "wb") as outf:
            session.download_file(outf, dataset["download_url"])

print("Example run finished")
