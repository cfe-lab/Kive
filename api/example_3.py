"Create and use a dataset using an external file."
import io
import pathlib
import pprint

import kiveapi

# Use HTTPS on a real server, so your password is encrypted.
# Don't put your real password in source code, store it in a text file
# that is only readable by your user account or some more secure storage.
session = kiveapi.KiveAPI("http://localhost:8000")
session.login('kive', 'kive')


# Set up an External File to use in an example run.
EFD_DIRECTORY = pathlib.Path("/tmp")
EFD_DIRECTORY_NAME = "tmp"
EFD_NAME = "api_example_external_file.csv"
EFD_CONTENT = "name\nCamus"

with (EFD_DIRECTORY / EFD_NAME).open("w") as outf:
    outf.write(EFD_CONTENT)


# Upload data
try:
    uploaded_dataset = session.add_dataset(
        'API Example 4 External Dataset',
        'None',
        None,
        None,
        None,
        ["Everyone"],
        externalfiledirectory=EFD_DIRECTORY_NAME,
        external_path=EFD_NAME,
    )
except kiveapi.KiveMalformedDataException as e:
    print(e)
    pass


# Now get the file and check that the results make sense.
retrieved_dataset = session.find_datasets(name='API Example 4 External Dataset')[0]

pprint.pprint(retrieved_dataset.__dict__)


assert retrieved_dataset.dataset_id == uploaded_dataset.dataset_id
assert retrieved_dataset.filename == uploaded_dataset.filename
assert retrieved_dataset.name == "API Example 4 External Dataset"
assert retrieved_dataset.users_allowed == []
assert retrieved_dataset.groups_allowed == ["Everyone"]
assert retrieved_dataset.externalfiledirectory == EFD_DIRECTORY_NAME
assert retrieved_dataset.external_path == EFD_NAME

buffer = io.StringIO()
retrieved_dataset.download(buffer)
assert buffer.getvalue() == EFD_CONTENT
