import os
import StringIO

from kiveapi import KiveAPI, KiveMalformedDataException

# Testing creation of Datasets using external files.
KiveAPI.SERVER_URL = 'http://localhost:8000'
kive = KiveAPI()
kive.login('kive', 'kive')  # don't do this in practice, store your password somewhere safe

efd_name = "ExternalFiles"  # make an ExternalFileDirectory with this name
efd_path = ""  # fill this in with your own path
external_path = "external_file.dat"
external_file_contents = "foo"

with open(os.path.join(efd_path, external_path), "wb") as f:
    f.write(external_file_contents)

# Upload data
try:
    ext_ds = kive.add_dataset(
        'ExternalDatasetFile',
        'External copy of 1234A_R1',
        None,
        None,
        None,
        ["Everyone"],
        externalfiledirectory=efd_name,
        external_path=external_path
    )
except KiveMalformedDataException:
    ext_ds = kive.find_datasets(name='ExternalDatasetFile')[0]

# Now get the file and check that the results make sense.
retrieved_ext_ds = kive.get_dataset(ext_ds.dataset_id)

print retrieved_ext_ds.__dict__

assert retrieved_ext_ds.dataset_id == ext_ds.dataset_id
assert retrieved_ext_ds.filename == ext_ds.filename
assert retrieved_ext_ds.name == "ExternalDatasetFile"
assert retrieved_ext_ds.cdt.cdt_id == "__raw__"
assert retrieved_ext_ds.users_allowed == []
assert retrieved_ext_ds.groups_allowed == ["Everyone"]
assert retrieved_ext_ds.externalfiledirectory == efd_name
assert retrieved_ext_ds.external_path == external_path

sio = StringIO.StringIO()
retrieved_ext_ds.download(sio)
assert sio.getvalue() == external_file_contents
sio.close()
