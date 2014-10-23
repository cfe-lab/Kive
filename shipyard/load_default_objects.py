"""
Load default CodeResources and Methods.
Should run nukeDB.bash first.
"""
import os
from django.core.files import File
from method.models import CodeResource, CodeResourceRevision, MethodFamily
from metadata.models import CompoundDatatype, AccessControl
from django.contrib.auth.models import User

shipyard_user = User.objects.get(pk=1)

try:
    cdt = CompoundDatatype.objects.get(pk=4)  # from initial_data.json
except:
    print 'Initial Datatypes not loaded - run nukeDB.bash'
    raise

samplecode_path = '../samplecode'

# load fasta2csv.py as a CodeResource
cr = CodeResource(name='fasta2csv', description='Convert FASTA to CSV', filename='fasta2csv.py',
                  user=shipyard_user)
cr.save()

with open(os.path.join(samplecode_path, 'fasta2csv.py'), 'rb') as f:
    crv = CodeResourceRevision(coderesource=cr,
                               revision_name='Prototype',
                               revision_number=1,
                               revision_desc='First version of fasta2csv.py',
                               content_file=File(f),
                               user=shipyard_user)
    crv.full_clean()
    crv.save()

# create MethodFamily
mf = MethodFamily(name='Adapters', description='Scripts to convert raw data into CSV',
                  user=shipyard_user)
mf.full_clean()
mf.save()

# create first Method in this family
m = mf.members.create(revision_number=1,
                      revision_name='fasta2csv version 1',
                      revision_desc='Converts FASTA to CSV; first version.',
                      user=shipyard_user,
                      driver=crv)

in1 = m.create_input(compounddatatype=None,
                     dataset_name='FASTA',
                     dataset_idx=1)
in1.full_clean()
in1.save()


out1 = m.create_output(compounddatatype=cdt,
                       dataset_name='CSV',
                       dataset_idx=1)
out1.full_clean()
out1.save()
