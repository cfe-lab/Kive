#! /usr/bin/env python

import django
import tempfile
from django.conf import settings

from metadata.models import CompoundDatatype, Datatype, BasicConstraint, kive_user
from librarian.models import Dataset
from constants import CDTs, datatypes
import kive.settings

django.setup()

__author__ = 'rliang'


prototype_CDT = CompoundDatatype.objects.get(pk=CDTs.PROTOTYPE_PK)

# A dummy Datatype with a prototype.
with tempfile.TemporaryFile() as f:
    f.write("""example,valid
True,True
true,False
y,False
n,False
False,False
false,false"""
    )
    f.seek(0)
    proto_SD = Dataset.create_dataset(
        file_path=None, user=kive_user(), cdt=CompoundDatatype.objects.get(pk=CDTs.PROTOTYPE_PK),
        name="AlwaysTruePrototype", description="Prototype for dummy Datatype",
        file_handle=f
    )

always_true = Datatype(
    user=kive_user(),
    name="Python True",
    description="True in python",
    proto_SD=proto_SD
)
always_true.save()
always_true.restricts.add(Datatype.objects.get(pk=datatypes.BOOL_PK))

always_true.basic_constraints.create(
    ruletype=BasicConstraint.REGEXP,
    rule="True"
)
