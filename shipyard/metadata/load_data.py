"""
Quick driver that we will use to pre-load default DTs and CDTs.

The DTs we create are:
 - string
 - boolean
 - integer
 - float
 - NaturalNumber

The CDTs we create are
 - verif_in: (string to_test)
 - verif_out: (NaturalNumber failed_row)
 - prototype_cdt: (string example, bool valid)

To use this script to recreate a fixture, execfile() it from the Django shell,
and then use ./manage.py dumpdata.  Note that as of February 2014,
the fixture file has been customized by Art and is no longer the file
that is directly generated from this.
"""
from metadata.models import Datatype, CompoundDatatype

STR_DT = Datatype(id=1, name="string", description="basic string type")
STR_DT.clean()
STR_DT.save()

BOOL_DT = Datatype(id=2, name="boolean", description="basic boolean type")
BOOL_DT.clean()
BOOL_DT.save()
BOOL_DT.restricts.add(STR_DT)
BOOL_DT.clean()

FLOAT_DT = Datatype(id=3, name="float", description="basic float type")
FLOAT_DT.clean()
FLOAT_DT.save()
FLOAT_DT.restricts.add(STR_DT)
FLOAT_DT.clean()

INT_DT = Datatype(id=4, name="integer", description="basic integer type")
INT_DT.clean()
INT_DT.save()
# We don't need to add STR_DT as a restricted type because FLOAT_DT
# already restricts it.
INT_DT.restricts.add(FLOAT_DT)
INT_DT.clean()

NaturalNumber_DT = Datatype(id=5, name="natural number", description="positive integer")
NaturalNumber_DT.clean()
NaturalNumber_DT.save()
NaturalNumber_DT.restricts.add(INT_DT)
NaturalNumber_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL, rule="1")
NaturalNumber_DT.clean()

verif_in = CompoundDatatype(id=1)
verif_in.save()
verif_in.members.create(datatype=STR_DT, column_name="to_test", column_idx=1)
verif_in.clean()

verif_out = CompoundDatatype(id=2)
verif_out.save()
verif_out.members.create(datatype=NaturalNumber_DT, column_name="failed_row", column_idx=1)
verif_out.clean()

prototype_cdt = CompoundDatatype(id=3)
prototype_cdt.save()
prototype_cdt.members.create(datatype=STR_DT, column_name="example", column_idx=1)
prototype_cdt.members.create(datatype=BOOL_DT, column_name="valid", column_idx=2)
