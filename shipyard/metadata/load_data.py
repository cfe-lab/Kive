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
"""
from metadata.models import *

STR_DT = Datatype(id=1, name="string", description="basic string type",
                  Python_type=Datatype.STR)
STR_DT.clean()
STR_DT.save()

BOOL_DT = Datatype(id=2, name="boolean", description="basic boolean type",
                   Python_type=Datatype.BOOL)
BOOL_DT.clean()
BOOL_DT.save()
BOOL_DT.restricts.add(STR_DT)
BOOL_DT.clean()

FLOAT_DT = Datatype(id=3, name="float", description="basic float type",
                    Python_type=Datatype.FLOAT)
FLOAT_DT.clean()
FLOAT_DT.save()
FLOAT_DT.restricts.add(STR_DT)
FLOAT_DT.clean()

INT_DT = Datatype(id=4, name="integer", description="basic integer type",
                  Python_type=Datatype.INT)
INT_DT.clean()
INT_DT.save()
# We don't need to add STR_DT as a restricted type because FLOAT_DT
# already restricts it.
INT_DT.restricts.add(FLOAT_DT)
INT_DT.clean()

NaturalNumber_DT = Datatype(
    id=5,
    name="natural number",
    description="positive integer",
    Python_type=Datatype.INT)
NaturalNumber_DT.clean()
NaturalNumber_DT.save()
NaturalNumber_DT.restricts.add(INT_DT)
NaturalNumber_DT.basic_constraints.create(ruletype=BasicConstraint.MIN_VAL,
                                          rule="1")
NaturalNumber_DT.clean()

verif_in = CompoundDatatype(id=1)
verif_in.save()
verif_in.members.create(datatype=STR_DT, column_name="to_test",
                        column_idx=1)
verif_in.clean()

verif_out = CompoundDatatype(id=2)
verif_out.save()
verif_out.members.create(datatype=NaturalNumber_DT, column_name="failed_row",
                         column_idx=1)
verif_out.clean()

prototype_cdt = CompoundDatatype(id=3)
prototype_cdt.save()
prototype_cdt.members.create(datatype=STR_DT, column_name="example",
                             column_idx=1)
prototype_cdt.members.create(datatype=BOOL_DT, column_name="valid",
                             column_idx=2)
