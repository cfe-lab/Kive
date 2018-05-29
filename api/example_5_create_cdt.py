from kiveapi import KiveAPI

# Testing creation of Datasets using external files.
KiveAPI.SERVER_URL = 'http://localhost:8000'
kive = KiveAPI()
kive.login('kive', 'kive')  # don't do this in practice, store your password somewhere safe

# Kive internal Datatype primary keys:
str_pk = 1
bool_pk = 2
float_pk = 3
int_pk = 4
natural_number_pk = 5

# Define a new CompoundDatatype.
cdt = kive.create_cdt(
    "CDTCreatedByPythonAPI",
    users=["kive"],
    groups=["Everyone"],
    members=[
        {
            "column_idx": 1,
            "column_name": "col1_str",
            "datatype": str_pk
        },
        {
            "column_idx": 2,
            "column_name": "col2_bool",
            "datatype": bool_pk
        },
        {
            "column_idx": 3,
            "column_name": "col3_float",
            "datatype": float_pk
        },
        {
            "column_idx": 4,
            "column_name": "col4_int",
            "datatype": int_pk
        },
        {
            "column_idx": 5,
            "column_name": "col5_natural_number",
            "datatype": natural_number_pk
        }
    ]
)

print(cdt)
