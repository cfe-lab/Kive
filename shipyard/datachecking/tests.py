"""
This file demonstrates writing tests using the unittest module. These will pass
when you run "manage.py test".

Replace this with more appropriate tests for your application.
"""

from django.test import TestCase
from django.contrib.auth.models import User

from datachecking.models import *
from librarian.models import *

import tempfile


class BlankCell(TestCase):

    def setUp(self):
        self.test_CDT = CompoundDatatype()
        self.test_CDT.save()
        self.test_CDT.members.create(datatype=self.INT, column_name="firstcol", column_idx=1)
        self.test_CDT.clean()

        self.user_doug = User.objects.create_user('doug', 'dford@deco.com', 'durrrrr')
        self.user_doug.save()


        with tempfile.TemporaryFile() as f:
            f.write("""firstcol
22
33
17

23
8
""")
            test_SD = SymbolicDataset.create_SD(None, self.test_CDT, make_dataset=True,
                                                user=self.user_doug, name="Test SD",
                                                description="Starting lineup", created_by=None,
                                                check=True, file_handle=f)
            