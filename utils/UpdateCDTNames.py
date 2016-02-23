#! /usr/bin/env python

import django
django.setup()

from metadata.models import CompoundDatatype

[x.set_name() for x in CompoundDatatype.objects.all()]