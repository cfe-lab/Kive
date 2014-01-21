"""
copperfish.models

Data model for the Shipyard (Copperfish) project - open source
software that performs revision control on datasets and bioinformatic
pipelines.
"""

from django.db import models
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes import generic
from django.core.exceptions import ValidationError, ObjectDoesNotExist
from django.core.validators import MinValueValidator
from django.db import transaction

# Python math functions
import operator
# To calculate MD5 hash
import hashlib
# Regular expressions
import re
# Augments regular expressions
import string
# For checking file paths
import os.path
import os
import sys
import csv
import glob
import subprocess
import stat
import file_access_utils
import datetime


