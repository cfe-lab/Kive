import os
import sys
import tempfile
import shutil
import random
import logging
import csv
import time
import re
from subprocess import Popen, PIPE

from django.core.files import File
from django.contrib.auth.models import User
from django.test import TestCase
from django.utils import timezone

from archive.models import *
from librarian.models import *
from metadata.models import *
from metadata.tests import clean_up_all_files
from method.models import *
from pipeline.models import *
from datachecking.models import *
from sandbox.execute import Sandbox

import file_access_utils

from constants import datatypes, CDTs


def create_sandbox_testing_tools_environment(case):
    case.STR = Datatype.objects.get(pk=datatypes.STR_PK)

    # Predefined datatypes.
    case.datatype_str = new_datatype("my_string", "sequences of ASCII characters", case.STR)

    # A CDT composed of only one column, strings.
    case.cdt_string = CompoundDatatype()
    case.cdt_string.save()
    case.cdt_string.members.create(datatype=case.datatype_str, column_name="word", column_idx=1)

    # A code resource which does nothing.
    case.coderev_noop = make_first_revision("noop", "a script to do nothing", "noop.sh",
            '#!/bin/bash\n cat "$1" > "$2"')

    # A Method telling Shipyard how to use the noop code on string data.
    case.method_noop = make_first_method("string noop", "a method to do nothing to strings", case.coderev_noop)
    simple_method_io(case.method_noop, case.cdt_string, "strings", "same_strings")

    # Another totally different Method that uses the same CodeRevision and yes it does the same thing.
    case.method_trivial = make_first_method(
        "string trivial",
        "a TOTALLY DIFFERENT method that TOTALLY does SOMETHING to strings by leaving them alone",
        case.coderev_noop)
    simple_method_io(case.method_trivial, case.cdt_string, "strings", "untouched_strings")

    # A third one, only this one takes raw input.
    case.method_noop_raw = make_first_method("raw noop", "do nothing to raw data", case.coderev_noop)
    simple_method_io(case.method_noop_raw, None, "raw", "same raw")

    # An ordinary user.
    case.user_bob = User.objects.create_user('bob', 'bob@talabs.com', 'verysecure')
    case.user_bob.save()


def destroy_sandbox_testing_tools_environment(case):
    """
    Clean up a TestCase where create_sandbox_testing_tools_environment has been called.
    # """
    clean_up_all_files()


def new_datatype(dtname, dtdesc, shipyardtype):
    """
    Helper function to create a new datatype.
    """
    datatype = Datatype(name=dtname, description=dtdesc)
    datatype.save()
    datatype.restricts.add(Datatype.objects.get(pk=shipyardtype.pk))
    datatype.complete_clean()
    return datatype


def make_first_revision(resname, resdesc, resfn, contents):
    """
    Helper function to make a CodeResource and the first version.
    """
    resource = CodeResource(name=resname, description=resdesc,
        filename=resfn)
    resource.clean()
    resource.save()
    with tempfile.TemporaryFile() as f:
        f.write(contents)
        revision = CodeResourceRevision(
            coderesource=resource,
            revision_name="1",
            revision_desc="first version",
            content_file=File(f))
        revision.clean()
        revision.save()
    resource.clean()
    return revision


def make_first_method(famname, famdesc, driver):
    """
    Helper function to make a new MethodFamily for a new Method.
    """
    family = MethodFamily(name=famname, description=famdesc)
    family.clean()
    family.save()
    method = Method(
        revision_name="v1",
        revision_desc="first version",
        family=family,
        driver=driver)
    method.clean()
    method.save()
    family.clean()
    return method


def simple_method_io(method, cdt, indataname, outdataname):
    """
    Helper function to create inputs and outputs for a simple
    Method with one input, one output, and the same CompoundDatatype
    for both incoming and outgoing data.
    """
    minput = method.create_input(compounddatatype=cdt,
        dataset_name = indataname,
        dataset_idx = 1)
    minput.clean()
    moutput = method.create_output(compounddatatype=cdt,
        dataset_name = outdataname,
        dataset_idx = 1)
    moutput.clean()
    method.clean()
    return minput, moutput


def make_first_pipeline(pname, pdesc):
    """
    Helper function to make a new PipelineFamily and the first Pipeline
    member.
    """
    family = PipelineFamily(name=pname, description=pdesc)
    family.clean()
    family.save()
    pipeline = Pipeline(family=family, revision_name="v1", revision_desc="first version")
    pipeline.clean()
    pipeline.save()
    family.clean()
    return pipeline


def make_second_pipeline(pipeline):
    """
    Create a second version of a Pipeline, in the same family as the first,
    without making any changes. Hook up the steps to each other, but don't
    create inputs and outputs for the new Pipeline.
    """
    new_pipeline = Pipeline(family=pipeline.family, revision_name="v2", revision_desc="second version")
    new_pipeline.save()

    for step in pipeline.steps.all():
        new_step = new_pipeline.steps.create(transformation=step.transformation, step_num=step.step_num)
        for cable in step.cables_in.all():
            if cable.source.transformation.__class__.__name__ == "PipelineStep":
                new_step.cables_in.create(source = cable.source, dest = cable.dest)
    return new_pipeline


def create_linear_pipeline(pipeline, methods, indata, outdata):
    """
    Helper function to create a "linear" pipeline, ie.

            ___       __
      in --|   |-...-|  |-- out
           |___|     |__|

    indata and outdata are the names of the input and output datasets.
    """
    # Create pipeline input.
    if methods[0].inputs.first().is_raw():
        cdt_in = None
    else:
        cdt_in = methods[0].inputs.first().structure.compounddatatype
    pipeline_in = pipeline.create_input(compounddatatype=cdt_in, dataset_name = indata, dataset_idx = 1)

    # Create steps.
    steps = []
    for i, method in enumerate(methods):
        step = pipeline.steps.create(transformation=methods[i], step_num=i+1)
        if i == 0:
            source = pipeline_in
        else:
            source = methods[i-1].outputs.first()
        step.cables_in.create(source_step = i, source = source, dest = methods[i].inputs.first())
        step.complete_clean()
        steps.append(step)

    # Create pipeline output.
    pipeline.create_outcable(output_name=outdata, output_idx=1, source_step=len(steps),
                             source=methods[-1].outputs.first())
    pipeline.create_outputs()
    pipeline.complete_clean()


# This is potentially slow so we don't just build it into the create_... function above.
# This is also kind of a hack -- depends on case.user_bob and case.cdt_string being present.
def make_words_symDS(case):
    """
    Set up a data file of words in the specified test case.

    PRE: the specified test case has a member CDT called cdt_string and user user_bob.
    """
    string_datafile = tempfile.NamedTemporaryFile(delete=False)
    string_datafile.write("word\n")
    string_datafile.close()
    os.system("head -1 /usr/share/dict/words >> {}".
              format(string_datafile.name))
    case.symds_words = SymbolicDataset.create_SD(string_datafile.name,
        name="blahblah", cdt=case.cdt_string, user=case.user_bob,
        description="blahblahblah", make_dataset=True)

    os.remove(string_datafile.name)
