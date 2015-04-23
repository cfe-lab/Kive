import csv
import os
import random
import shutil
import subprocess
import tempfile
import time

from django.contrib.auth.models import User
from django.core.files import File
from django.db import transaction

from constants import datatypes
import file_access_utils
from librarian.models import SymbolicDataset
from metadata.models import CompoundDatatype, Datatype, everyone_group
from metadata.tests import clean_up_all_files
from method.models import CodeResource, CodeResourceRevision, Method, MethodFamily
from pipeline.models import Pipeline, PipelineFamily
import sandbox.execute


def create_sandbox_testing_tools_environment(case):
    case.STR = Datatype.objects.get(pk=datatypes.STR_PK)

    # An ordinary user.
    case.user_bob = User.objects.create_user('bob', 'bob@talabs.com', 'verysecure')
    case.user_bob.save()
    case.user_bob.groups.add(everyone_group())
    case.user_bob.save()

    # Predefined datatypes.
    case.datatype_str = new_datatype("my_string", "sequences of ASCII characters", case.STR, case.user_bob)
    case.datatype_str.grant_everyone_access()

    # A CDT composed of only one column, strings.
    case.cdt_string = CompoundDatatype(user=case.user_bob)
    case.cdt_string.save()
    case.cdt_string.members.create(datatype=case.datatype_str, column_name="word", column_idx=1)
    case.cdt_string.grant_everyone_access()

    # A code resource which does nothing.
    case.coderev_noop = make_first_revision(
        "noop", "a script to do nothing", "noop.sh",
        '#!/bin/bash\n cat "$1" > "$2"',
        case.user_bob)
    case.coderev_noop.coderesource.grant_everyone_access()
    case.coderev_noop.grant_everyone_access()

    # A Method telling Shipyard how to use the noop code on string data.
    case.method_noop = make_first_method("string noop", "a method to do nothing to strings", case.coderev_noop,
                                         case.user_bob)
    case.method_noop.family.grant_everyone_access()
    case.method_noop.grant_everyone_access()
    simple_method_io(case.method_noop, case.cdt_string, "strings", "same_strings")

    # Another totally different Method that uses the same CodeRevision and yes it does the same thing.
    case.method_trivial = make_first_method(
        "string trivial",
        "a TOTALLY DIFFERENT method that TOTALLY does SOMETHING to strings by leaving them alone",
        case.coderev_noop,
        case.user_bob)
    case.method_trivial.family.grant_everyone_access()
    case.method_trivial.grant_everyone_access()
    simple_method_io(case.method_trivial, case.cdt_string, "strings", "untouched_strings")

    # A third one, only this one takes raw input.
    case.method_noop_raw = make_first_method("raw noop", "do nothing to raw data", case.coderev_noop,
                                             case.user_bob)
    case.method_noop_raw.family.grant_everyone_access()
    case.method_noop_raw.grant_everyone_access()
    simple_method_io(case.method_noop_raw, None, "raw", "same raw")


def destroy_sandbox_testing_tools_environment(case):
    """
    Clean up a TestCase where create_sandbox_testing_tools_environment has been called.
    # """
    clean_up_all_files()


def create_sequence_manipulation_environment(case):
    create_sandbox_testing_tools_environment(case)

    # Alice is a Shipyard user.
    case.user_alice = User.objects.create_user('alice', 'alice@talabs.com', 'secure')
    case.user_alice.save()
    case.user_alice.groups.add(everyone_group())
    case.user_alice.save()

    # Alice's lab has two tasks - complement DNA, and reverse and complement DNA.
    # She wants to create a pipeline for each. In the background, this also creates
    # two new pipeline families.
    case.pipeline_complement = make_first_pipeline("DNA complement", "a pipeline to complement DNA", case.user_alice)
    case.pipeline_reverse = make_first_pipeline("DNA reverse", "a pipeline to reverse DNA", case.user_alice)
    case.pipeline_revcomp = make_first_pipeline("DNA revcomp", "a pipeline to reverse and complement DNA",
                                                case.user_alice)

    # Alice is only going to be manipulating DNA, so she creates a "DNA"
    # data type. A "string" datatype, which she will use for the headers,
    # has been predefined in Shipyard. She also creates a compound "record"
    # datatype for sequence + header.
    case.datatype_dna = new_datatype("DNA", "sequences of ATCG", case.STR, case.user_alice)
    case.cdt_record = CompoundDatatype(user=case.user_alice)
    case.cdt_record.save()
    case.cdt_record.members.create(datatype=case.datatype_str, column_name="header", column_idx=1)
    case.cdt_record.members.create(datatype=case.datatype_dna, column_name="sequence", column_idx=2)
    case.cdt_record.grant_everyone_access()

    # Alice uploads code to perform each of the tasks. In the background,
    # Shipyard creates new CodeResources for these scripts and sets her
    # uploaded files as the first CodeResourceRevisions.
    case.coderev_complement = make_first_revision(
        "DNA complement", "a script to complement DNA",
        "complement.sh",
        """#!/bin/bash
        cat "$1" | cut -d ',' -f 2 | tr 'ATCG' 'TAGC' | paste -d, "$1" - | cut -d ',' -f 1,3 > "$2"
        """,
        case.user_alice)
    case.coderev_reverse = make_first_revision(
        "DNA reverse", "a script to reverse DNA", "reverse.sh",
        """#!/bin/bash
        cat "$1" | cut -d ',' -f 2 | rev | paste -d, "$1" - | cut -d ',' -f 1,3 > "$2"
        """,
        case.user_alice)

    # To tell the system how to use her code, Alice creates two Methods,
    # one for each CodeResource. In the background, this creates two new
    # MethodFamilies with her Methods as the first member of each.
    case.method_complement = make_first_method("DNA complement", "a method to complement strings of DNA",
                                               case.coderev_complement,
                                               case.user_alice)
    simple_method_io(case.method_complement, case.cdt_record, "DNA_to_complement", "complemented_DNA")
    case.method_reverse = make_first_method("DNA reverse", "a method to reverse strings of DNA",
                                            case.coderev_complement,
                                            case.user_alice)
    simple_method_io(case.method_reverse, case.cdt_record, "DNA_to_reverse", "reversed_DNA")

    # Now Alice is ready to define her pipelines. She uses the GUI to drag
    # the "complement" method into the "complement" pipeline, creates
    # the pipeline's input and output, and connects them to the inputs and
    # output of the method.
    create_linear_pipeline(case.pipeline_complement, [case.method_complement], "lab data",
                           "complemented lab data")
    case.pipeline_complement.create_outputs()
    create_linear_pipeline(case.pipeline_reverse, [case.method_reverse], "lab data", "reversed lab data")
    case.pipeline_reverse.create_outputs()
    create_linear_pipeline(case.pipeline_revcomp, [case.method_reverse, case.method_complement], "lab data",
                           "reverse and complemented lab data")
    case.pipeline_revcomp.create_outputs()

    # Here is some data which is sitting on Alice's hard drive.
    case.labdata = "header,sequence\n"
    for i in range(10):
        seq = "".join([random.choice("ATCG") for _ in range(10)])
        case.labdata += "patient{},{}\n".format(i, seq)
    case.datafile = tempfile.NamedTemporaryFile(delete=False)
    case.datafile.write(case.labdata)
    case.datafile.close()

    # Alice uploads the data to the system.
    case.symds_labdata = SymbolicDataset.create_SD(case.datafile.name, user=case.user_alice,
                                                   name="lab data", cdt=case.cdt_record,
                                                   description="data from the lab",
                                                   make_dataset=True)

    # Now Alice is ready to run her pipelines. The system creates a Sandbox
    # where she will run each of her pipelines.
    case.sandbox_complement = sandbox.execute.Sandbox(case.user_alice, case.pipeline_complement, [case.symds_labdata])
    case.sandbox_revcomp = sandbox.execute.Sandbox(case.user_alice, case.pipeline_revcomp, [case.symds_labdata])

    # A second version of the complement Pipeline which doesn't keep any output.
    case.pipeline_complement_v2 = Pipeline(family=case.pipeline_complement.family, revision_name="2",
                                           revision_desc="second version", user=case.user_alice)
    case.pipeline_complement_v2.save()
    create_linear_pipeline(case.pipeline_complement_v2, [case.method_complement], "lab data",
                                "complemented lab data")
    case.pipeline_complement_v2.steps.last().add_deletion(case.method_complement.outputs.first())
    case.pipeline_complement_v2.outcables.first().delete()
    case.pipeline_complement_v2.create_outputs()

    # A second version of the reverse/complement Pipeline which doesn't keep
    # intermediate or final output.
    case.pipeline_revcomp_v2 = Pipeline(family=case.pipeline_revcomp.family, revision_name="2",
                                        revision_desc="second version", user=case.user_alice)
    case.pipeline_revcomp_v2.save()
    create_linear_pipeline(case.pipeline_revcomp_v2, [case.method_reverse, case.method_complement],
                                 "lab data", "revcomped lab data")
    case.pipeline_revcomp_v2.steps.get(step_num=1).add_deletion(case.method_reverse.outputs.first())
    case.pipeline_revcomp_v2.steps.get(step_num=2).add_deletion(case.method_complement.outputs.first())
    case.pipeline_revcomp_v2.outcables.first().delete()
    case.pipeline_revcomp_v2.create_outputs()

    # A third version of the reverse/complement Pipeline which keeps
    # final output, but not intermediate.
    case.pipeline_revcomp_v3 = Pipeline(family=case.pipeline_revcomp.family, revision_name="3",
                                        revision_desc="third version", user=case.user_alice)
    case.pipeline_revcomp_v3.save()
    create_linear_pipeline(case.pipeline_revcomp_v3, [case.method_reverse, case.method_complement],
                                 "lab data", "revcomped lab data")
    case.pipeline_revcomp_v3.steps.get(step_num=1).add_deletion(case.method_reverse.outputs.first())
    case.pipeline_revcomp_v3.create_outputs()

    # Another method which turns DNA into RNA.
    case.coderev_DNA2RNA = make_first_revision("DNA to RNA", "a script to reverse DNA", "DNA2RNA.sh",
            """#!/bin/bash
            cat "$1" | cut -d ',' -f 2 | tr 'T' 'U' | paste -d, "$1" - | cut -d ',' -f 1,3 > "$2"
            """,
            case.user_alice)
    case.method_DNA2RNA = make_first_method("DNA to RNA", "a method to turn strings of DNA into RNA",
                                            case.coderev_DNA2RNA, case.user_alice)
    simple_method_io(case.method_DNA2RNA, case.cdt_record, "DNA_to_convert", "RNA")

    # A pipeline which reverses DNA, then turns it into RNA.
    case.pipeline_revRNA = make_first_pipeline(
        "DNA to reversed RNA",
        "a pipeline to reverse DNA and translate it to RNA",
        case.user_alice)
    create_linear_pipeline(case.pipeline_revRNA, [case.method_reverse, case.method_DNA2RNA], "lab data",
                           "RNA'd lab data")
    case.pipeline_revRNA.create_outputs()

    # Separator to print between Pipeline executions, to make viewing logs easier.
    case.sep = " "*80 + "\n" + "*"*80 + "\n" + " "*80 + "\n"

    # Figure out the MD5 of the output file created when the complement method
    # is run on Alice's data, so we can check it later.
    tmpdir = tempfile.mkdtemp()
    outfile = os.path.join(tmpdir, "output")
    case.method_complement.invoke_code(tmpdir, [case.datafile.name], [outfile])
    time.sleep(1)
    case.labdata_compd_md5 = file_access_utils.compute_md5(open(outfile))
    shutil.rmtree(tmpdir)


def destroy_sequence_manipulation_environment(case):
    clean_up_all_files()
    if os.path.exists(case.datafile.name):
        os.remove(case.datafile.name)


def create_word_reversal_environment(case):
    """
    Create an environment with some word-reversal code and pipelines.
    """
    create_sandbox_testing_tools_environment(case)

    # A code resource which reverses a file.
    case.coderev_reverse = make_first_revision("reverse", "a script to reverse lines of a file", "reverse.py",
        ("#!/usr/bin/env python\n"
         "import sys\n"
         "import csv\n"
         "with open(sys.argv[1]) as infile, open(sys.argv[2], 'w') as outfile:\n"
         "  reader = csv.reader(infile)\n"
         "  writer = csv.writer(outfile)\n"
         "  for row in reader:\n"
         "      writer.writerow([row[1][::-1], row[0][::-1]])\n"),
        case.user_bob)

    # A CDT with two columns, word and drow.
    case.cdt_wordbacks = CompoundDatatype(user=case.user_bob)
    case.cdt_wordbacks.save()
    case.cdt_wordbacks.members.create(datatype=case.datatype_str, column_name="word", column_idx=1)
    case.cdt_wordbacks.members.create(datatype=case.datatype_str, column_name="drow", column_idx=2)
    case.cdt_wordbacks.grant_everyone_access()

    # A second CDT, much like the first :]
    case.cdt_backwords = CompoundDatatype(user=case.user_bob)
    case.cdt_backwords.save()
    case.cdt_backwords.members.create(datatype=case.datatype_str, column_name="drow", column_idx=1)
    case.cdt_backwords.members.create(datatype=case.datatype_str, column_name="word", column_idx=2)
    case.cdt_backwords.grant_everyone_access()

    # Methods for the reverse CRR, and noop CRR.
    case.method_reverse = make_first_method("string reverse", "a method to reverse strings",
                                            case.coderev_reverse, case.user_bob)
    simple_method_io(case.method_reverse, case.cdt_wordbacks, "words_to_reverse", "reversed_words")
    case.method_re_reverse = make_first_method("string re-reverse", "a method to re-reverse strings",
                                               case.coderev_reverse, case.user_bob)
    simple_method_io(case.method_re_reverse, case.cdt_backwords, "words_to_rereverse", "rereversed_words")

    case.method_noop_wordbacks = make_first_method(
        "noop wordback",
        "a method to do nothing on two columns (word, drow)",
        case.coderev_noop,
        case.user_bob)
    simple_method_io(case.method_noop_wordbacks, case.cdt_wordbacks, "words", "more_words")
    case.method_noop_backwords = make_first_method(
        "noop backword",
        "a method to do nothing on two columns",
        case.coderev_noop,
        case.user_bob)
    simple_method_io(case.method_noop_backwords, case.cdt_backwords, "backwords", "more_backwords")

    # Some data of type (case.datatype_str: word).
    string_datafile = tempfile.NamedTemporaryFile(delete=False)
    string_datafile.write("word\n")
    string_datafile.close()
    os.system("head -1 /usr/share/dict/words >> {}".
              format(string_datafile.name))
    case.symds_words = SymbolicDataset.create_SD(
        string_datafile.name,
        name="blahblah", cdt=case.cdt_string, user=case.user_bob,
        description="blahblahblah", make_dataset=True,
        groups_allowed=[everyone_group()])

    os.remove(string_datafile.name)

    # Some data of type (case.datatype_str: word, case.datatype_str: drow).
    case.wordbacks_datafile = tempfile.NamedTemporaryFile(delete=False)
    writer = csv.writer(case.wordbacks_datafile)
    writer.writerow(["word", "drow"])
    random.seed("Constant seed avoids intermittent failures.")
    for _ in range(20):
        i = random.randint(1,99171)
        sed = subprocess.Popen(["sed", "{}q;d".format(i), "/usr/share/dict/words"],
                               stdout=subprocess.PIPE)
        word, _ = sed.communicate()
        word = word.strip()
        writer.writerow([word, word[::-1]])
    case.wordbacks_datafile.close()

    case.backwords_datafile = tempfile.NamedTemporaryFile(delete=False)
    writer = csv.writer(case.backwords_datafile)
    writer.writerow(["drow", "word"])
    for _ in range(20):
        i = random.randint(1,99171)
        sed = subprocess.Popen(["sed", "{}q;d".format(i), "/usr/share/dict/words"],
                               stdout=subprocess.PIPE)
        word, _ = sed.communicate()
        word = word.strip()
        writer.writerow([word[::-1], word])
    case.backwords_datafile.close()

    case.symds_wordbacks = SymbolicDataset.create_SD(
        case.wordbacks_datafile.name, user=case.user_bob,
        name="wordbacks", cdt=case.cdt_wordbacks,
        description="random reversed words", make_dataset=True,
        groups_allowed=[everyone_group()])

    case.symds_backwords = SymbolicDataset.create_SD(
        case.backwords_datafile.name, user=case.user_bob,
        name="backwords", cdt=case.cdt_backwords,
        description="random reversed words", make_dataset=True,
        groups_allowed=[everyone_group()])


def destroy_word_reversal_environment(case):
    clean_up_all_files()
    if hasattr(case, "words_datafile"):
        os.remove(case.words_datafile.name)


def make_crisscross_cable(cable):
    """
    Helper to take a cable whose source and destination CDTs both have two columns that can be
    reversed (e.g. string-string or int-int, etc.) and add "crisscross" wiring.
    """
    source_cdt = cable.source.structure.compounddatatype
    dest_cdt = cable.dest.structure.compounddatatype
    cable.custom_wires.create(source_pin=source_cdt.members.get(column_idx=1),
                              dest_pin=dest_cdt.members.get(column_idx=2))
    cable.custom_wires.create(source_pin=source_cdt.members.get(column_idx=2),
                              dest_pin=dest_cdt.members.get(column_idx=1))


def new_datatype(dtname, dtdesc, kivetype, user, grant_everyone_access=True):
    """
    Helper function to create a new datatype.
    """
    datatype = Datatype(name=dtname, description=dtdesc, user=user)
    datatype.save()
    datatype.restricts.add(Datatype.objects.get(pk=kivetype.pk))
    if grant_everyone_access:
        datatype.grant_everyone_access()
    # datatype.complete_clean()
    return datatype


def make_first_revision(resname, resdesc, resfn, contents, user, grant_everyone_access=True):
    """
    Helper function to make a CodeResource and the first version.
    """
    resource = CodeResource(name=resname, description=resdesc, filename=resfn, user=user)
    # resource.clean()
    resource.save()
    if grant_everyone_access:
        resource.grant_everyone_access()
    with tempfile.TemporaryFile() as f:
        f.write(contents)
        with transaction.atomic():
            revision = CodeResourceRevision(
                coderesource=resource,
                revision_name="1",
                revision_desc="first version",
                content_file=File(f),
                user=user)
            revision.save()
            revision.clean()
    if grant_everyone_access:
        revision.grant_everyone_access()
    resource.clean()
    return revision


def make_first_method(famname, famdesc, driver, user, grant_everyone_access=True):
    """
    Helper function to make a new MethodFamily for a new Method.
    """
    family = MethodFamily(name=famname, description=famdesc, user=user)
    family.save()
    if grant_everyone_access:
        family.grant_everyone_access()
    with transaction.atomic():
        method = Method(
            revision_name="v1",
            revision_desc="first version",
            family=family,
            driver=driver,
            user=user)
        method.save()
        method.clean()
    if grant_everyone_access:
        method.grant_everyone_access()
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


def make_first_pipeline(pname, pdesc, user, grant_everyone_access=True):
    """
    Helper function to make a new PipelineFamily and the first Pipeline
    member.
    """
    family = PipelineFamily(name=pname, description=pdesc, user=user)
    family.save()
    if grant_everyone_access:
        family.grant_everyone_access()
    pipeline = Pipeline(family=family, revision_name="v1", revision_desc="first version", user=user)
    pipeline.clean()
    pipeline.save()
    if grant_everyone_access:
        pipeline.grant_everyone_access()
    family.clean()
    return pipeline


def make_second_pipeline(pipeline, grant_everyone_access=True):
    """
    Create a second version of a Pipeline, in the same family as the first,
    without making any changes. Hook up the steps to each other, but don't
    create inputs and outputs for the new Pipeline.
    """
    new_pipeline = Pipeline(family=pipeline.family, revision_name="v2", revision_desc="second version",
                            user=pipeline.user)
    new_pipeline.save()
    if grant_everyone_access:
        new_pipeline.grant_everyone_access()

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
    pipeline_in = pipeline.create_input(compounddatatype=cdt_in, dataset_name=indata, dataset_idx=1)

    # Create steps.
    steps = []
    for i, _method in enumerate(methods):
        step = pipeline.steps.create(transformation=methods[i], step_num=i+1)
        if i == 0:
            source = pipeline_in
        else:
            source = methods[i-1].outputs.first()
        step.cables_in.create(source_step=i, source=source, dest=methods[i].inputs.first())
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
    case.symds_words.grant_everyone_access()
    case.symds_words.save()

    os.remove(string_datafile.name)


# An environment resulting from a user that's messed things up.
def create_grandpa_sandbox_environment(case):
    create_sandbox_testing_tools_environment(case)

    # A guy who doesn't know what he is doing.
    # May 14, 2014: dag, yo -- RL
    # May 20, 2014: he's doing his best, man -- RL
    case.user_grandpa = User.objects.create_user('grandpa', 'gr@nd.pa', '123456')
    case.user_grandpa.save()
    case.user_grandpa.groups.add(everyone_group())
    case.user_grandpa.save()

    # A code resource, method, and pipeline which are empty.
    case.coderev_faulty = make_first_revision(
        "faulty",
        "a script...?",
        "faulty.sh", "",
        case.user_grandpa
    )
    case.method_faulty = make_first_method(
        "faulty",
        "a method to... uh...",
        case.coderev_faulty,
        case.user_grandpa
    )
    case.method_faulty.clean()
    simple_method_io(case.method_faulty, case.cdt_string, "strings", "i don't know")
    case.pipeline_faulty = make_first_pipeline("faulty pipeline", "a pipeline to do nothing", case.user_grandpa)
    create_linear_pipeline(case.pipeline_faulty, [case.method_faulty, case.method_noop], "data", "the abyss")
    case.pipeline_faulty.create_outputs()

    # A code resource, method, and pipeline which fail.
    case.coderev_fubar = make_first_revision(
        "fubar", "a script which always fails",
        "fubar.sh", "#!/bin/bash\nexit 1",
        case.user_grandpa
    )
    case.method_fubar = make_first_method("fubar", "a method which always fails", case.coderev_fubar,
                                          case.user_grandpa)
    case.method_fubar.clean()
    simple_method_io(case.method_fubar, case.cdt_string, "strings", "broken strings")
    case.pipeline_fubar = make_first_pipeline("fubar pipeline", "a pipeline which always fails", case.user_grandpa)
    create_linear_pipeline(case.pipeline_fubar,
                           [case.method_noop, case.method_fubar, case.method_noop], "indata", "outdata")
    case.pipeline_fubar.create_outputs()

    # Some data to run through the faulty pipelines.
    case.grandpa_datafile = tempfile.NamedTemporaryFile(delete=False)
    case.grandpa_datafile.write("word\n")
    random.seed("Constant seed avoids intermittent failures.")
    for _ in range(20):
        i = random.randint(1,99171)
        case.grandpa_datafile.write("{}\n".format(i))
    case.grandpa_datafile.close()
    case.symds_grandpa = SymbolicDataset.create_SD(
        case.grandpa_datafile.name, user=case.user_grandpa,
        name="numbers", cdt=case.cdt_string,
        description="numbers which are actually strings", make_dataset=True)
    case.symds_grandpa.clean()


def destroy_grandpa_sandbox_environment(case):
    clean_up_all_files()
    os.remove(case.grandpa_datafile.name)


def make_SD(contents, CDT, make_dataset, user, name, description, created_by, check):
    """
    Wrapper for create_SD that creates a SymbolicDataset from a string.
    """
    with tempfile.TemporaryFile() as f:
        f.write(contents)
        test_SD = SymbolicDataset.create_SD(None, user, cdt=CDT, make_dataset=make_dataset,
                                            name=name, description=description, created_by=created_by,
                                            check=check, file_handle=f)

    return test_SD
