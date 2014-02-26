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
from method.models import *
from pipeline.models import *
from datachecking.models import *
from sandbox.execute import Sandbox

import file_access_utils

from constants import *

def rmf(path):
    try:
        os.remove(path)
    except OSError:
        pass

def clean_files():
    for dataset in Dataset.objects.all():
        rmf(dataset.dataset_file.name)
    for crr in CodeResourceRevision.objects.all():
        rmf(crr.content_file.name)
    for cls in [MethodOutput, VerificationLog]:
        for output in cls.objects.all():
            rmf(output.output_log.name)
            rmf(output.error_log.name)

class UtilityMethods(TestCase):

    def setUp(self):

        # Predefined datatypes.
        self.datatype_str = self.new_datatype("string", 
            "sequences of ASCII characters", Datatype.STR)

        # A CDT composed of only one column, strings.
        self.cdt_string = CompoundDatatype()
        self.cdt_string.save()
        self.cdt_string.members.create(datatype=self.datatype_str, 
            column_name="word", column_idx=1)

        # A code resource which does nothing.
        self.coderev_noop = self.make_first_revision("noop",
                "a script to do nothing", "noop.sh",
                '#!/bin/bash\n cat "$1" > "$2"')

        # A Method telling Shipyard how to use the noop code on string data.
        self.method_noop = self.make_first_method("string noop",
                "a method to do nothing to strings",
                self.coderev_noop)
        self.simple_method_io(self.method_noop, self.cdt_string,
                "strings", "same_strings")

    def tearDown(self):
        clean_files()

    def make_second_pipeline(self, pipeline):
        """
        Create a second version of a Pipeline, in the same family as the first,
        without making any changes. Hook up the steps to each other, but don't
        create inputs and outputs for the new Pipeline.
        """
        new_pipeline = Pipeline(family=pipeline.family, revision_name="2", 
            revision_desc="second version")
        new_pipeline.save()

        for step in pipeline.steps.all():
            new_step = new_pipeline.steps.create(transformation=step.transformation, step_num=step.step_num)
            for cable in step.cables_in.all():
                if cable.source.transformation.__class__.__name__ == "PipelineStep":
                    new_step.cables_in.create(source = cable.source,
                        dest = cable.dest)
        return new_pipeline

    def create_linear_pipeline(self, pipeline, methods, indata, outdata):
        """
        Helper function to create a "linear" pipeline, ie.

                ___       __
          in --|   |-...-|  |-- out
               |___|     |__|

        indata and outdata are the names of the input and output datasets.
        """
        # Create pipeline input.
        cdt_in = methods[0].inputs.first().structure.first().compounddatatype
        pipeline_in = pipeline.create_input(compounddatatype=cdt_in,
            dataset_name = indata, dataset_idx = 1)

        # Create steps.
        steps = []
        for i, method in enumerate(methods):
            step = pipeline.steps.create(transformation=methods[i], step_num=i+1)
            if i == 0:
                source = pipeline_in
            else:
                source = methods[i-1].outputs.first()
            step.cables_in.create(source_step = i, 
                source = source,
                dest = methods[i].inputs.first())
            step.complete_clean()
            steps.append(step)

        # Create pipeline output.
        pipeline.create_outcable(output_name = outdata,
            output_idx = 1,
            source_step = len(steps),
            source = methods[-1].outputs.first())

        pipeline.complete_clean()

    def simple_method_io(self, method, cdt, indataname, outdataname):
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

    def new_datatype(self, dtname, dtdesc, pytype):
        """
        Helper function to create a new datatype.
        """
        datatype = Datatype(name=dtname, description=dtdesc)
        datatype.save()
        datatype.restricts.add(Datatype.objects.get(pk=datatypes.STR_PK))
        datatype.complete_clean()
        return datatype

    def make_first_revision(self, resname, resdesc, resfn, contents):
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

    def make_first_method(self, famname, famdesc, driver):
        """
        Helper function to make a new MethodFamily for a new Method.
        """
        family = MethodFamily(name=famname, description=famdesc)
        family.clean()
        family.save()
        method = Method(revision_name="1",
            revision_desc="first version",
            family=family,
            driver=driver)
        method.clean()
        method.save()
        family.clean()
        return method

    def make_first_pipeline(self, pname, pdesc):
        """
        Helper function to make a new PipelineFamily and the first Pipeline
        member.  
        """
        family = PipelineFamily(name=pname, description=pdesc)
        family.clean()
        family.save()
        pipeline = Pipeline(family=family, revision_name="1", 
            revision_desc="first version")
        pipeline.complete_clean()
        pipeline.save()
        family.clean()
        return pipeline

class ExecuteTestsRM(UtilityMethods):

    def setUp(self):
        super(ExecuteTestsRM, self).setUp()

        # Alice is a Shipyard user.
        self.user_alice = User.objects.create_user('alice', 'alice@talabs.com', 'secure')
        self.user_alice.save()

        # Alice's lab has two tasks - complement DNA, and reverse and complement DNA.
        # She wants to create a pipeline for each. In the background, this also creates
        # two new pipeline families.
        self.pipeline_complement = self.make_first_pipeline("DNA complement",
            "a pipeline to complement DNA")
        self.pipeline_reverse = self.make_first_pipeline("DNA reverse",
            "a pipeline to reverse DNA")
        self.pipeline_revcomp = self.make_first_pipeline("DNA revcomp",
                "a pipeline to reverse and complement DNA")

        # Alice is only going to be manipulating DNA, so she creates a "DNA"
        # data type. A "string" datatype, which she will use for the headers,
        # has been predefined in Shipyard. She also creates a compound "record"
        # datatype for sequence + header.
        self.datatype_dna = self.new_datatype("DNA", "sequences of ATCG",
            Datatype.STR)
        self.cdt_record = CompoundDatatype()
        self.cdt_record.save()
        self.cdt_record.members.create(datatype=self.datatype_str, 
            column_name="header", column_idx=1)
        self.cdt_record.members.create(datatype=self.datatype_dna,
            column_name="sequence", column_idx=2)

        # Alice uploads code to perform each of the tasks. In the background, 
        # Shipyard creates new CodeResources for these scripts and sets her
        # uploaded files as the first CodeResourceRevisions.
        self.coderev_complement = self.make_first_revision("DNA complement",
                "a script to complement DNA", "complement.sh",
                """#!/bin/bash
                cat "$1" | cut -d ',' -f 2 | tr 'ATCG' 'TAGC' | paste -d, "$1" - | cut -d ',' -f 1,3 > "$2"
                """)
        self.coderev_reverse = self.make_first_revision("DNA reverse",
                "a script to reverse DNA", "reverse.sh",
                """#!/bin/bash
                cat "$1" | cut -d ',' -f 2 | rev | paste -d, "$1" - | cut -d ',' -f 1,3 > "$2"
                """)

        # To tell the system how to use her code, Alice creates two Methods,
        # one for each CodeResource. In the background, this creates two new
        # MethodFamilies with her Methods as the first member of each.
        self.method_complement = self.make_first_method("DNA complement",
                "a method to complement strings of DNA",
                self.coderev_complement)
        self.simple_method_io(self.method_complement, self.cdt_record,
                "DNA_to_complement", "complemented_DNA")
        self.method_reverse = self.make_first_method("DNA reverse",
                "a method to reverse strings of DNA", self.coderev_complement)
        self.simple_method_io(self.method_reverse, self.cdt_record,
                "DNA_to_reverse", "reversed_DNA")

        # Now Alice is ready to define her pipelines. She uses the GUI to drag
        # the "complement" method into the "complement" pipeline, creates
        # the pipeline's input and output, and connects them to the inputs and
        # output of the method.
        self.create_linear_pipeline(self.pipeline_complement,
            [self.method_complement], "lab data", "complemented lab data")
        self.pipeline_complement.create_outputs()
        self.create_linear_pipeline(self.pipeline_reverse,
            [self.method_reverse], "lab data", "reversed lab data")
        self.pipeline_reverse.create_outputs()
        self.create_linear_pipeline(self.pipeline_revcomp, 
            [self.method_reverse, self.method_complement], "lab data", 
            "reverse and complemented lab data")
        self.pipeline_revcomp.create_outputs()

        # Here is some data which is sitting on Alice's hard drive.
        self.labdata = "header,sequence\n"
        for i in range(10):
            seq = "".join([random.choice("ATCG") for j in range(10)])
            self.labdata += "patient{},{}\n".format(i, seq)
        self.datafile = tempfile.NamedTemporaryFile(delete=False)
        self.datafile.write(self.labdata)
        self.datafile.close()

        # Alice uploads the data to the system.
        self.symds_labdata = SymbolicDataset.create_SD(self.datafile.name,
            name="lab data", cdt=self.cdt_record, user=self.user_alice,
            description="data from the lab", make_dataset=True)

        # Now Alice is ready to run her pipelines. The system creates a Sandbox
        # where she will run each of her pipelines.
        self.sandbox_complement = Sandbox(self.user_alice, self.pipeline_complement, [self.symds_labdata])
        self.sandbox_revcomp = Sandbox(self.user_alice, self.pipeline_revcomp, [self.symds_labdata])

        # A second version of the complement Pipeline which doesn't keep any output.
        self.pipeline_complement_v2 = Pipeline(family=self.pipeline_complement.family,
            revision_name="2",
            revision_desc="second version")
        self.pipeline_complement_v2.save()
        self.create_linear_pipeline(self.pipeline_complement_v2,
            [self.method_complement], "lab data", "complemented lab data")
        last_output = self.pipeline_complement_v2.steps.last().add_deletion(
            self.method_complement.outputs.first())
        self.pipeline_complement_v2.outcables.first().delete()
        self.pipeline_complement_v2.create_outputs()

        # A second version of the reverse/complement Pipeline which doesn't keep 
        # intermediate or final output.
        self.pipeline_revcomp_v2 = Pipeline(family=self.pipeline_revcomp.family,
            revision_name="2",
            revision_desc="second version")
        self.pipeline_revcomp_v2.save()
        self.create_linear_pipeline(self.pipeline_revcomp_v2,
            [self.method_reverse, self.method_complement], "lab data",
            "revcomped lab data")
        self.pipeline_revcomp_v2.steps.first().add_deletion(
            self.method_reverse.outputs.first())
        self.pipeline_revcomp_v2.steps.last().add_deletion(
            self.method_complement.outputs.first())
        self.pipeline_revcomp_v2.outcables.first().delete()
        self.pipeline_revcomp_v2.create_outputs()

        # A third version of the reverse/complement Pipeline which keeps
        # final output, but not intermediate.
        self.pipeline_revcomp_v3 = Pipeline(family=self.pipeline_revcomp.family,
            revision_name="3",
            revision_desc="third version")
        self.pipeline_revcomp_v3.save()
        self.create_linear_pipeline(self.pipeline_revcomp_v3,
            [self.method_reverse, self.method_complement], "lab data",
            "revcomped lab data")
        self.pipeline_revcomp_v3.steps.first().add_deletion(
            self.method_reverse.outputs.first())
        self.pipeline_revcomp_v3.create_outputs()

        # Another method which turns DNA into RNA.
        self.coderev_DNA2RNA = self.make_first_revision("DNA to RNA",
                "a script to reverse DNA", "DNA2RNA.sh",
                """#!/bin/bash
                cat "$1" | cut -d ',' -f 2 | tr 'T' 'U' | paste -d, "$1" - | cut -d ',' -f 1,3 > "$2"
                """)
        self.method_DNA2RNA = self.make_first_method("DNA to RNA",
                "a method to turn strings of DNA into RNA",
                self.coderev_DNA2RNA)
        self.simple_method_io(self.method_DNA2RNA, self.cdt_record,
                "DNA_to_convert", "RNA")

        # A pipeline which reverses DNA, then turns it into RNA.
        self.pipeline_revRNA = self.make_first_pipeline("DNA to reversed RNA",
            "a pipeline to reverse DNA and translate it to RNA")
        self.create_linear_pipeline(self.pipeline_revRNA,
            [self.method_reverse, self.method_DNA2RNA], "lab data", "RNA'd lab data")
        self.pipeline_revRNA.create_outputs()

        # Separator to print between Pipeline executions, to make viewing logs easier.
        self.sep = " "*80 + "\n" + "*"*80 + "\n" + " "*80 + "\n"

        # Figure out the MD5 of the output file created when the complement method
        # is run on Alice's data, so we can check it later.
        tmpdir = tempfile.mkdtemp()
        outfile = os.path.join(tmpdir, "output")
        self.method_complement.run_code(tmpdir, [self.datafile.name], [outfile])
        time.sleep(1)
        self.labdata_compd_md5 = file_access_utils.compute_md5(open(outfile))
        shutil.rmtree(tmpdir)

    def tearDown(self):
        clean_files()
        os.remove(self.datafile.name)

    def test_execute_pipeline_spaces_in_dataset_name(self):
        """
        You should be allowed to have spaces in the name of your dataset.
        """
        coderev = self.make_first_revision("test",
                "a script for testing purposes", "test.sh",
                """#!/bin/bash
                cat "$1" > "$2"
                """)
        method = self.make_first_method("test", "a test method", coderev)
        self.simple_method_io(method, self.cdt_record,
                "input name with spaces", "more spaces")
        pipeline = self.make_first_pipeline("test", "a test pipeline")
        self.create_linear_pipeline(pipeline, [method], "in data", "out data")
        pipeline.create_outputs()
        
        sandbox = Sandbox(self.user_alice, pipeline, [self.symds_labdata])
        sandbox.execute_pipeline()
        runstep = sandbox.run.runsteps.first()
        execlog = runstep.log.first()
        print(execlog.methodoutput.error_log.read())
        self.assertEqual(runstep.successful_execution(), True)
        self.assertEqual(execlog.missing_outputs(), [])
        self.assertEqual(execlog.methodoutput.return_code, 0)

    def test_execute_pipeline_run(self):
        """
        Check the coherence of Runs created when a pipeline is executed the first time.
        """
        run = self.sandbox_complement.execute_pipeline()
        self.assertEqual(run.user, self.user_alice)
        self.assertEqual(run.start_time.date(), timezone.now().date())
        self.assertEqual(run.start_time < timezone.now(), True)
        self.assertEqual(run.is_complete(), True)
        self.assertEqual(run.parent_runstep, None)
        self.assertEqual(run.complete_clean(), None)

    def test_execute_pipeline_runstep(self):
        """
        Check the coherence of a RunStep created when a Pipeline is executed the first time.
        """
        run = self.sandbox_complement.execute_pipeline()
        runstep = run.runsteps.first()

        self.assertEqual(runstep.run, run)
        self.assertEqual(runstep.start_time.date(), timezone.now().date())
        self.assertEqual(runstep.start_time < timezone.now(), True)
        self.assertEqual(runstep.reused, False)
        self.assertEqual(runstep.is_complete(), True)
        self.assertEqual(runstep.complete_clean(), None)
        self.assertEqual(hasattr(runstep, "child_run"), False)
        self.assertEqual(runstep.successful_execution(), True)
        self.assertEqual(runstep.outputs.count(), 1)

    def test_execute_pipeline_symds_contents(self):
        """
        Test that the content checks, which take place as part of Pipeline
        execution, pass in the ordinary Pipeline execution case.
        """
        run = self.sandbox_complement.execute_pipeline()
        runstep = run.runsteps.first()
        execrecord = runstep.execrecord
        symds = execrecord.execrecordouts.first().symbolicdataset
        check = symds.content_checks.first()

        self.assertEqual(symds.content_checks.count(), 1) # should have been checked once
        self.assertEqual(check.symbolicdataset, symds)
        self.assertEqual(check.end_time is None, False)
        self.assertEqual(check.start_time <= check.end_time, True)
        self.assertEqual(check.start_time.date(), check.end_time.date())
        self.assertEqual(check.is_fail(), False)

    def test_execute_pipeline_symbolicdataset(self):
        """
        Test the integrity of a SymbolicDataset output by a PipelineStep in
        the middle of a Pipeline.
        """
        run = self.sandbox_complement.execute_pipeline()
        runstep = run.runsteps.first()
        execrecord = runstep.execrecord
        symds = execrecord.execrecordouts.first().symbolicdataset
        ds = runstep.outputs.first()

        sys.stderr.write(self.sep)

        self.assertEqual(symds.MD5_checksum, self.labdata_compd_md5)
        self.assertEqual(symds.dataset, ds)
        self.assertEqual(hasattr(symds, "usurps"), False)
        self.assertEqual(symds.has_data(), True)
        self.assertEqual(symds.num_rows(), 10)
        self.assertEqual(symds.is_raw(), False)
        self.assertEqual(symds.get_cdt(), self.cdt_record)
        self.assertEqual(symds.structure.compounddatatype, self.cdt_record)
        self.assertEqual(symds.structure.num_rows, 10)
        self.assertEqual(symds.is_OK(), True)

    def test_execute_pipeline_runstep_execrecordout(self):
        """
        Check the coherence of a RunStep's ExecRecord's ExecRecordOut, created
        when a Pipeline is executed the first time.
        """
        pipelinestep = self.pipeline_complement.steps.first()
        run = self.sandbox_complement.execute_pipeline()
        runstep = run.runsteps.first()
        symds_out = runstep.outputs.first().symbolicdataset
        execlog = runstep.log.first()
        execrecord = runstep.execrecord
        execrecordout = execrecord.execrecordouts.first()

        self.assertEqual(execrecordout is None, False)
        self.assertEqual(execrecordout.execrecord, execrecord)
        self.assertEqual(execrecordout.symbolicdataset, symds_out)
        self.assertEqual(execrecordout.generic_output, pipelinestep.transformation.outputs.first())
        self.assertEqual(execrecordout.has_data(), True)
        self.assertEqual(execrecordout.is_OK(), True)

    def test_execute_pipeline_runstep_execrecord(self):
        """
        Check the coherence of a RunStep's ExecRecord, created when a Pipeline
        is executed the first time.
        """
        run = self.sandbox_complement.execute_pipeline()
        runstep = run.runsteps.first()
        execlog = runstep.log.first()
        execrecord = runstep.execrecord
        outputs = self.method_complement.outputs.all()

        self.assertEqual(execrecord.generator, execlog)
        self.assertEqual(execrecord.runsteps.first(), runstep)
        #self.assertEqual(execrecord.runs.first(), run)
        self.assertEqual(execrecord.complete_clean(), None)
        self.assertEqual(execrecord.general_transf(), runstep.pipelinestep.transformation)
        self.assertEqual(execrecord.provides_outputs(outputs), True)
        self.assertEqual(execrecord.outputs_OK(), True)

    def test_execute_pipeline_twice(self):
        """
        You can't execute a pipeline twice in the same Sandbox.
        """
        run1 = self.sandbox_complement.execute_pipeline()
        run2 = self.sandbox_complement.execute_pipeline()
        self.assertEqual(run1 is run2, True)

    def test_execute_pipeline_reuse(self):
        """
        An identical pipeline, run in a different sandbox, should reuse an ExecRecord
        and not create an ExecLog.
        """
        self.sandbox_complement.execute_pipeline()
        sandbox2 = Sandbox(self.user_alice, self.pipeline_complement, [self.symds_labdata])
        sandbox2.execute_pipeline()

        step1 = self.sandbox_complement.run.runsteps.first()
        step2 = sandbox2.run.runsteps.first()

        self.assertEqual(step1.reused, False)
        self.assertEqual(step2.reused, True)
        self.assertEqual(step2.log.first(), None)
        self.assertEqual(step1.execrecord, step2.execrecord)

    def test_execute_pipeline_fill_in_ER(self):
        """
        Running an identical Pipeline where we did not keep the data around the first time
        should fill in an existing ExecRecord, but also create a new ExecLog.
        """

        sandbox = Sandbox(self.user_alice, self.pipeline_complement, [self.symds_labdata])
        sandbox.execute_pipeline()
        sys.stderr.write(self.sep)
        self.sandbox_complement.execute_pipeline()

        step1 = sandbox.run.runsteps.first()
        step2 = self.sandbox_complement.run.runsteps.first()

        self.assertEqual(step1.reused, False)
        self.assertEqual(step2.reused, True)
        self.assertEqual(step1.log.first() is not None, True)
        self.assertEqual(step2.log.first() is None, True)
        self.assertEqual(step1.execrecord, step2.execrecord)

    def test_execute_pipeline_reuse_within_different_pipeline(self):
        """
        Running the same dataset through the same Method, in two different 
        pipelines, should reuse an ExecRecord.
        """
        sandbox_reverse = Sandbox(self.user_alice, self.pipeline_reverse, [self.symds_labdata])
        sandbox_revcomp = Sandbox(self.user_alice, self.pipeline_revcomp, [self.symds_labdata])
        sandbox_reverse.execute_pipeline()
        sandbox_revcomp.execute_pipeline()

        step1 = sandbox_reverse.run.runsteps.first()
        step2 = sandbox_revcomp.run.runsteps.first()

        self.assertEqual(step1.reused, False)
        self.assertEqual(step2.reused, True)
        self.assertEqual(step2.log.first(), None)
        self.assertEqual(step1.execrecord, step2.execrecord)

    def test_execute_pipeline_output_symds(self):
        """
        A Pipeline with no deleted outputs should have a SymbolicDataset as an output.
        """
        self.sandbox_complement.execute_pipeline()
        output = self.sandbox_complement.run.runoutputcables.first()
        output_symds = output.execrecord.execrecordouts.first().symbolicdataset
        self.assertEqual(output_symds is not None, True)

    def test_pipeline_trivial_cable(self):
        """
        A trivial cable should have is_trivial() = True.
        """
        outcable = self.pipeline_complement.outcables.first()
        self.assertEqual(outcable.is_trivial(), True)

    def test_trivial_cable_num_rows(self):
        """
        A trivial cable should have the same dataset all the way through.
        """
        self.sandbox_complement.execute_pipeline()

        step = self.sandbox_complement.run.runsteps.first()
        step_output_SD = step.execrecord.execrecordouts.first().symbolicdataset

        outcable = self.sandbox_complement.run.runoutputcables.first()
        outcable_input_SD = outcable.execrecord.execrecordins.first().symbolicdataset
        outcable_output_SD = outcable.execrecord.execrecordouts.first().symbolicdataset

        self.assertEqual(step_output_SD, outcable_input_SD)
        self.assertEqual(outcable_input_SD, outcable_output_SD)
        self.assertEqual(step_output_SD.num_rows(), outcable_input_SD.num_rows())
        self.assertEqual(outcable_input_SD.num_rows(), outcable_output_SD.num_rows())

    def test_execute_pipeline_num_rows(self):
        """
        A pipeline which does not change the number of rows in a dataset,
        should have the same number of rows in all SD's along the way.
        """
        self.sandbox_complement.execute_pipeline()

        incable = self.sandbox_complement.run.runsteps.first().RSICs.first()
        incable_input_SD = incable.execrecord.execrecordins.first().symbolicdataset
        incable_output_SD = incable.execrecord.execrecordins.first().symbolicdataset

        step = self.sandbox_complement.run.runsteps.first()
        step_input_SD = step.execrecord.execrecordins.first().symbolicdataset
        step_output_SD = step.execrecord.execrecordouts.first().symbolicdataset

        outcable = self.sandbox_complement.run.runoutputcables.first()
        outcable_input_SD = outcable.execrecord.execrecordins.first().symbolicdataset
        outcable_output_SD = outcable.execrecord.execrecordouts.first().symbolicdataset

        self.assertEqual(incable_input_SD.num_rows(), self.symds_labdata.num_rows())
        self.assertEqual(incable_input_SD.num_rows(), incable_output_SD.num_rows())
        self.assertEqual(incable_output_SD.num_rows(), step_input_SD.num_rows())
        self.assertEqual(step_input_SD.num_rows(), step_output_SD.num_rows())
        self.assertEqual(step_output_SD.num_rows(), outcable_input_SD.num_rows())
        self.assertEqual(outcable_input_SD.num_rows(), outcable_output_SD.num_rows())

#    def test_execute_pipeline_reuse_clever(self):
#        """
#        A Pipeline uses, in an intermediate step, a Symbolic Dataset which has previously
#        been created by another pipeline.
#
#        At the moment, this fails, because of the way equality of SD's is
#        defined (we don't match by MD5). Possibly we will implement this in the future.
#        """
#        # Feed input dataset to both reverse and complement pipelines.
#        sandbox_reverse = Sandbox(self.user_alice, self.pipeline_reverse, [self.symds_labdata])
#        sandbox_reverse.execute_pipeline()
#        sandbox_complement = Sandbox(self.user_alice, self.pipeline_complement, [self.symds_labdata])
#        sandbox_complement.execute_pipeline()
#
#        outcable_complement = sandbox_complement.run.runoutputcables.first()
#        symds_compd = outcable_complement.execrecord.execrecordouts.first().symbolicdataset
#
#        # Now feed the reversed data into the reverse and complement pipeline.
#        # Out of the first step should come the input data (because we're just
#        # reversing what was reversed). Then we feed that into the complement
#        # step, which should reuse the ExecRecord from the first run of the
#        # complement pipeline.
#        outcable_reverse = sandbox_reverse.run.runoutputcables.first()
#        symds_reversed = outcable_reverse.execrecord.execrecordouts.first().symbolicdataset
#
#        sandbox_revcomp = Sandbox(self.user_alice, self.pipeline_revcomp, [symds_reversed])
#        sandbox_revcomp.execute_pipeline()
#
#        # What did we get out of the second (complement) step of the reverse and
#        # complement pipeline?
#        rev_step = sandbox_revcomp.run.runsteps.first()
#        comp_step = sandbox_revcomp.run.runsteps.last()
#        rev_output = rev_step.execrecord.execrecordouts.first().symbolicdataset
#        comp_output = comp_step.execrecord.execrecordouts.first().symbolicdataset
#
#        self.assertEqual(rev_output, self.symds_labdata)
#        self.assertEqual(comp_output, symds_compd)
#        self.assertEqual(comp_step.reused, True)

    def test_discard_intermediate_file(self):
        """
        A Pipeline which indicates one of its intermediate outputs should not be kept,
        should not create any datasets for that output.
        """
        step = self.pipeline_revcomp_v2.steps.first()
        sandbox = Sandbox(self.user_alice, self.pipeline_revcomp_v2, [self.symds_labdata])
        sandbox.execute_pipeline()
        runstep = sandbox.run.runsteps.first()
        output = runstep.execrecord.execrecordouts.first().symbolicdataset
        self.assertEqual(runstep.pipelinestep.outputs_to_retain(), [])
        self.assertEqual(output.has_data(), False)

    def test_recover_intermediate_dataset(self):
        """
        Test recovery of an intermediate dataset.
        """
        # Don't keep the intermediate or final output.
        sandbox = Sandbox(self.user_alice, self.pipeline_revcomp_v2, [self.symds_labdata])
        sandbox.execute_pipeline()
        steps = sandbox.run.runsteps.all()
        steps = sorted(steps, key = lambda step: step.pipelinestep.step_num)

        # This time we need the final output - that means we have to recover the intermediate
        # output.
        sandbox2 = Sandbox(self.user_alice, self.pipeline_revcomp_v3, [self.symds_labdata])
        sandbox2.execute_pipeline()

class BadRunTests(UtilityMethods):
    """
    Tests for when things go wrong during Pipeline execution.
    """
    def setUp(self):
        super(BadRunTests, self).setUp()

        # A guy who doesn't know what he is doing.
        self.user_grandpa = User.objects.create_user('grandpa', 'gr@nd.pa', '123456')
        self.user_grandpa.save()

        # A code resource, method, and pipeline which are empty.
        self.coderev_faulty = self.make_first_revision("faulty",
            "a script...?",
            "faulty.sh", "")
        self.method_faulty = self.make_first_method("faulty",
                "a method to... uh...",
                self.coderev_faulty)
        self.method_faulty.clean()
        self.simple_method_io(self.method_faulty, self.cdt_string,
                "strings", "i don't know")
        self.pipeline_faulty = self.make_first_pipeline("faulty pipeline",
            "a pipeline to do nothing")
        self.create_linear_pipeline(self.pipeline_faulty,
            [self.method_faulty, self.method_noop], "data", "the abyss")
        self.pipeline_faulty.create_outputs()

        # Some data to run through the faulty pipeline.
        self.grandpa_datafile = tempfile.NamedTemporaryFile(delete=False)
        self.grandpa_datafile.write("word\n")
        for line in range(20):
            i = random.randint(1,99171)
            self.grandpa_datafile.write("{}\n".format(i))
        self.grandpa_datafile.close()
        self.symds_grandpa = SymbolicDataset.create_SD(self.grandpa_datafile.name,
            name="numbers", cdt=self.cdt_string, user=self.user_grandpa,
            description="numbers which are actually strings", make_dataset=True)
        self.symds_grandpa.clean()

    def tearDown(self):
        super(BadRunTests, self).tearDown()
        clean_files()

    def test_code_bad_execution(self):
        """
        If the user's code bombs, we should get an ExecLog with a -1 return code.
        """
        sandbox = Sandbox(self.user_grandpa, self.pipeline_faulty, [self.symds_grandpa])
        sandbox.execute_pipeline()
        runstep1 = sandbox.run.runsteps.first()
        log = runstep1.log.first()
        interm_SD = runstep1.execrecord.execrecordouts.first().symbolicdataset
        self.assertEqual(log.is_successful(), False)
        self.assertEqual(log.methodoutput.return_code, -1)
        self.assertEqual(log.missing_outputs(), [interm_SD])

class FindSDTests(UtilityMethods):
    """
    Tests for first_generator_of_SD.
    """
    def setUp(self):
        super(FindSDTests, self).setUp()

        self.user_bob = User.objects.create_user('bob', 'bob@talabs.com', 'verysecure')
        self.user_bob.save()
        self.setup_simple_pipeline()
        self.setup_twostep_pipeline()
        self.setup_nested_pipeline()

    def tearDown(self):
        super(FindSDTests, self).tearDown()
        clean_files()
        os.remove(self.string_datafile.name)
        os.remove(self.words_datafile.name)

    def make_custom_wire(self, cable):
        source_cdt = cable.source.structure.first().compounddatatype
        dest_cdt = cable.dest.structure.first().compounddatatype
        cable.custom_wires.create(source_pin=source_cdt.members.first(), 
            dest_pin=dest_cdt.members.last())
        cable.custom_wires.create(source_pin=source_cdt.members.last(), 
            dest_pin=dest_cdt.members.first())

    def setup_nested_pipeline(self):
        # A two-step pipeline with custom cable wires at each step.
        self.pipeline_nested = self.make_first_pipeline("nested pipeline",
            "a pipeline with a sub-pipeline")

        transforms = [self.method_noop_backwords, self.pipeline_twostep, self.method_noop_backwords]
        self.create_linear_pipeline(self.pipeline_nested,
            transforms, "data", "unchanged data")
        cable = self.pipeline_nested.steps.last().cables_in.first()
        self.make_custom_wire(cable)
        self.pipeline_nested.create_outputs()
        self.pipeline_nested.complete_clean()
    
    def setup_twostep_pipeline(self):
        """
        (drow,word) (word,drow) (word,drow)    (drow,word)  (drow,word)    (drow,word)
                         _____________              ______________
           [o]====<>====|o           o|=====<>=====|o            o|============[o]
                        |   reverse   |            |     noop     |
                        |_____________|            |______________|
        """
        # A code resource which reverses a file.
        self.coderev_reverse = self.make_first_revision("reverse",
            "a script to reverse lines of a file", "reverse.sh",
            '#!/bin/bash\nrev "$1" | sed \'s/\\r//g\' > "$2"')

        # A CDT with two columns, word and drow.
        self.cdt_words = CompoundDatatype()
        self.cdt_words.save()
        self.cdt_words.members.create(datatype=self.datatype_str,
            column_name="word", column_idx=1)
        self.cdt_words.members.create(datatype=self.datatype_str,
            column_name="drow", column_idx=2)

        # A second CDT, much like the first :]
        self.cdt_backwords = CompoundDatatype()
        self.cdt_backwords.save()
        self.cdt_backwords.members.create(datatype=self.datatype_str,
            column_name="drow", column_idx=1)
        self.cdt_backwords.members.create(datatype=self.datatype_str,
            column_name="word", column_idx=2)

        # Methods for the reverse CRR, and noop CRR with backwords CDT.
        self.method_reverse = self.make_first_method("string reverse",
            "a method to reverse strings",
            self.coderev_reverse)
        self.simple_method_io(self.method_reverse, self.cdt_words,
            "words_to_reverse", "reversed_words")
        self.method_noop_backwords = self.make_first_method("noop",
            "a method to do nothing on two columns",
            self.coderev_noop)
        self.simple_method_io(self.method_noop_backwords, self.cdt_backwords,
            "backwords", "more_backwords")

        # A two-step pipeline with custom cable wires at each step.
        self.pipeline_twostep = self.make_first_pipeline("two-step pipeline",
            "a two-step pipeline with custom cable wires at each step")
        self.pipeline_twostep.create_input(compounddatatype=self.cdt_backwords,
            dataset_name="words_to_reverse", dataset_idx = 1)

        methods = [self.method_reverse, self.method_noop_backwords]
        for i, method in enumerate(methods):
            step = self.pipeline_twostep.steps.create(transformation=methods[i], step_num=i+1)
            if i == 0:
                source = self.pipeline_twostep.inputs.first()
            else:
                source = methods[i-1].outputs.first()
            cable = step.cables_in.create(source_step = i, 
                source = source,
                dest = methods[i].inputs.first())
            self.make_custom_wire(cable)

        cable = self.pipeline_twostep.create_outcable(output_name = "reversed_words",
            output_idx = 1,
            source_step = 2,
            source = methods[-1].outputs.first())

        self.pipeline_twostep.create_outputs()
        self.pipeline_twostep.complete_clean()

        # Some data to run through the two-step pipeline.
        self.words_datafile = tempfile.NamedTemporaryFile(delete=False)
        self.words_datafile.write('drow,word\n')
        for line in range(20):
            i = random.randint(1,99171)
            sed = Popen(["sed", "{}q;d".format(i), "/usr/share/dict/words"],
                        stdout=PIPE)
            word, _ = sed.communicate()
            word = word.strip()
            self.words_datafile.write('{},{}\n'.format(word[::-1], word))
        self.words_datafile.close()

        self.symds_backwords = SymbolicDataset.create_SD(self.words_datafile.name,
            name="backwords", cdt=self.cdt_backwords, user=self.user_bob,
            description="random reversed words", make_dataset=True)
    
    def setup_simple_pipeline(self):
        # A simple, one-step pipeline, which does nothing.
        self.pipeline_noop = self.make_first_pipeline("simple pipeline",
            "a simple, one-step pipeline")
        self.create_linear_pipeline(self.pipeline_noop,
            [self.method_noop], "lab data", "complemented lab data")
        self.pipeline_noop.create_outputs()

        # Some data to run through the simple pipeline.
        self.string_datafile = tempfile.NamedTemporaryFile(delete=False)
        self.string_datafile.write("word\n")
        self.string_datafile.close()
        # Aw heck.
        os.system("cat /usr/share/dict/words >> {}".
                format(self.string_datafile.name))
        self.symds_words = SymbolicDataset.create_SD(self.string_datafile.name,
            name="blahblah", cdt=self.cdt_string, user=self.user_bob,
            description="blahblahblah", make_dataset=True)

    def test_find_symds_pipeline_input(self):
        """
        Finding a SymbolicDataset which was input to a Pipeline should return None
        as the generator, and the top-level run as the run.
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_noop, [self.symds_words])
        sandbox.execute_pipeline()

        run, gen = sandbox.first_generator_of_SD(self.symds_words)
        self.assertEqual(run, sandbox.run)
        self.assertEqual(gen, None)

    def test_find_symds_step_output(self):
        """
        Finding a SymbolicDataset which was output from a step, and also input
        to a cable, should return the step (and in particular, not the cable).
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_noop, [self.symds_words])
        sandbox.execute_pipeline()
        symds_out = sandbox.run.runsteps.first().execrecord.execrecordouts.first().symbolicdataset

        run, gen = sandbox.first_generator_of_SD(symds_out)
        self.assertEqual(run, sandbox.run)
        self.assertEqual(gen, self.pipeline_noop.steps.first())

    def test_find_symds_pipeline_input_custom_wire(self):
        """
        Finding a SymbolicDataset which was passed through a custom wire to a
        Pipeline should return the cable as the generator, and the top-level
        run as the run.
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_twostep, [self.symds_backwords])
        sandbox.execute_pipeline()

        runcable = sandbox.run.runsteps.first().RSICs.first()
        symds_to_find = runcable.execrecord.execrecordouts.first().symbolicdataset

        run, gen = sandbox.first_generator_of_SD(symds_to_find)
        self.assertEqual(run, sandbox.run)
        self.assertEqual(gen, runcable.PSIC)

    def test_find_symds_custom_wire(self):
        """
        Finding a SymbolicDataset which was produced by a custom wire as an 
        intermediate step should return the cable as the generator, and the
        top-level run as the run.
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_twostep, [self.symds_backwords])
        sandbox.execute_pipeline()

        runcable = sandbox.run.runsteps.last().RSICs.first()
        symds_to_find = runcable.execrecord.execrecordouts.first().symbolicdataset

        run, gen = sandbox.first_generator_of_SD(symds_to_find)
        self.assertEqual(run, sandbox.run)
        self.assertEqual(gen, runcable.PSIC)

    def test_find_symds_subpipeline(self):
        """
        Find a symbolic dataset in a sub-pipeline, which is output from a step.
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_nested, [self.symds_backwords])
        sandbox.execute_pipeline()

        for step in sandbox.run.runsteps.all():
            if step.pipelinestep.step_num == 2:
                subrun = step.child_run
                runstep = subrun.runsteps.first()
                outrecord = runstep.execrecord.execrecordouts.first()
                symds_to_find = outrecord.symbolicdataset
                break

        run, gen = sandbox.first_generator_of_SD(symds_to_find)
        self.assertEqual(run, subrun)
        self.assertEqual(gen, runstep.pipelinestep)

    def test_find_symds_subpipeline_input(self):
        """
        Find a symbolic dataset in a sub-pipeline, which is input to the sub-pipeline
        on a custom cable.
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_nested, [self.symds_backwords])
        sandbox.execute_pipeline()

        for step in sandbox.run.runsteps.all():
            if step.pipelinestep.step_num == 2:
                subrun = step.child_run
                runstep = subrun.runsteps.first()
                cable = runstep.RSICs.first()
                symds_to_find = runstep.execrecord.execrecordins.first().symbolicdataset

        run, gen = sandbox.first_generator_of_SD(symds_to_find)
        self.assertEqual(run, subrun)
        self.assertEqual(gen, cable.PSIC)

class CustomConstraintTests(UtilityMethods):
    """
    Test the creation and use of custom constraints.
    """

    def setUp(self):
        self.user_oscar = User.objects.create_user('oscar', 'oscar@thegrouch.com', 'garbage')
        self.workdir = tempfile.mkdtemp()

        # A Datatype with basic constraints.
        self.dt_basic = self._setup_datatype("alpha", "strings of letters", 
                [("regexp", "^[A-Za-z]+$")], 
                [Datatype.objects.get(pk=datatypes.STR_PK)])
        
        # A Datatype with custom constraints restricting the basic datatype.
        self.dt_custom = self._setup_datatype("words", 
                "correctly spelled words", [], [self.dt_basic])

        # Set up the custom constraint, a spell checker.
        self._setup_custom_constraint("spellcheck",
            "a spell checker",
            """#!/bin/bash
            echo failed_row > "$2"
            row_num=0
            for row in $(cat "$1"); do
              if [[ $row_num -gt 0 ]]; then
                 if [[ "x$(echo $row | aspell list)" != "x" ]]; then
                    echo $row_num >> "$2"
                 fi  
              fi  
              row_num=$(($row_num+1))
            done""",
            self.dt_custom)

        # A compound datatype composed of alphabetic strings and correctly
        # spelled words.
        self.cdt_constraints = self._setup_compounddatatype(
                [self.dt_basic, self.dt_custom],
                ["letter strings", "words"])

        # A file conforming to the compound datatype.
        self.good_datafile = self._setup_datafile(self.cdt_constraints,
                [["abcab", "hello"], ["goodbye", "world"]])

        # A file not conforming to the compound datatype.
        self.bad_datafile = self._setup_datafile(self.cdt_constraints,
                [["hello", "Spock"], ["1ive", "10ng"], ["and", "porsper"]])

        # A pipeline to process the constraint CDT.
        self.pipeline_noop = self._setup_onestep_pipeline("noop",
                "does nothing", '#!/bin/bash\n cat "$1" > "$2"',
                self.cdt_constraints)

        # A pipeline to mess up the constraint CDT.
        self.pipeline_mangle = self._setup_onestep_pipeline("mangle",
                "messes up data", 
                """#!/bin/bash
                echo "letter strings,words" > "$2"
                echo 1234,yarrr >> "$2"
                """, self.cdt_constraints)

    def tearDown(self):
        super(self.__class__, self).tearDown()
        os.remove(self.good_datafile)
        os.remove(self.bad_datafile)
        clean_files()

    def _setup_onestep_pipeline(self, name, desc, script, cdt):
        """
        Helper function to set up a one step pipeline which passes the same
        dataset all the way through.

        name    name of the pipeline
        desc    description for the pipeline
        script  contents of CodeResourceRevision which will drive the method
        cdt     CompoundDatatype used throughout the pipeline
        """
        coderev = self.make_first_revision(name, desc, 
                "{}.sh".format(name), script)
        method = self.make_first_method(name, desc, coderev)
        self.simple_method_io(method, cdt, "in data", "out data")
        pipeline = self.make_first_pipeline(name, desc)
        self.create_linear_pipeline(pipeline, [method], "in data", "out data")
        pipeline.create_outputs()
        return pipeline

    def _setup_datafile(self, compounddatatype, lines):
        """
        Helper function to set up a datafile for a compounddatatype on the file
        system.
        """
        datafile = tempfile.NamedTemporaryFile(delete=False, dir=self.workdir)
        header = [m.column_name for m in compounddatatype.members.all()]
        writer = csv.writer(datafile)
        writer.writerow(header)
        [writer.writerow(line) for line in lines]
        datafile.close()
        return datafile.name

    def _setup_datatype(self, name, desc, basic_constraints, restricts):
        """
        Helper function to set up a Datatype, given a list of basic
        constraints (which are tuples (ruletype, rule)), and a list
        of other datatypes to restrict.
        """
        datatype = Datatype(name=name, description=desc)
        datatype.save()
        for supertype in restricts:
            datatype.restricts.add(supertype)
        for ruletype, rule in basic_constraints:
            datatype.basic_constraints.create(ruletype=ruletype, rule=rule)
        return(datatype)

    def _setup_compounddatatype(self, datatypes, column_names):
        """
        Helper function to create a compound datatype, given a list of members
        and column names.
        """
        compounddatatype = CompoundDatatype()
        compounddatatype.save()
        for i in range(len(datatypes)):
            compounddatatype.members.create(datatype=datatypes[i],
                    column_name = column_names[i], column_idx=i+1)
        compounddatatype.save()
        return compounddatatype

    def _setup_custom_constraint(self, name, desc, script, datatype):
        """
        Helper function to set up a custom constraint on a datatype.
        
        INPUTS
        name        name of the code resource of the verifier
        desc        description for the code resource of the verifier
        script      contents of verification script
        datatype    datatype which will recieve custom constraint
        """
        scriptfile = tempfile.NamedTemporaryFile(delete=False,
                dir=self.workdir)
        scriptfile.write(script)
        scriptfile.close()

        coderesource = CodeResource(name=name, filename="{}.sh".format(name),
                description=desc)
        coderesource.save()
        revision = coderesource.revisions.create(revision_name="1", 
                revision_desc="first version", content_file=scriptfile.name)
        revision.save()
        methodfamily = MethodFamily()
        methodfamily.save()
        method = methodfamily.members.create(driver=revision)
        method.create_input("to_test", 1, 
                compounddatatype=CompoundDatatype.objects.get(pk=CDTs.VERIF_IN_PK))
        method.create_output("failed_row", 1, 
                compounddatatype=CompoundDatatype.objects.get(pk=CDTs.VERIF_OUT_PK))
        method.save()
        customconstraint = CustomConstraint(datatype = datatype,
                verification_method = method)
        customconstraint.save()

    def _setup_content_check_log(self, datafile, cdt, user, name, desc):
        """
        Helper function to create a SymbolicDataset and ContentCheckLog
        for a given CompoundDatatype.
        """
        symbolicdataset = SymbolicDataset.create_SD(datafile, cdt=cdt,
                user=user, name=name, description=desc)
        log = ContentCheckLog(symbolicdataset=symbolicdataset)
        log.save()
        return log

    def tearDown(self):
        # Clean up the work directory.
        shutil.rmtree(self.workdir)
        super(self.__class__, self).tearDown()
        clean_files()

    def test_summarize_CSV_no_output(self):
        """
        A verification method which produces no output should throw a ValueError.
        """
        dt_no_output = self._setup_datatype("numerics", "strings of digits",
                [("regexp", "^[0-9]+$")],
                [Datatype.objects.get(pk=datatypes.INT_PK)])
        self._setup_custom_constraint("empty", "a script producing no output",
                "#!/bin/bash", dt_no_output)
        cdt_no_output = self._setup_compounddatatype( 
                [dt_no_output, self.dt_basic],
                ["numerics", "letter strings"])
        no_output_datafile = self._setup_datafile(cdt_no_output,
                [[123, "foo"], [456, "bar"], [789, "baz"]])

        self.assertRaisesRegexp(ValueError,
                re.escape(error_messages["verification_no_output"].
                        format(dt_no_output)),
                lambda: SymbolicDataset.create_SD(no_output_datafile, 
                        cdt_no_output, self.user_oscar, "no output", 
                        "data with a bad verifier"))

        os.remove(no_output_datafile)

    def test_verification_method_failed_row_too_large(self):
        """
        If a verification method produces a row which is greater than the number
        of rows in the input, a ValueError should be raised.
        """
        dt_big_row = self._setup_datatype("barcodes",
                "strings of upper case alphanumerics of length between 10 and 12", 
                [("regexp", "^[A-Z0-9]+$"), ("minlen", 10), ("maxlen", 12)],
                [Datatype.objects.get(pk=datatypes.STR_PK)])
        self._setup_custom_constraint("bigrow", 
                "a script outputting a big row number",
                '#!/bin/bash\necho -e "failed_row\\n1000" > "$2"',
                dt_big_row)
        cdt_big_row = self._setup_compounddatatype(
                [dt_big_row, self.dt_custom], ["barcodes", "words"])
        big_row_datafile = self._setup_datafile(cdt_big_row,
                [["ABCDE12345", "hello"], ["12345ABCDE", "goodbye"]])

        self.assertRaisesRegexp(ValueError,
                re.escape(error_messages["verification_large_row"].
                    format(dt_big_row, 1000, 2)),
                lambda: SymbolicDataset.create_SD(big_row_datafile,
                    cdt=cdt_big_row, user=self.user_oscar, name="big row",
                    description="data with a verifier outputting too high a row number"))

        os.remove(big_row_datafile)

    def test_summarize_correct_datafile(self):
        """
        A conforming datafile should return a CSV summary with no errors.
        """
        log = self._setup_content_check_log(self.good_datafile,
            self.cdt_constraints, self.user_oscar, "constraint data",
            "data to test custom constraint checking")
        with open(self.good_datafile) as f:
            summary = self.cdt_constraints.summarize_CSV(f, self.workdir, log)
        expected_header = [m.column_name for m in self.cdt_constraints.members.all()]
        self.assertEqual(summary.has_key("num_rows"), True)
        self.assertEqual(summary.has_key("header"), True)
        self.assertEqual(summary.has_key("bad_num_cols"), False)
        self.assertEqual(summary.has_key("bad_col_indices"), False)
        self.assertEqual(summary.has_key("failing_cells"), False)
        self.assertEqual(summary["num_rows"], 2)
        self.assertEqual(summary["header"], expected_header)

    def test_create_SD_bad_datafile(self):
        """
        We sholudn't be allowed to create a SymbolicDataset from a bad file.
        """
        self.assertRaisesRegexp(ValueError,
                re.escape(error_messages["bad_input_file"].
                    format(self.bad_datafile, self.cdt_constraints)),
                lambda: SymbolicDataset.create_SD(self.bad_datafile, 
                    cdt=self.cdt_constraints, user=self.user_oscar,
                    name="bad data", 
                    description="invalid data to test custom constraint checking"))

    def _test_content_check_integrity(self, content_check, execlog, symds):
        """
        Things which should be true about a ContentCheckLog, whether or not
        it indicated errors.
        """
        self.assertEqual(content_check.clean(), None)
        self.assertEqual(content_check.execlog, execlog)
        self.assertEqual(content_check.symbolicdataset, symds)
        self.assertEqual(content_check.end_time is not None, True)
        self.assertEqual(content_check.start_time.date(),
                content_check.end_time.date())
        self.assertEqual(content_check.start_time <= content_check.end_time,
                True)

    def _test_upload_data_good(self):
        """
        Helper function to upload good data.
        """
        symds_good = SymbolicDataset.create_SD(self.good_datafile,
                cdt=self.cdt_constraints, user=self.user_oscar,
                name="good data",
                description="data which conforms to all its constraints")
        return symds_good

    def _test_upload_data_bad(self):
        """
        Helper function to upload bad data.
        """
        symds_bad = SymbolicDataset.create_SD(self.bad_datafile,
                cdt=self.cdt_constraints, user=self.user_oscar,
                name="good data",
                description="data which conforms to all its constraints")
        return symds_bad

    def _test_setup_prototype_good(self):
        prototype_cdt = CompoundDatatype.objects.get(pk=CDTs.PROTOTYPE_PK)
        prototype_file = self._setup_datafile(prototype_cdt, 
                [["hello", "True"], ["hell", "True"], ["hel", "False"],
                 ["he", "True"], ["h", "False"]])
        prototype_SD = SymbolicDataset.create_SD(prototype_file, 
                cdt=prototype_cdt, user=self.user_oscar, name="good prototype",
                description="working prototype for constraint CDT")
        os.remove(prototype_file)

        # Add a prototype to the custom DT, and make a new CDT.
        self.dt_custom.prototype = prototype_SD.dataset
        self.dt_custom.save()
        cdt = self._setup_compounddatatype(
                [self.dt_basic, self.dt_custom],
                ["letter strings", "words"])
        return cdt

    def _test_setup_prototype_bad(self):
        prototype_cdt = CompoundDatatype.objects.get(pk=CDTs.PROTOTYPE_PK)
        prototype_file = self._setup_datafile(prototype_cdt, 
                [["hello", "False"], ["hell", "True"], ["hel", "False"],
                 ["he", "True"], ["h", "False"]])
        prototype_SD = SymbolicDataset.create_SD(prototype_file, 
                cdt=prototype_cdt, user=self.user_oscar, name="good prototype",
                description="working prototype for constraint CDT")
        os.remove(prototype_file)

        # Add a prototype to the custom DT.
        self.dt_custom.prototype = prototype_SD.dataset
        self.dt_custom.save()
        return self.dt_custom

    def _test_execute_pipeline_constraints(self, pipeline):
        """
        Helper function to execute a pipeline with the cdt_constraints 
        compound datatype as input.
        """
        symds_good = self._test_upload_data_good()
        sandbox = Sandbox(self.user_oscar, pipeline, [symds_good])
        sandbox.execute_pipeline()
        runstep = sandbox.run.runsteps.first()
        execlog = runstep.log.first()
        symds_out = runstep.execrecord.execrecordouts.first().symbolicdataset
        content_check = symds_out.content_checks.first()
        return (content_check, execlog, symds_out)

    def test_execute_pipeline_content_check_good(self):
        """
        Test the integrity of the ContentCheck created while running a
        Pipeline on some data with CustomConstraints.
        """
        content_check, execlog, symds_out = \
                self._test_execute_pipeline_constraints(self.pipeline_noop)
        self._test_content_check_integrity(content_check, execlog, symds_out)
        self.assertEqual(content_check.is_fail(), False)

    def test_execute_pipeline_content_check_bad(self):
        """
        Test the integrity of the ContentCheck created while running a
        Pipeline on some data with CustomConstraints, where the output data
        does not pass the content check.
        """
        content_check, execlog, symds_out = \
                self._test_execute_pipeline_constraints(self.pipeline_mangle)
        self._test_content_check_integrity(content_check, execlog, symds_out)
        self.assertEqual(content_check.is_fail(), True)

    def test_upload_data_content_check_good(self):
        """
        Test the integrity of a ContentCheck created when uploading a dataset.
        """
        symds_good = self._test_upload_data_good()
        content_check = symds_good.content_checks.first()
        self._test_content_check_integrity(content_check, None, symds_good)
        self.assertEqual(content_check.is_fail(), False)

    def _test_verification_log(self, verif_log, content_check, CDTM):
        """
        Checks which should pass for any VerificationLog, succesful or not.
        """
        self.assertEqual(verif_log is not None, True)
        self.assertEqual(verif_log.clean(), None)
        self.assertEqual(verif_log.contentchecklog, content_check) 
        self.assertEqual(verif_log.CDTM, CDTM)
        self.assertEqual(verif_log.end_time is not None, True)
        self.assertEqual(verif_log.end_time.date(), verif_log.start_time.date())
        self.assertEqual(verif_log.start_time <= verif_log.end_time, True)

    def test_execute_pipeline_verification_log_good(self):
        """
        Test the integrity of the VerificationLog created while running a
        Pipeline on some data with CustomConstraints.
        """
        content_check, execlog, symds_out = \
                self._test_execute_pipeline_constraints(self.pipeline_noop)

        verif_log = content_check.verification_logs.first()
        self._test_verification_log(verif_log, content_check, 
                self.cdt_constraints.members.last())
        self.assertEqual(verif_log.return_code, 0)
        self.assertEqual(verif_log.output_log.read(), "")
        self.assertEqual(verif_log.error_log.read(), "")

    def test_execute_pipeline_verification_log_bad(self):
        """
        Test the integrity of the VerificationLog created while running a
        Pipeline on some data with CustomConstraints, when the data does not
        conform.
        """
        content_check, execlog, symds_out = \
                self._test_execute_pipeline_constraints(self.pipeline_mangle)

        verif_log = content_check.verification_logs.first()
        self._test_verification_log(verif_log, content_check, 
                self.cdt_constraints.members.last())
        self.assertEqual(verif_log.return_code, 0)
        self.assertEqual(verif_log.output_log.read(), "")
        self.assertEqual(verif_log.error_log.read(), "")

    def test_upload_data_verification_log_good(self):
        """
        Test the integrity of the VerificationLog created while uploading
        conforming data with CustomConstraints.
        """
        symds_good = self._test_upload_data_good()
        content_check = symds_good.content_checks.first()
        verif_log = content_check.verification_logs.first()
        self._test_verification_log(verif_log, content_check, 
                self.cdt_constraints.members.last())
        self.assertEqual(verif_log.return_code, 0)
        self.assertEqual(verif_log.output_log.read(), "")
        self.assertEqual(verif_log.error_log.read(), "")

    def test_upload_data_prototype_good_contentcheck(self):
        """
        Test the integrity of the ContentCheckLog created when a Dataset with
        CustomConstraints is uploaded with a working prototype.
        """
        cdt = self._test_setup_prototype_good()
        symds_good = SymbolicDataset.create_SD(self.good_datafile,
                cdt=cdt, user=self.user_oscar, name="good data",
                description="data which conforms to all its constraints")
        self.assertEqual(symds_good.clean(), None)
        content_check = symds_good.content_checks.first()
        self._test_content_check_integrity(content_check, None, symds_good)
        self.assertEqual(content_check.is_fail(), False)

    def test_upload_data_prototype_bad(self):
        """
        Test the integrity of the ContentCheckLog created when a Dataset with
        CustomConstraints is uploaded with a working prototype.
        """
        dt = self._test_setup_prototype_bad()
        self.assertRaisesRegexp(ValidationError,
                re.escape(error_messages["prototype_bad_invalid"].format(dt, "hello")),
                dt.clean)
