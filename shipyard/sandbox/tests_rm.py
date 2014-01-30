import os
import tempfile
import shutil
import random
import logging

from django.core.files import File
from django.contrib.auth.models import User
from django.test import TestCase
from django.utils import timezone

from archive.models import MethodOutput
from librarian.models import SymbolicDataset
from metadata.models import Datatype, CompoundDatatype
from method.models import CodeResource, CodeResourceRevision, Method, MethodFamily
from pipeline.models import Pipeline, PipelineFamily
from sandbox.execute import Sandbox

import file_access_utils

class ExecuteTestsRM(TestCase):

    def setUp(self):
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
        self.pipeline_revcomp = self.make_first_pipeline("DNA reverse",
                "a pipeline to reverse and complement DNA")

        # Alice is only going to be manipulating DNA, so she creates a
        # "DNA" data type, and a "string" datatype for the headers. She
        # also creates a compound "record" datatype for sequence + header.
        self.datatype_dna = self.new_datatype("DNA", "sequences of ATCG",
            Datatype.STR)
        self.datatype_str = self.new_datatype("string", 
            "sequences of ASCII characters", Datatype.STR)
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
                cat $1 | cut -d ',' -f 2 | tr 'ATCG' 'TAGC' | paste -d, $1 - | cut -d ',' -f 1,3 > $2
                """)
        self.coderev_reverse = self.make_first_revision("DNA reverse",
                "a script to reverse DNA", "reverse.sh",
                """#!/bin/bash
                cat $1 | cut -d ',' -f 2 | rev | paste -d, $1 - | cut -d ',' -f 1,3 > $2
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
            revision_name="2",
            revision_desc="second version")
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
                cat $1 | cut -d ',' -f 2 | tr 'T' 'U' | paste -d, $1 - | cut -d ',' -f 1,3 > $2
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

        self.sep = " "*80 + "\n" + "*"*80 + "\n" + " "*80 + "\n"

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
        datatype = Datatype(name=dtname, description=dtdesc, 
          Python_type=pytype)
        datatype.clean()
        datatype.save()
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

        sandbox = Sandbox(self.user_alice, self.pipeline_complement_v2, [self.symds_labdata])
        sandbox.execute_pipeline()
        self.sandbox_complement.execute_pipeline()

        step1 = sandbox.run.runsteps.first()
        step2 = self.sandbox_complement.run.runsteps.first()

        self.assertEqual(step1.reused, False)
        self.assertEqual(step2.reused, False)
        self.assertEqual(step2.log.first() is not None, True)
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

        print(self.sep)

        # This time we need the final output - that means we have to recover the intermediate
        # output.
        sandbox2 = Sandbox(self.user_alice, self.pipeline_revcomp_v3, [self.symds_labdata])
        sandbox2.execute_pipeline()
