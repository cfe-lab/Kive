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

from method.tests import samplecode_path

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
        self.pipeline_revcomp = self.make_first_pipeline("DNA reverse",
                "a pipeline to reverse strings of DNA")

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
        self.create_linear_pipeline(self.pipeline_revcomp, 
            [self.method_complement, self.method_reverse], "lab data", 
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

        # Begin the tests!

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
        See what happens when we try to reuse a pipeline.
        """
        self.sandbox_complement.execute_pipeline()
        sandbox2 = Sandbox(self.user_alice, self.pipeline_complement, [self.symds_labdata])
        sandbox2.execute_pipeline()

    def test_execute_pipeline_recover(self):
        pipeline_complement_v2 = Pipeline(family=self.pipeline_complement.family,
            revision_name="2",
            revision_desc="second version")
        pipeline_complement_v2.save()
        self.create_linear_pipeline(pipeline_complement_v2,
            [self.method_complement], "lab data", "complemented lab data")
        pipeline_complement_v2.steps.last().add_deletion(1)
        pipeline_complement_v2.outcables.first().delete()
        pipeline_complement_v2.create_outputs()

        sandbox = Sandbox(self.user_alice, pipeline_complement_v2, [self.symds_labdata])
        sandbox.execute_pipeline()
        sandbox.execute_pipeline()

    def test_execute_cable_deleted_data(self):
        """
        What happens if we deleted the datafile?
        """
        scratch_dir = tempfile.mkdtemp()
        output_path = tempfile.mkstemp(dir=scratch_dir)[1]
        infile = tempfile.mkstemp(dir=scratch_dir, prefix="")[1]
        shutil.copyfile(self.datafile.name, infile)

        symds_todelete = SymbolicDataset.create_SD(infile,
            name="bate", cdt=self.cdt_record, user=self.user_alice,
            description="data the gremlins are going to delete", make_dataset=True)
        os.remove(symds_todelete.dataset.dataset_file.name)

        run = self.pipeline_complement.pipeline_instances.create(user=self.user_alice)
        record = self.pipeline_complement.steps.first().pipelinestep_instances.create(run=run)
        cable = self.pipeline_complement.steps.first().cables_in.first()
        self.sandbox_complement.execute_pipeline()
        #self.sandbox_complement.execute_cable(cable, symds_todelete, output_path, record)

        shutil.rmtree(scratch_dir)
