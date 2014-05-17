import tempfile
import csv
import re
import os
import shutil

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError

from metadata.models import Datatype, CompoundDatatype, CustomConstraint
from method.models import MethodFamily, CodeResource
from librarian.models import SymbolicDataset
from datachecking.models import ContentCheckLog
from sandbox.execute import Sandbox

from sandbox.tests_rm import UtilityMethods, clean_files

from constants import datatypes, CDTs

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
        self._setup_custom_constraint("spellcheck", "a spell checker",
            """#!/bin/bash
            echo failed_row > "$2"
            row_num=0
            for row in $(cat "$1"); do
              if [[ $row_num -gt 0 ]]; then
                 grep $row /usr/share/dict/words > /dev/null
                 if [[ $? -eq 1 ]]; then
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
                [["hello", "there"], ["live", "long"], ["and", "porsper"]])

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
        scriptfile = tempfile.NamedTemporaryFile(delete=False, dir=self.workdir)
        scriptfile.write(script)
        scriptfile.close()

        coderesource = CodeResource(name=name, filename="{}.sh".format(name), description=desc)
        coderesource.save()
        revision = coderesource.revisions.create(revision_name="1", revision_desc="first version",
                                                 content_file=scriptfile.name)
        revision.save()
        methodfamily = MethodFamily()
        methodfamily.save()
        method = methodfamily.members.create(driver=revision)
        method.create_input("to_test", 1, compounddatatype=CompoundDatatype.objects.get(pk=CDTs.VERIF_IN_PK))
        method.create_output("failed_row", 1, compounddatatype=CompoundDatatype.objects.get(pk=CDTs.VERIF_OUT_PK))
        method.save()
        cc = CustomConstraint(verification_method = method)
        cc.save()
        datatype.custom_constraint = cc
        datatype.save()

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
        self._setup_custom_constraint("empty", "a script producing no output", "#!/bin/bash", dt_no_output)
        cdt_no_output = self._setup_compounddatatype( 
                [dt_no_output, self.dt_basic],
                ["numerics", "letter strings"])
        no_output_datafile = self._setup_datafile(cdt_no_output,
                [[123, "foo"], [456, "bar"], [789, "baz"]])

        self.assertRaisesRegexp(ValueError,
                                re.escape('Verification method for Datatype "{}" produced no output'
                                          .format(dt_no_output)),
                                lambda: SymbolicDataset.create_SD(no_output_datafile, cdt_no_output, self.user_oscar,
                                                                  "no output", "data with a bad verifier"))
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
                                re.escape('Verification method for Datatype "{}" indicated an error in row {}, but '
                                          'only {} rows were checked'.format(dt_big_row, 1000, 2)),
                                lambda: SymbolicDataset.create_SD(big_row_datafile, cdt=cdt_big_row,
                                    user=self.user_oscar, name="big row", 
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
        We sholudn't be allowed to create a SymbolicDataset from a file
        which does not conform to the Datatype's CustomConstraints.
        """
        self.assertRaisesRegexp(ValueError,
                                re.escape('The entry at row {}, column {} of file "{}" did not pass the constraints of '
                                          'Datatype "{}"'.format(3, 2, self.bad_datafile, self.dt_custom)),
                                lambda: SymbolicDataset.create_SD(self.bad_datafile, cdt=self.cdt_constraints,
                                    user=self.user_oscar, name="bad data", 
                                    description="invalid data to test custom constraint checking"))

    def _test_content_check_integrity(self, content_check, execlog, symds):
        """
        Things which should be true about a ContentCheckLog, whether or not
        it indicated errors.
        """
        self.assertEqual(content_check.clean(), None)
        self.assertEqual(content_check.execlog, execlog)
        self.assertEqual(content_check.symbolicdataset, symds)
        self.assertIsNotNone(content_check.end_time)
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
        content_check, execlog, symds_out = self._test_execute_pipeline_constraints(self.pipeline_noop)
        self._test_content_check_integrity(content_check, execlog, symds_out)
        self.assertEqual(content_check.is_fail(), False)

    def test_execute_pipeline_content_check_bad(self):
        """
        Test the integrity of the ContentCheck created while running a
        Pipeline on some data with CustomConstraints, where the output data
        does not pass the content check.
        """
        content_check, execlog, symds_out = self._test_execute_pipeline_constraints(self.pipeline_mangle)
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
        self.assertIsNotNone(verif_log)
        self.assertIsNone(verif_log.clean())
        self.assertEqual(verif_log.contentchecklog, content_check) 
        self.assertEqual(verif_log.CDTM, CDTM)
        self.assertIsNotNone(verif_log.start_time)
        self.assertIsNotNone(verif_log.end_time)
        self.assertEqual(verif_log.end_time.date(), verif_log.start_time.date())
        self.assertEqual(verif_log.start_time <= verif_log.end_time, True)

    def test_execute_pipeline_verification_log_good(self):
        """
        Test the integrity of the VerificationLog created while running a
        Pipeline on some data with CustomConstraints.
        """
        content_check, execlog, symds_out = self._test_execute_pipeline_constraints(self.pipeline_noop)

        verif_log = content_check.verification_logs.first()
        self._test_verification_log(verif_log, content_check, self.cdt_constraints.members.last())
        self.assertEqual(verif_log.return_code, 0)
        self.assertEqual(verif_log.output_log.read(), "")
        self.assertEqual(verif_log.error_log.read(), "")

    def test_execute_pipeline_verification_log_bad(self):
        """
        Test the integrity of the VerificationLog created while running a
        Pipeline on some data with CustomConstraints, when the data does not
        conform.
        """
        content_check, execlog, symds_out = self._test_execute_pipeline_constraints(self.pipeline_mangle)
        verif_log = content_check.verification_logs.first()
        self._test_verification_log(verif_log, content_check, self.cdt_constraints.members.last())
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
        self._test_verification_log(verif_log, content_check, self.cdt_constraints.members.last())
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
        CustomConstraints is uploaded with a prototype not agreeing with the
        CustomConstraints.
        """
        dt = self._test_setup_prototype_bad()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('The prototype for Datatype "{}" indicates the value "{}" should be '
                                          'invalid, but it passed all constraints'.format(dt, "hello")),
                                dt.clean)
