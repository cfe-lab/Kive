from django.test import TestCase

from transformation.models import *
from metadata.models import *


class TransformationTestCase(TestCase):
    fixtures = ["initial_data"]

    def setUp(self):
        # Create some objects.
        t = Transformation(); t.save()
        Transformation().save()

        for i in range(4):
            TransformationInput(transformation=t, dataset_idx=i+1, dataset_name=str(i)).save()
        for i in range(4):
            TransformationOutput(transformation=t, dataset_idx=i+1, dataset_name=str(i)).save()

        XputStructure(transf_xput=TransformationInput.objects.all()[0], 
                compounddatatype=CompoundDatatype.objects.first()).save()
        XputStructure(transf_xput=TransformationInput.objects.all()[1], 
                compounddatatype=CompoundDatatype.objects.first(),
                min_row=0,
                max_row=100).save()
        XputStructure(transf_xput=TransformationOutput.objects.all()[0], 
                compounddatatype=CompoundDatatype.objects.first()).save()
        XputStructure(transf_xput=TransformationOutput.objects.all()[1], 
                compounddatatype=CompoundDatatype.objects.first(),
                min_row=0,
                max_row=100).save()

    def tearDown(self):
        pass


class XputStructureTests(TransformationTestCase):
    def setUp(self):
        super(XputStructureTests, self).setUp()

    def tearDown(self):
        super(XputStructureTests, self).tearDown()

    def test_identical_all_fields_set(self):
        """Two XputStructures with all the same fields are identical."""
        s1, s2 = XputStructure.objects.all()[:2]
        s1.min_row = s2.min_row = 0
        s1.max_row = s2.max_row = 100
        s1.compounddatatype = s2.compounddatatype
        s1.save()
        s2.save()
        self.assertTrue(s1.is_identical(s2))

    def test_identical_min_row_none(self):
        """
        Two XputStructures with null min_rows, and all other fields the
        same, are identical.
        """
        s1, s2 = XputStructure.objects.all()[:2]
        s1.min_row = s2.min_row = None
        s1.max_row = s2.max_row = 100
        s1.compounddatatype = s2.compounddatatype
        s1.save()
        s2.save()
        self.assertTrue(s1.is_identical(s2))

    def test_identical_max_row_none(self):
        """
        Two XputStructures with null max_rows, and all other fields the
        same, are identical.
        """
        s1, s2 = XputStructure.objects.all()[:2]
        s1.min_row = s2.min_row = 3
        s1.max_row = s2.max_row = None
        s1.compounddatatype = s2.compounddatatype
        s1.save()
        s2.save()
        self.assertTrue(s1.is_identical(s2))


class TransformationInputTests(TransformationTestCase):
    def setUp(self):
        super(TransformationInputTests, self).setUp()

    def tearDown(self):
        super(TransformationInputTests, self).tearDown()

    def test_inputs_identical_no_structure(self):
        """Two raw TransformationInputs are identical."""
        t1, t2 = TransformationInput.objects.filter(structure__isnull=True)[:2]
        for t in (t1, t2):
            if not t.is_raw():
                t.structure.delete()
                t.save()
        self.assertTrue(t1.is_raw() and t2.is_raw())
        self.assertTrue(t1.is_identical(t2))

    def test_inputs_identical_with_structure(self):
        """Two TransformationInputs with the same structure are identical."""
        t1, t2 = TransformationInput.objects.filter(structure__isnull=False)[:2]
        s = XputStructure.objects.first()
        for t in (t1, t2):
            t.structure = s
            t.save()
        self.assertTrue(t1.structure.is_identical(t2.structure))
        self.assertTrue(t1.is_identical(t2))

    def test_inputs_not_identical_one_structure(self):
        """A Raw TransformationInput is not identical to a non-raw one."""
        t1 = TransformationInput.objects.filter(structure__isnull=False).first()
        t2 = TransformationInput.objects.filter(structure__isnull=True).first()
        self.assertFalse(t1.is_identical(t2))

    def test_inputs_not_identical_different_structure(self):
        """
        Two TransformationInputs with different structures are not
        identical.
        """
        t1 = TransformationInput.objects.filter(structure__isnull=False).first()
        t2 = TransformationInput.objects.filter(structure__isnull=False).exclude(structure=t1.structure).first()
        self.assertFalse(t1.is_identical(t2))


class TransformationOutputTests(TransformationTestCase):
    def setUp(self):
        super(TransformationOutputTests, self).setUp()

    def tearDown(self):
        super(TransformationOutputTests, self).tearDown()

    def test_inputs_identical_no_structure(self):
        """Two raw TransformationOutputs are identical."""
        t1, t2 = TransformationOutput.objects.filter(structure__isnull=True)[:2]
        for t in (t1, t2):
            if not t.is_raw():
                t.structure.delete()
                t.save()
        self.assertTrue(t1.is_raw() and t2.is_raw())
        self.assertTrue(t1.is_identical(t2))

    def test_inputs_identical_with_structure(self):
        """Two TransformationOutputs with the same structure are identical."""
        t1, t2 = TransformationOutput.objects.filter(structure__isnull=False)[:2]
        s = XputStructure.objects.first()
        for t in (t1, t2):
            t.structure = s
            t.save()
        self.assertTrue(t1.structure.is_identical(t2.structure))
        self.assertTrue(t1.is_identical(t2))

    def test_inputs_not_identical_one_structure(self):
        """A Raw TransformationOutput is not identical to a non-raw one."""
        t1 = TransformationOutput.objects.filter(structure__isnull=False).first()
        t2 = TransformationOutput.objects.filter(structure__isnull=True).first()
        self.assertFalse(t1.is_identical(t2))

    def test_inputs_not_identical_different_structure(self):
        """
        Two TransformationOutputs with different structures are not
        identical.
        """
        t1 = TransformationOutput.objects.filter(structure__isnull=False).first()
        t2 = TransformationOutput.objects.filter(structure__isnull=False).exclude(structure=t1.structure).first()
        self.assertFalse(t1.is_identical(t2))


class TransformationTests(TransformationTestCase):
    def setUp(self):
        super(TransformationTests, self).setUp()

    def tearDown(self):
        super(TransformationTests, self).tearDown()

    def test_identical_no_xputs(self):
        """Two Transformations with no inputs or outputs are identical."""
        t1, t2 = Transformation.objects.all()[:2]
        t1.inputs.all().delete()
        t2.inputs.all().delete()
        t1.outputs.all().delete()
        t2.outputs.all().delete()
        self.assertTrue(t1.is_identical(t2))

    def test_identical_same_xputs(self):
        """Two Transformations with the same xputs are identical."""
        t1 = Transformation.objects.filter(inputs__isnull=False, outputs__isnull=False).first()
        t2 = Transformation.objects.filter(inputs__isnull=True, outputs__isnull=True).first()
        for input1 in t1.inputs.all():
            t2.create_input(
                    "x" + input1.dataset_name,
                    input1.dataset_idx,
                    compounddatatype=input1.get_cdt(), 
                    min_row=input1.get_min_row(),
                    max_row=input1.get_max_row())
        for output1 in t1.outputs.all():
            t2.create_output(
                    "x" + output1.dataset_name,
                    output1.dataset_idx,
                    compounddatatype=output1.get_cdt(), 
                    min_row=output1.get_min_row(),
                    max_row=output1.get_max_row())
        self.assertTrue(t1.inputs.count() == t2.inputs.count())
        self.assertTrue(t1.outputs.count() == t2.outputs.count())
        self.assertTrue(t1.is_identical(t2))

    def test_identical_different_inputs(self):
        """Two Transformations with different inputs are not identical."""
        t1 = Transformation.objects.filter(inputs__isnull=False, outputs__isnull=False).first()
        t2 = Transformation.objects.filter(inputs__isnull=True, outputs__isnull=True).first()
        for input1 in t1.inputs.all():
            # Modify one of the inputs.
            if input1.dataset_idx == 1:
                if input1.is_raw():
                    t2.create_input(
                        "x" + input1.dataset_name,
                        input1.dataset_idx,
                        compounddatatype=CompoundDatatype.objects.first(),
                        min_row=0,
                        max_row=100)
                else:
                    t2.create_input("x" + input1.dataset_name, input1.dataset_idx)
            else:
                t2.create_input(
                        "x" + input1.dataset_name,
                        input1.dataset_idx,
                        compounddatatype=input1.get_cdt(), 
                        min_row=input1.get_min_row(),
                        max_row=input1.get_max_row())
        for output1 in t1.outputs.all():
            t2.create_output(
                    "x" + output1.dataset_name,
                    output1.dataset_idx,
                    compounddatatype=output1.get_cdt(), 
                    min_row=output1.get_min_row(),
                    max_row=output1.get_max_row())
        self.assertTrue(t1.inputs.count() == t2.inputs.count())
        self.assertTrue(t1.outputs.count() == t2.outputs.count())
        self.assertFalse(t1.is_identical(t2))

    def test_identical_different_outputs(self):
        """Two Transformations with different outputs are not identical."""
        t1 = Transformation.objects.filter(inputs__isnull=False, outputs__isnull=False).first()
        t2 = Transformation.objects.filter(inputs__isnull=True, outputs__isnull=True).first()
        for output1 in t1.outputs.all():
            # Modify one of the outputs.
            if output1.dataset_idx == 1:
                if output1.is_raw():
                    t2.create_output(
                        "x" + output1.dataset_name,
                        output1.dataset_idx,
                        compounddatatype=CompoundDatatype.objects.first(),
                        min_row=0,
                        max_row=100)
                else:
                    t2.create_output("x" + output1.dataset_name, output1.dataset_idx)
            else:
                t2.create_output(
                        "x" + output1.dataset_name,
                        output1.dataset_idx,
                        compounddatatype=output1.get_cdt(), 
                        min_row=output1.get_min_row(),
                        max_row=output1.get_max_row())
        for input1 in t1.inputs.all():
            t2.create_input(
                    "x" + input1.dataset_name,
                    input1.dataset_idx,
                    compounddatatype=input1.get_cdt(), 
                    min_row=input1.get_min_row(),
                    max_row=input1.get_max_row())
        self.assertTrue(t1.inputs.count() == t2.inputs.count())
        self.assertTrue(t1.outputs.count() == t2.outputs.count())
        self.assertFalse(t1.is_identical(t2))
