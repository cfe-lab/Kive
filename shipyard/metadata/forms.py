"""
metadata.forms
"""

from django import forms
from metadata.models import Datatype, BasicConstraint, CompoundDatatypeMember
from datetime import datetime

class DatatypeForm (forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(DatatypeForm, self).__init__(*args, **kwargs)
        
    restricts = forms.ModelMultipleChoiceField(queryset = Datatype.objects.all(), required=False, help_text='The new Datatype is a special case of one or more existing Datatypes; e.g., DNA restricts string.')
    #Python_type = forms.ChoiceField(Datatype.PYTHON_TYPE_CHOICES, widget=forms.Select(attrs={'onchange': 'switchConstraintForm(this.value)'}), help_text='How the Datatype will be stored in the database.')
    Python_type = forms.ChoiceField(Datatype.PYTHON_TYPE_CHOICES, help_text='How the Datatype will be stored in the database.')
    date_created = datetime.now()
    
    class Meta:
        model = Datatype
        fields = ('name', 'description', 'restricts', 'Python_type')
    
    #restricts = forms.ModelMultipleChoiceField(queryset = Datatype.objects.all(),
    #   widget = FilteredSelectMultiple('name', is_stacked=False))
    #python_type = forms.ModelChoiceField(queryset = Datatype.objects.all())
    
class BasicConstraintForm (forms.ModelForm):
    #ruletype = forms.ChoiceField(BasicConstraint.CONSTRAINT_TYPES)
    class Meta:
        model = BasicConstraint
        #exclude = ('datatype', )

class IntegerConstraintForm (forms.Form):
    minval = forms.FloatField(required=False, help_text='Minimum numerical value')
    maxval = forms.FloatField(required=False, help_text='Maximum numerical value')

class StringConstraintForm (forms.Form):
    minlen = forms.IntegerField(required=False, help_text='Minimum string length (must be non-negative integer)')
    maxlen = forms.IntegerField(required=False, help_text='Maximum string length (must be non-negative integer)')
    regexp = forms.CharField(required=False, help_text='A regular expression that can be recognized by the Python re module (Perl-like syntax).')

class CompoundDatatypeMemberForm(forms.ModelForm):
    datatype = forms.ModelChoiceField(queryset = Datatype.objects.all(), required=True, help_text="This column's expected datatype")
    #column_idx = forms.ChoiceField(choices=[(1, '1')])
    class Meta:
        model = CompoundDatatypeMember
        exclude = ('compounddatatype','column_idx')
