"""
Generate an HTML form to create a new Datatype object
"""

from django.contrib.admin.widgets import FilteredSelectMultiple
from django import forms
from copperfish.models import Datatype, BasicConstraint, CodeResource, CodeResourceRevision, CodeResourceDependency
from datetime import datetime

class DatatypeForm (forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(DatatypeForm, self).__init__(*args, **kwargs)
        
    restricts = forms.ModelMultipleChoiceField(queryset = Datatype.objects.all(), required=False, help_text='The new Datatype is a special case of one or more existing Datatypes; e.g., DNA restricts string.')
    Python_type = forms.ChoiceField(Datatype.PYTHON_TYPE_CHOICES, widget=forms.Select(attrs={'onchange': 'switchConstraintForm(this.value)'}), help_text='How the Datatype will be stored in the database.')
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


# code resource forms
class CodeResourceForm (forms.ModelForm):
    class Meta:
        model = CodeResource

class CodeResourcePrototypeForm (forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(CodeResourcePrototypeForm, self).__init__(*args, **kwargs)
        self.fields['revision_name'].help_text = 'Name for this new code resource'
        self.fields['revision_desc'].help_text = 'A detailed description of this new code resource'
        self.fields['content_file'].help_text = 'File containing this new code resource'
    class Meta:
        model = CodeResourceRevision
        #fields = ('revision_name', 'revision_desc', 'content_file')
        exclude = ('revision_parent', 'coderesource', 'MD5_checksum',)

class CodeResourceRevisionForm (forms.ModelForm):
   class Meta:
        model = CodeResourceRevision
        fields = ('revision_name', 'revision_desc', 'content_file', )

class CodeResourceDependencyForm (forms.ModelForm):
    coderesource = forms.ChoiceField([(x, x.name) for x in CodeResource.objects.all()])
    class Meta:
        model = CodeResourceDependency
        exclude = ('coderesourcerevision', 'requirement')

