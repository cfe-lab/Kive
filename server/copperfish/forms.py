"""
Generate an HTML form to create a new Datatype object
"""

from django.contrib.admin.widgets import FilteredSelectMultiple
from django import forms
from copperfish.models import Datatype, BasicConstraint
from datetime import datetime

class DatatypeForm (forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(DatatypeForm, self).__init__(*args, **kwargs)
        
    restricts = forms.ModelMultipleChoiceField(queryset = Datatype.objects.all(), required=False)
    Python_type = forms.ChoiceField(Datatype.PYTHON_TYPE_CHOICES)
    date_created = datetime.now()
    
    class Meta:
        model = Datatype
        fields = ('name', 'description', 'restricts', 'Python_type')
    
    #restricts = forms.ModelMultipleChoiceField(queryset = Datatype.objects.all(),
    #   widget = FilteredSelectMultiple('name', is_stacked=False))
    #python_type = forms.ModelChoiceField(queryset = Datatype.objects.all())
    



class BasicConstraintForm (forms.ModelForm):
    ruletype = forms.ChoiceField(BasicConstraint.CONSTRAINT_TYPES)
    class Meta:
        model = BasicConstraint
        exclude = ('datatype', )
