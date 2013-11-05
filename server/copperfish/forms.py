"""
Generate an HTML form to create a new Datatype object
"""

from django.contrib.admin.widgets import FilteredSelectMultiple
from django import forms
from copperfish.models import Datatype, BasicConstraint
from datetime import datetime

class DatatypeForm (forms.ModelForm):
    restricts = forms.ModelMultipleChoiceField(queryset = Datatype.objects.all(), required=False)
    Python_type = forms.ChoiceField(Datatype.PYTHON_TYPE_CHOICES)
    date_created = datetime.now()
    """
    # constraints
    reg_exp = forms.CharField()
    min_len = forms.IntegerField()
    max_len = forms.IntegerField()
    min_val = forms.IntegerField()
    max_val = forms.IntegerField()
    """
    class Meta:
        model = Datatype
        fields = ('name', 'description',)
    
    #restricts = forms.ModelMultipleChoiceField(queryset = Datatype.objects.all(),
    #   widget = FilteredSelectMultiple('name', is_stacked=False))
    #python_type = forms.ModelChoiceField(queryset = Datatype.objects.all())
    



class BasicConstraintForm (forms.ModelForm):
    ruletype = forms.ChoiceField(BasicConstraint.CONSTRAINT_TYPES)
    class Meta:
        model = BasicConstraint
        exclude = ('datatype', )
