"""
Generate an HTML form to create a new Datatype object
"""

from django import forms
from method.models import CodeResource, CodeResourceRevision, CodeResourceDependency

# code resource forms
class CodeResourceMinimalForm (forms.Form):
    """
    use for validating only two entries
    """
    revision_name = forms.CharField(max_length=255)
    revision_desc = forms.CharField(max_length=255)

class CodeResourcePrototypeForm (forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(CodeResourcePrototypeForm, self).__init__(*args, **kwargs)
        self.fields['revision_name'].label = 'Name of prototype'
        self.fields['revision_name'].help_text = 'A short name for this prototype'
        self.fields['revision_desc'].label = 'Description'
        self.fields['revision_desc'].help_text = 'A detailed description of this prototype'
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
    coderesource = forms.ChoiceField([('', '--- CodeResource ---')] + [(x.id, x.name) for x in CodeResource.objects.all()])
    revisions = forms.ChoiceField(choices=[('', '--- select a CodeResource first ---')])
    class Meta:
        model = CodeResourceDependency
        exclude = ('coderesourcerevision', 'requirement')

