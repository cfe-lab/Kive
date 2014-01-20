from django.http import HttpResponse, Http404
from copperfish.models import CodeResource, CodeResourceRevision
from django.core import serializers

def populate_revision_dropdown (request):
    """
    resource_add.html template can render multiple forms for CodeResourceDependency that
     have fields for CodeResource and CodeResourceRevision.  We want to only populate the
     latter with the revisions that correspond to the CodeResource selected in the first
     drop-down.  The 'change' event triggers an Ajax request that this function will handle
     and return a JSON object with the revision info.
    """
    from copperfish.models import CodeResourceRevision
    json = {}
    if request.is_ajax():
        coderesource_id = request.POST.get('cr_id')
        coderesource = CodeResource.objects.get(pk=coderesource_id) # pk (primary key) implies id__exact
        response = HttpResponse()
        response.write(serializers.serialize("json", CodeResourceRevision.objects.filter(coderesource=coderesource), fields=('pk', 'revision_name')))
        print response
        return response
    else:
        raise Http404
