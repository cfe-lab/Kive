from django.http import HttpResponse, Http404
from method.models import CodeResource, CodeResourceRevision
from django.core import serializers

def populate_revision_dropdown (request):
    """
    resource_add.html template can render multiple forms for CodeResourceDependency that
     have fields for CodeResource and CodeResourceRevision.  We want to only populate the
     latter with the revisions that correspond to the CodeResource selected in the first
     drop-down.  The 'change' event triggers an Ajax request that this function will handle
     and return a JSON object with the revision info.
    """
    if request.is_ajax():
        response = HttpResponse()
        coderesource_id = request.GET.get('cr_id')
        if coderesource_id != '':
            # pk (primary key) implies id__exact
            coderesource = CodeResource.objects.get(pk=coderesource_id)
            response.write(
                serializers.serialize("json",
                                      CodeResourceRevision.objects
                                      .filter(coderesource=coderesource)
                                      .order_by('-revision_number'),
                                      fields=('pk', 'revision_number', 'revision_name')))
        return response
    else:
        raise Http404
