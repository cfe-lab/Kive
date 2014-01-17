
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
        coderesource_id = request.POST.get('id')

