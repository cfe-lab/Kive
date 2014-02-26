from django.http import HttpResponse, Http404
from method.models import MethodFamily, Method
from django.core import serializers

def populate_method_revision_dropdown (request):
    """
    copied from Method ajax.py
    """
    json = {}
    if request.is_ajax():
        response = HttpResponse()
        method_family_id = request.POST.get('mf_id')
        if method_family_id != '':
            method_family = MethodFamily.objects.get(pk=method_family_id)
            response.write(serializers.serialize("json", Method.objects.filter(family=method_family), fields=('pk', 'revision_name')))
        return response
    else:
        raise Http404
