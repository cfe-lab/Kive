from django.http import HttpResponse, Http404
from method.models import MethodFamily, Method
from django.core import serializers
import json

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


def get_method_io (request):
    if request.is_ajax():
        method_id = request.POST.get('mid')
        response_data = {}
        method = Method.objects.filter(pk=method_id)[0]
        response_data.update({'inputs': method.get_num_inputs(),
                              'outputs': method.get_num_outputs()})
        return HttpResponse(json.dumps(response_data), content_type='application/json')
    else:
        raise Http404
