from django.http import HttpResponse, Http404
from method.models import MethodFamily, Method
from django.core import serializers
import json

def populate_method_revision_dropdown (request):
    """
    copied from Method ajax.py
    """
    if request.is_ajax():
        response = HttpResponse()
        method_family_id = request.POST.get('mf_id')
        if method_family_id != '':
            method_family = MethodFamily.objects.get(pk=method_family_id)

            # Go through all Methods with this family and retrieve their primary key and revision_name.
            method_dicts = []
            for curr_method in Method.objects.filter(family=method_family):
                method_dicts.append({"pk": curr_method.pk, "model": "method.method",
                                     "fields": {"revision_name": curr_method.revision_name}})

            response.write(json.dumps(method_dicts))
        return response
    else:
        raise Http404


def get_method_io (request):
    """
    handles ajax request from pipelines.html
    populates a dictionary with information about this method's transformation
    inputs and outputs, returns as JSON.
    """
    if request.is_ajax():
        method_id = request.POST.get('mid')
        method = Method.objects.filter(pk=method_id)[0]

        inputs = {}
        for input in method.inputs.all():
            if not input.has_structure:
                # input is unstructured
                cdt_pk = '__raw__'
                cdt_label = 'raw'
            else:
                structure = input.structure
                cdt_pk = structure.compounddatatype.pk
                cdt_label = structure.compounddatatype.__unicode__()
            inputs.update({input.dataset_idx: {'datasetname': input.dataset_name,
                                               'cdt_pk': cdt_pk,
                                               'cdt_label': cdt_label}})
        outputs = {}
        for output in method.outputs.all():
            if not output.has_structure:
                # output is unstructured
                cdt_pk = '__raw__'
                cdt_label = 'raw'
            else:
                structure = output.structure
                cdt_pk = structure.compounddatatype.pk
                cdt_label = structure.compounddatatype.__unicode__()
            outputs.update({output.dataset_idx: {'datasetname': output.dataset_name,
                                                 'cdt_pk': cdt_pk,
                                                 'cdt_label': cdt_label}})

        response_data = {'inputs': inputs, 'outputs': outputs}
        return HttpResponse(json.dumps(response_data), content_type='application/json')
    else:
        raise Http404
