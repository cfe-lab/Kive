from django.http import HttpResponse, Http404
from django.core import serializers
from django.contrib.auth.models import User

from pipeline.models import Pipeline, PipelineFamily
from archive.models import Dataset
from execute import Sandbox

import json

def qs2dict(qs):
    ser = serializers.serialize("json", qs)
    return json.loads(ser)

def get_pipeline_inputs(request):
    """Get inputs for a pipeline, and possible datasets for each."""
    if request.is_ajax():
        response = HttpResponse()
        pipeline_pk = request.POST.get("pk")
        pipeline = Pipeline.objects.get(pk=pipeline_pk)
        res = []
        for trans_input in pipeline.inputs.all():
            cdt = trans_input.compounddatatype
            if trans_input.is_raw():
                compat = Dataset.objects.filter(symbolicdataset__structure__isnull=True)
            else:
                compat = Dataset.objects.filter(symbolicdataset__structure__compounddatatype=cdt)
            input_dict = qs2dict([trans_input,])[0]
            compat_dict = qs2dict(compat)
            res.append([input_dict, compat_dict])
        response.write(json.dumps(res))
        return response
    else:
        raise Http404

def run_pipeline(request):
    """Run a Pipeline as the global Shipyard user."""
    if request.is_ajax():
        response = HttpResponse()
        pipeline_pk = request.POST.get("pipeline_pk")
        dataset_pks = request.POST.get("dataset_pks")
        user = User.objects.get(first_name="shipyard")
        response.write(serializers.serialize("json", user))
        return response
    else:
        raise Http404
