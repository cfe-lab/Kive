from django.http import HttpResponse, Http404
from django.core import serializers
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType

from pipeline.models import Pipeline, PipelineFamily
from archive.models import Dataset, RunOutputCable
from execute import Sandbox

import json
from pprint import pprint

def qs2dict(qs):
    """Make a QuerySet or Django object into a dictionary."""
    if qs.__class__.__name__ == "QuerySet":
        ser = serializers.serialize("json", qs)
        return json.loads(ser)
    else:
        ser = serializers.serialize("json", [qs,])
        return json.loads(ser)[0]

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
            input_dict = qs2dict(trans_input)
            compat_dict = qs2dict(compat)
            res.append([input_dict, compat_dict])
        response.write(json.dumps(res))
        return response
    else:
        raise Http404

def get_pipeline_outputs(request):
    if request.is_ajax():
        response = HttpResponse()
        pipeline_pk = request.POST.get("pk")
        pipeline = Pipeline.objects.get(pk=pipeline_pk)
        roc_ctype = ContentType.objects.get_for_model(RunOutputCable)

        res = [[qs2dict(to), []] for to in pipeline.outputs.order_by("dataset_idx")]
        for run in pipeline.pipeline_instances.all():
            for i, outcable in enumerate(run.runoutputcables.order_by("pipelineoutputcable__output_idx")):
                sd = outcable.execrecord.execrecordouts.first().symbolicdataset
                dataset_dict = qs2dict(sd.dataset)
                if sd.has_data() and not dataset_dict in res[i][1]:
                    res[i][1].append(dataset_dict)
        response.write(json.dumps(res))
        return response
    else:
        raise Http404

def run_pipeline(request):
    """Run a Pipeline as the global Shipyard user."""
    if request.is_ajax():
        response = HttpResponse()
        pipeline_pk = request.POST.get("pipeline_pk")
        pipeline = Pipeline.objects.get(pk=pipeline_pk)

        dataset_pks = request.POST.getlist("dataset_pks[]")

        datasets = [Dataset.objects.get(pk=pk) for pk in dataset_pks]
        inputs = [d.symbolicdataset for d in datasets]

        user = User.objects.get(username="shipyard")

        sandbox = Sandbox(user, pipeline, inputs)
        sandbox.execute_pipeline()

        response.write(serializers.serialize("json", [sandbox.run,]))
        return response
    else:
        raise Http404
