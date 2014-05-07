"""
pipeline views
"""

from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, Context
from django.core.context_processors import csrf
from method.models import *
from metadata.models import *
from pipeline.models import *
import json

def pipelines(request):
    """
    Display existing pipeline families, represented by the
    root members (without parent).
    """
    t = loader.get_template('pipeline/pipelines.html')
    pipelines = Pipeline.objects.filter(revision_parent=None)
    c = Context({'pipelines': pipelines})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def pipeline_add(request):
    """
    Most of the heavy lifting is done by JavaScript and HTML5.
    I don't think we need to use forms here.
    """
    t = loader.get_template('pipeline/pipeline_add.html')
    method_families = MethodFamily.objects.all()
    compound_datatypes = CompoundDatatype.objects.all()
    c = Context({'method_families': method_families, 'compound_datatypes': compound_datatypes})
    c.update(csrf(request))

    if request.method == 'POST':
        # FIXME: this is probably a lousy way to handle JSON
        query = request.POST.dict()
        exec('formdata=%s' % query.keys()[0])
        print formdata

        # does Pipeline family with this name already exist?
        if PipelineFamily.objects.filter(name=formdata['family_name']).exists():
            response_data = {'status': 'failure',
                             'error_msg': 'Duplicate pipeline family name'}
            return HttpResponse(json.dumps(response_data), content_type='application/json')

        # make Pipeline family
        pl_family = PipelineFamily(
            name=formdata['family_name'],
            description=formdata['family_desc']
        )
        pl_family.save()

        # make pipeline revision
        pipeline = pl_family.members.create(
            revision_name=formdata['revision_name'],
            revision_desc=formdata['revision_desc']
        )

        # make pipeline inputs
        for key, val in formdata['pipeline_inputs'].iteritems():
            pk = int(val['pk'])
            pipeline.create_input(
                compounddatatype=None if pk < 0 else CompoundDatatype.objects.get(pk=pk),
                dataset_name=val['dataset_name'],
                dataset_idx=val['dataset_idx']
            )

        # make pipeline steps
        for key, val in formdata['pipeline_step'].iteritems():
            pk = val['transformation_pk']  # primary key to CodeResourceRevision
            method = Method.objects.get(pk=pk)
            pipeline_step = pipeline.steps.create(
                transformation=method,
                step_num=int(val['step_num'])
            )
            # add input cables
            for k2, v2 in val['cables_in'].iteritems():
                if v2['source'] == 'Method':
                    source_method = Method.objects.get(pk=v2['source_pk'])
                    pipeline_step.cables_in.create(
                        dest=method.inputs.get(dataset_name=v2['dest_dataset_name']),
                        source_step=int(v2['source_step']),
                        source=source_method.outputs.get(dataset_name=
                                                         v2['source_dataset_name']))
                else:
                    # data from pipeline input (raw or CDT)
                    pipeline_step.cables_in.create(
                        dest=method.inputs.get(dataset_name=v2['dest_dataset_name']),
                        source_step=int(v2['source_step']),
                        source=pl_rev.inputs.get(dataset_name=v2['source_dataset_name'])
                    )

            # add output cables
            for k2, v2 in val['cables_out'].iteritems():
                outcabling = pipeline.create_outcable(
                    source_step=int(v2['source_step']),
                    source=pipeline_step.transformation.outputs.get(dataset_name=v2['dataset_name']),
                    output_name=v2['output_name'],
                    output_idx=v2['output_idx']
                )

        try:
            pipeline.clean()
            pipeline.save()
            response_data = {'status': 'success'}
        except:
            response_data = {'status': 'failure'}

        return HttpResponse(json.dumps(response_data), content_type='application/json')
    else:
        return HttpResponse(t.render(c))


def pipeline_exec(request):
    t = loader.get_template('pipeline/pipeline_exec.html')
    method_families = MethodFamily.objects.all()
    compound_datatypes = CompoundDatatype.objects.all()
    c = Context({'method_families': method_families, 'compound_datatypes': compound_datatypes})
    c.update(csrf(request))
    return HttpResponse(t.render(c))