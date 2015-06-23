"""
pipeline views
"""

from django.http import HttpResponse, Http404
from django.template import loader, RequestContext
from django.contrib.auth.decorators import login_required, user_passes_test

import json
import logging

from method.models import MethodFamily
from metadata.models import CompoundDatatype
from pipeline.models import Pipeline, PipelineFamily, PipelineSerializationException
import metadata.forms
from portal.views import developer_check, admin_check
from pipeline.serializers import PipelineFamilySerializer

logger = logging.getLogger(__name__)


@login_required
@user_passes_test(developer_check)
def pipelines(request):
    """
    Display existing pipeline families, represented by the
    root members (without parent).
    """
    t = loader.get_template('pipeline/pipelines.html')
    families = PipelineFamily.filter_by_user(request.user)
    families_json = json.dumps(
        PipelineFamilySerializer(
            families,
            context={"request": request},
            many=True).data
    )

    c = RequestContext(
        request,
        {
            'pipeline_families': families_json,
            "is_user_admin": admin_check(request.user)
        })
    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def pipeline_exec(request):
    t = loader.get_template('pipeline/pipeline_exec.html')
    method_families = MethodFamily.filter_by_user(request.user)
    compound_datatypes = CompoundDatatype.filter_by_user(request.user)
    c = RequestContext(request, {'method_families': method_families, 'compound_datatypes': compound_datatypes})
    return HttpResponse(t.render(c))

