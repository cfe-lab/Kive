from django.http import HttpResponse, Http404
from django.core import serializers
from django.contrib.auth.decorators import login_required, user_passes_test

from rest_framework.renderers import JSONRenderer

from method.models import CodeResourceRevision, MethodFamily
from metadata.models import KiveUser
from portal.views import developer_check, admin_check
import method.serializers


@login_required
@user_passes_test(developer_check)
def populate_revision_dropdown(request):
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
            response.write(
                serializers.serialize(
                    "json",
                    CodeResourceRevision.filter_by_user(request.user).filter(
                        coderesource__pk=coderesource_id
                    ).order_by("-revision_number"),
                    fields=('pk', 'revision_number', 'revision_name')
                )
            )
        return response
    else:
        raise Http404


@login_required
@user_passes_test(developer_check)
def method_families(request):
    if request.is_ajax():
        response = HttpResponse()
        # Get all families excluding the ones that the administrator can already see.
        if request.QUERY_PARAMS.
        kive_user = KiveUser.kiveify(request.user)

        other_families = MethodFamily.objects.filter(kive_user.access_query())
        mf_serializer = method.serializers.MethodFamilyTableSerializer(other_families, many=True)
        response.write(
            JSONRenderer().render(mf_serializer.data)
        )
        return response
    else:
        raise Http404


@login_required
@user_passes_test(developer_check)
@user_passes_test(admin_check)
def method_family_admin_access(request):
    if request.is_ajax():
        response = HttpResponse()

        # The administrator sees all MethodFamilies.
        other_families = MethodFamily.objects.all()
        mf_serializer = method.serializers.MethodFamilyTableSerializer(other_families, many=True)
        response.write(
            JSONRenderer().render(mf_serializer.data)
        )
        return response
    else:
        raise Http404

