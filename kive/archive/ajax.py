from django.db.models import Q
from rest_framework import permissions, status
from rest_framework.decorators import detail_route
from rest_framework.exceptions import APIException
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from archive.forms import DatasetForm
from archive.serializers import DatasetSerializer, MethodOutputSerializer
from archive.models import Dataset, MethodOutput, summarize_redaction_plan
from archive.views import _build_download_response

from kive.ajax import RemovableModelViewSet, RedactModelMixin, IsGrantedReadOnly, IsGrantedReadCreate,\
    StandardPagination

from librarian.models import SymbolicDataset


JSON_CONTENT_TYPE = 'application/json'

class DatasetViewSet(RemovableModelViewSet, RedactModelMixin):
    """ List and modify datasets.
    
    POST to the list to upload a new dataset, DELETE an instance to remove it
    along with all runs that produced or consumed it, or PATCH is_redacted=true
    on an instance to blank its contents along with any other instances or logs
    that used it as input.
    
    Query parameters for the list view:
    * page_size=n - limit the results and page through them
    * is_granted=true - For administrators, this limits the list to only include
        records that the user has been explicitly granted access to. For other
        users, this has no effect.
    * filters[n][key]=x&filters[n][val]=y - Apply different filters to the
        search. n starts at 0 and increases by 1 for each added filter.
        Some filters just have a key and ignore the val value. The possible
        filters are listed below.
    * filters[n][key]=smart&filters[n][val]=match - name or description contain
        the value (case insensitive)
    * filters[n][key]=name&filters[n][val]=match - name contains the value (case
        insensitive)
    * filters[n][key]=uploaded - only include datasets uploaded by users, not
        generated by pipeline runs.
    * filters[n][key]=cdt&filters[n][val]=id - only include datasets with the
        compound datatype id, or raw type if id is missing.
    """
    queryset = Dataset.objects.all()
    serializer_class = DatasetSerializer
    permission_classes = (permissions.IsAuthenticated, IsGrantedReadCreate)
    pagination_class = StandardPagination

    def filter_granted(self, queryset):
        """ Filter a queryset to only include records explicitly granted.
        """
        return queryset.filter(
            symbolicdataset__in=SymbolicDataset.filter_by_user(self.request.user))

    def filter_queryset(self, queryset):
        queryset = super(DatasetViewSet, self).filter_queryset(queryset)
        i = 0
        while True:
            key = self.request.GET.get('filters[{}][key]'.format(i))
            if key is None:
                break
            value = self.request.GET.get('filters[{}][val]'.format(i), '')
            queryset = self._add_filter(queryset, key, value)
            i += 1
        return queryset
    
    def _add_filter(self, queryset, key, value):
        if key == 'smart':
            return queryset.filter(Q(name__icontains=value) |
                                   Q(description__icontains=value))
        if key == 'name':
            return queryset.filter(name__icontains=value)
        if key == 'uploaded':
            return queryset.filter(created_by=None)
        if key == 'cdt':
            if value == '':
                return queryset.filter(symbolicdataset__structure__isnull=True)
            else:
                return queryset.filter(
                    symbolicdataset__structure__compounddatatype=value)
        raise APIException('Unknown filter key: {}'.format(key))

    def create(self, request):
        """
        Override the create function, this allows us to POST to
        this viewset, but also provides us with an incorrect form on
        the front end.
        """
        single_dataset_form = DatasetForm(request.POST, request.FILES, user=request.user, prefix="")
        symdataset = single_dataset_form.create_dataset(request.user) if single_dataset_form.is_valid() else None

        if symdataset is None:
            return Response({'errors': single_dataset_form.errors}, status=400)
        return Response(DatasetSerializer(symdataset.dataset, context={'request': request}).data,  status=201)

    def patch_object(self, request, pk=None):
        return Response(DatasetSerializer(self.get_object(), context={'request': request}).data)

    @detail_route(methods=['get'])
    def download(self, request, pk=None):
        """
        """
        accessible_SDs = SymbolicDataset.filter_by_user(request.user)
        dataset = self.get_object()

        if dataset.symbolicdataset not in accessible_SDs:
            return Response(None, status=status.HTTP_404_NOT_FOUND)

        return _build_download_response(dataset.dataset_file)


class MethodOutputViewSet(ReadOnlyModelViewSet):
    """ List and redact method output records.
    
    PATCH output_redacted=true, error_redacted=true, or code_redacted=true on an
    instance to blank its output log, error log, or return code.
    """
    queryset = MethodOutput.objects.all()
    serializer_class = MethodOutputSerializer
    permission_classes = (permissions.IsAuthenticated, IsGrantedReadOnly)

    def patch_object(self, request, pk=None):
        return Response(MethodOutputSerializer(
            self.get_object(),
            context={'request': request}).data)

    def partial_update(self, request, pk=None):
        method_output = self.get_object()
        redactions = {'output_redacted': method_output.redact_output_log,
                      'error_redacted': method_output.redact_error_log,
                      'code_redacted': method_output.redact_return_code}
        
        unexpected_keys = set(request.DATA.keys()) - set(redactions.keys())
        if unexpected_keys:
            return Response(
                {'errors': ['Cannot update fields ' + ','.join(unexpected_keys)]},
                status=status.HTTP_400_BAD_REQUEST)
        for field, redact in redactions.iteritems():
            if request.DATA.get(field, False):
                redact()
        return self.patch_object(request, pk)

    @detail_route(methods=['get'])
    def output_redaction_plan(self, request, pk=None):
        execlog = self.get_object().execlog
        redaction_plan = execlog.build_redaction_plan(output_log=True,
                                                      error_log=False,
                                                      return_code=False)
        return Response(summarize_redaction_plan(redaction_plan))

    @detail_route(methods=['get'])
    def error_redaction_plan(self, request, pk=None):
        execlog = self.get_object().execlog
        redaction_plan = execlog.build_redaction_plan(output_log=False,
                                                      error_log=True,
                                                      return_code=False)
        return Response(summarize_redaction_plan(redaction_plan))

    @detail_route(methods=['get'])
    def code_redaction_plan(self, request, pk=None):
        execlog = self.get_object().execlog
        redaction_plan = execlog.build_redaction_plan(output_log=False,
                                                      error_log=False,
                                                      return_code=True)
        return Response(summarize_redaction_plan(redaction_plan))

