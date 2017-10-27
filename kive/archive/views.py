"""
archive views
"""
import logging

from django.contrib.auth.decorators import login_required
from django.core.urlresolvers import reverse
from django.http import Http404

from archive.models import MethodOutput
from librarian.models import Dataset
from librarian.views import build_download_response, _build_raw_viewer

LOGGER = logging.getLogger(__name__)


@login_required
def stdout_download(request, methodoutput_id):
    """
    Display the standard output associated with the method output in the browser.
    """
    try:
        methodoutput = MethodOutput.objects.get(pk=methodoutput_id)
    except Dataset.DoesNotExist:
        raise Http404("Method output {} cannot be accessed".format(methodoutput_id))

    return build_download_response(methodoutput.output_log)


@login_required
def stdout_view(request, methodoutput_id):
    """
    Display the standard output associated with the method output in the browser.
    """
    return_url = None
    return_to_run = request.GET.get('run_id', None)
    if return_to_run is not None:
        if "view_run" in request.GET:
            return_url = reverse('view_run', kwargs={'run_id': return_to_run})
        else:
            return_url = reverse('view_results', kwargs={'run_id': return_to_run})

    try:
        methodoutput = MethodOutput.objects.get(pk=methodoutput_id)
    except Dataset.DoesNotExist:
        raise Http404("Method output {} cannot be accessed".format(methodoutput_id))

    return _build_raw_viewer(request, methodoutput.output_log, 'Standard out', methodoutput.get_absolute_log_url(),
                             return_url)


@login_required
def stderr_download(request, methodoutput_id):
    """
    Display the standard output associated with the method output in the browser.
    """
    try:
        methodoutput = MethodOutput.objects.get(pk=methodoutput_id)
    except Dataset.DoesNotExist:
        raise Http404("Method output {} cannot be accessed".format(methodoutput_id))

    return build_download_response(methodoutput.error_log)


@login_required
def stderr_view(request, methodoutput_id):
    """
    Display the standard error associated with the method output in the browser.
    """
    return_url = None
    return_to_run = request.GET.get('run_id', None)
    if return_to_run is not None:
        if "view_run" in request.GET:
            return_url = reverse('view_run', kwargs={'run_id': return_to_run})
        else:
            return_url = reverse('view_results', kwargs={'run_id': return_to_run})

    try:
        methodoutput = MethodOutput.objects.get(pk=methodoutput_id)
    except Dataset.DoesNotExist:
        raise Http404("Method output {} cannot be accessed".format(methodoutput_id))

    return _build_raw_viewer(request, methodoutput.error_log, 'Standard error', methodoutput.get_absolute_error_url(),
                             return_url)
