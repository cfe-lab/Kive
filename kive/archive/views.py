"""
archive views
"""
import logging
import mimetypes
import os

from django.contrib.auth.decorators import login_required
from django.core.servers.basehttp import FileWrapper
from django.core.urlresolvers import reverse
from django.http import HttpResponse, Http404

from archive.models import MethodOutput
from librarian.models import Dataset

LOGGER = logging.getLogger(__name__)


def _build_download_response(source_file):
    file_chunker = FileWrapper(source_file)  # Stream file in chunks to avoid overloading memory.
    mimetype = mimetypes.guess_type(source_file.url)[0]
    response = HttpResponse(file_chunker, content_type=mimetype)
    response['Content-Length'] = source_file.size
    response['Content-Disposition'] = 'attachment; filename="{}"'.format(os.path.basename(source_file.name))
    return response


@login_required
def stdout_download(request, methodoutput_id):
    """
    Display the standard output associated with the method output in the browser.
    """
    try:
        methodoutput = MethodOutput.objects.get(pk=methodoutput_id)
    except Dataset.DoesNotExist:
        raise Http404("Method output {} cannot be accessed".format(methodoutput_id))

    return _build_download_response(methodoutput.output_log)


@login_required
def stdout_view(request, methodoutput_id):
    """
    Display the standard output associated with the method output in the browser.
    """
    return_to_run = request.GET.get('run_id', None)
    return_url = None if return_to_run is None else reverse('view_run', kwargs={'run_id': return_to_run})

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

    return _build_download_response(methodoutput.error_log)


@login_required
def stderr_view(request, methodoutput_id):
    """
    Display the standard error associated with the method output in the browser.
    """
    return_to_run = request.GET.get('run_id', None)
    return_url = None if return_to_run is None else reverse('view_run', kwargs={'run_id': return_to_run})

    try:
        methodoutput = MethodOutput.objects.get(pk=methodoutput_id)
    except Dataset.DoesNotExist:
        raise Http404("Method output {} cannot be accessed".format(methodoutput_id))

    return _build_raw_viewer(request, methodoutput.error_log, 'Standard error', methodoutput.get_absolute_error_url(),
                             return_url)
