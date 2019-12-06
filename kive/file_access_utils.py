"""
Basic file-checking functionality used by Kive.
"""

import hashlib
import mimetypes
import os
from contextlib import contextmanager

from django.http import FileResponse


def build_download_response(field_file):
    # Intentionally leave this open for streaming response.
    # FileResponse will close it when streaming finishes.
    field_file.open('rb')

    mimetype = mimetypes.guess_type(field_file.name)[0]
    response = FileResponse(field_file, content_type=mimetype)
    response['Content-Length'] = field_file.size
    response['Content-Disposition'] = 'attachment; filename="{}"'.format(
        os.path.basename(field_file.name))
    return response


def compute_md5(file_to_checksum, chunk_size=1024*64):
    """Computes MD5 checksum of specified file.

    file_to_checksum should be an open, readable, file handle, with
    its position at the beginning, i.e. so that .read() gets the
    entire contents of the file.
    NOTE: under python3, the file should have been open in binary mode ("rb")
    so that bytes (not strings) are returned when iterating over the file.
    """
    md5gen = hashlib.md5()
    while True:
        chunk = file_to_checksum.read(chunk_size)
        if not chunk:
            return md5gen.hexdigest()
        md5gen.update(chunk)


@contextmanager
def use_field_file(field_file, mode='rb'):
    """ Context manager for FieldFile objects.

    Tries to leave a file object in the same state it was in when the context
    manager started.
    It's hard to tell when to close a FieldFile object. It opens implicitly
    when you first read from it. Sometimes, it's an in-memory file object, and
    it can't be reopened.
    """
    was_closed = field_file.closed
    field_file.open(mode)
    start_position = field_file.tell()
    try:
        yield field_file
    finally:
        if was_closed:
            field_file.close()
        else:
            field_file.seek(start_position)
