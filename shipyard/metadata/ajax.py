"""
Handle Ajax transaction requests from metadata templates.
"""

def get_python_type (request):
    """
    Return the lowest-level Python type (string, boolean, int, or
    float) given the Datatype restrictions set by the user.
    """
    if request.is_ajax():
        restricts = request.POST.getlist('restricts')

