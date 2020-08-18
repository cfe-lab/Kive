from django import template
from django.template.defaultfilters import stringfilter

register = template.Library()


@register.filter
@stringfilter
def get_view_link(download_link):
    """Transform a download link into the equivalent view link."""
    return download_link.replace("download", "view")
