def code_resource_revision_post_delete(instance, **kwargs):
    """Remove a CodeResourceRevision from the file system after it is deleted."""
    if instance.content_file:
        instance.content_file.delete(save=False)
