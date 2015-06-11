def staged_file_post_delete(instance, **kwargs):
    """Remove a staged file from the file system after it is deleted."""
    if instance.uploaded_file:
        instance.uploaded_file.delete(save=False)
