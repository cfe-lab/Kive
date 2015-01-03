def dataset_post_delete(instance, **kwargs):
    """Remove a Dataset from the file system after it is deleted."""
    if instance.dataset_file:
        instance.dataset_file.delete(save=False)
