def dataset_post_delete(instance, **kwargs):
    """Remove a Dataset from the file system after it is deleted."""
    if instance.dataset_file:
        instance.dataset_file.delete(save=False)


def methodoutput_post_delete(instance, **kwargs):
    """Remove output and error logs from the file system after a MethodOutput is deleted."""
    if instance.output_log:
        instance.output_log.delete(save=False)
    if instance.error_log:
        instance.error_log.delete(save=False)
