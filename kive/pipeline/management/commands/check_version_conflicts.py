import os

from django.core.management import BaseCommand

from pipeline.models import Pipeline


class Command(BaseCommand):
    help = "Checks for code resource revision conflicts in a pipeline."

    def handle(self, **kwargs):
        for pipeline in Pipeline.objects.all():
            print(pipeline)
            self.check_pipeline(pipeline)

    def check_pipeline(self, pipeline):
        revision_ids = {}  # {file_path: revision_id}
        for step in pipeline.steps.all():
            method = step.transformation.definite
            code_resource_revision = method.driver
            install_path = code_resource_revision.coderesource.filename
            self.check_revision(code_resource_revision,
                                install_path,
                                revision_ids,
                                step)
            for dependency in method.dependencies.all():
                code_resource_revision = dependency.requirement
                install_path = os.path.join(dependency.path,
                                            dependency.get_filename())
                self.check_revision(code_resource_revision,
                                    install_path,
                                    revision_ids,
                                    step)

    def check_revision(self, revision, install_path, revision_ids, step):
        old_revision_id = revision_ids.get(install_path)
        if old_revision_id is not None and old_revision_id != revision.id:
            raise RuntimeError(
                'Found version conflict at {} for step {} of {}.'.format(
                    install_path,
                    step,
                    step.pipeline))
        revision_ids[install_path] = revision.id
