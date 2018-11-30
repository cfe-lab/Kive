# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os
from io import BytesIO

import errno
from django.conf import settings
from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import Http404
from django.template.defaultfilters import filesizeformat
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.views.generic.base import TemplateView
from django.views.generic.edit import CreateView, UpdateView, ModelFormMixin
import rest_framework.reverse

from container.forms import ContainerFamilyForm, ContainerForm, \
    ContainerUpdateForm, ContainerAppForm, ContainerRunForm, BatchForm
from container.models import ContainerFamily, Container, ContainerApp, \
    ContainerRun, ContainerArgument
from file_access_utils import compute_md5
from portal.views import developer_check, AdminViewMixin

decorators = [login_required, user_passes_test(developer_check)]


@method_decorator(decorators, name='dispatch')
class ContainerFamilyList(TemplateView, AdminViewMixin):
    template_name = 'container/containerfamily_list.html'


@method_decorator(decorators, name='dispatch')
class ContainerFamilyUpdate(UpdateView, AdminViewMixin):
    model = ContainerFamily
    form_class = ContainerFamilyForm

    def form_valid(self, form):
        response = super(ContainerFamilyUpdate, self).form_valid(form)
        self.object.grant_from_json(form.cleaned_data["permissions"])
        return response

    def get_success_url(self):
        return reverse('container_families')


@method_decorator(decorators, name='dispatch')
class ContainerFamilyCreate(CreateView):
    model = ContainerFamily
    form_class = ContainerFamilyForm

    def form_valid(self, form):
        form.instance.user = self.request.user
        response = super(ContainerFamilyCreate, self).form_valid(form)
        self.object.grant_from_json(form.cleaned_data["permissions"])
        return response

    def get_success_url(self):
        return reverse('container_families')


@method_decorator(decorators, name='dispatch')
class ContainerCreate(CreateView, AdminViewMixin):
    model = Container
    form_class = ContainerForm

    def form_valid(self, form):
        form.instance.user = self.request.user
        form.instance.family_id = self.kwargs['family_id']

        # We need to get a file object to validate. We might have a path or we
        # might have to read the data out of memory.
        container_file = form.files['file']
        if hasattr(container_file, 'temporary_file_path'):
            with open(container_file.temporary_file_path()) as f:
                md5 = compute_md5(f)
        else:
            if hasattr(container_file, 'read'):
                f = BytesIO(container_file.read())
            else:
                f = BytesIO(container_file['content'])
            md5 = compute_md5(f)
            if hasattr(container_file, 'seek') and callable(container_file.seek):
                container_file.seek(0)
        form.instance.md5 = md5

        response = super(ContainerCreate, self).form_valid(form)
        self.object.grant_from_json(form.cleaned_data["permissions"])
        return response

    def get_success_url(self):
        return reverse('container_family_update',
                       kwargs=dict(pk=self.object.family_id))

    def get_context_data(self, **kwargs):
        context = super(ContainerCreate, self).get_context_data(**kwargs)
        context['family_id'] = self.kwargs['family_id']
        return context

    def get_form_kwargs(self):
        kwargs = super(ContainerCreate, self).get_form_kwargs()
        access_limits = kwargs.setdefault('access_limits', [])
        access_limits.append(
            ContainerFamily.objects.get(pk=self.kwargs['family_id']))
        return kwargs


@method_decorator(decorators, name='dispatch')
class ContainerUpdate(UpdateView, AdminViewMixin):
    model = Container
    form_class = ContainerUpdateForm

    def form_valid(self, form):
        response = super(ContainerUpdate, self).form_valid(form)
        self.object.grant_from_json(form.cleaned_data["permissions"])
        return response

    def get_success_url(self):
        return reverse('container_family_update',
                       kwargs=dict(pk=self.object.family_id))

    def get_context_data(self, **kwargs):
        context = super(ContainerUpdate, self).get_context_data(**kwargs)
        context['family_id'] = self.object.family_id
        context['download_url'] = reverse('container-download',
                                          kwargs=dict(pk=self.object.pk))
        context['file_name'] = self.object.file and os.path.relpath(
            self.object.file.name,
            Container.UPLOAD_DIR)
        try:
            file_size = filesizeformat(self.object.file.size)
        except OSError as ex:
            if ex.errno != errno.ENOENT:
                raise
            file_size = 'missing'
        context['file_size'] = file_size
        return context

    def get_form_kwargs(self):
        kwargs = super(ContainerUpdate, self).get_form_kwargs()
        access_limits = kwargs.setdefault('access_limits', [])
        access_limits.append(self.object.family)
        return kwargs


class ArgumentWriterMixin(ModelFormMixin):
    def __init__(self, *args, **kwargs):
        super(ArgumentWriterMixin, self).__init__(*args, **kwargs)

    def form_valid(self, form):
        response = super(ArgumentWriterMixin, self).form_valid(form)
        try:
            form.instance.write_inputs(form.cleaned_data['inputs'])
        except ValueError as ex:
            form.add_error('inputs', ex.message)
            response = super(ArgumentWriterMixin, self).form_invalid(form)
        try:
            form.instance.write_outputs(form.cleaned_data['outputs'])
        except ValueError as ex:
            form.add_error('outputs', ex.message)
            response = super(ArgumentWriterMixin, self).form_invalid(form)
        return response


@method_decorator(decorators, name='dispatch')
class ContainerAppCreate(CreateView, ArgumentWriterMixin):
    model = ContainerApp
    form_class = ContainerAppForm

    def form_valid(self, form):
        form.instance.container_id = self.kwargs['container_id']
        return super(ContainerAppCreate, self).form_valid(form)

    def get_success_url(self):
        return reverse('container_update',
                       kwargs=dict(pk=self.object.container_id))

    def get_context_data(self, **kwargs):
        context = super(ContainerAppCreate, self).get_context_data(**kwargs)
        context['container_id'] = self.kwargs['container_id']
        return context


@method_decorator(decorators, name='dispatch')
class ContainerAppUpdate(UpdateView, ArgumentWriterMixin, AdminViewMixin):
    model = ContainerApp
    form_class = ContainerAppForm

    def get_success_url(self):
        return reverse('container_update',
                       kwargs=dict(pk=self.object.container_id))

    def get_context_data(self, **kwargs):
        context = super(ContainerAppUpdate, self).get_context_data(**kwargs)
        context['container_id'] = self.object.container_id

        form = context['form']
        form.initial['inputs'] = self.object.inputs
        form.initial['outputs'] = self.object.outputs
        return context


@method_decorator(decorators, name='dispatch')
class ContainerChoiceList(TemplateView, AdminViewMixin):
    template_name = 'container/containerchoice_list.html'


@method_decorator(decorators, name='dispatch')
class ContainerInputList(TemplateView, AdminViewMixin):
    template_name = 'container/containerinput_list.html'

    def get_context_data(self, **kwargs):
        app_id = int(self.request.GET.get("app"))
        visible_containers = Container.filter_by_user(self.request.user)
        app_qs = ContainerApp.objects.filter(pk=app_id,
                                             container__in=visible_containers)

        app = app_qs.first()
        if app is None:
            raise Http404("ID {} is not accessible".format(app_id))
        context = super(ContainerInputList, self).get_context_data(**kwargs)
        context['app'] = app
        context['app_url'] = rest_framework.reverse.reverse('containerapp-detail',
                                                            kwargs=dict(pk=app.pk),
                                                            request=self.request)
        context['batch_form'] = BatchForm()
        context['inputs'] = [
            dict(name=arg.name, url=rest_framework.reverse.reverse('containerargument-detail',
                                                                   kwargs=dict(pk=arg.pk),
                                                                   request=self.request))
            for arg in app.arguments.filter(
                type=ContainerArgument.INPUT).order_by('position')]
        context['priolist'] = [t[0] for t in settings.SLURM_QUEUES]
        return context


@method_decorator(decorators, name='dispatch')
class ContainerRunList(TemplateView, AdminViewMixin):
    template_name = 'container/containerrun_list.html'


@method_decorator(decorators, name='dispatch')
class ContainerRunCreate(CreateView, AdminViewMixin):
    model = ContainerRun
    form_class = ContainerRunForm

    def form_valid(self, form):
        form.instance.user = self.request.user
        response = super(ContainerRunCreate, self).form_valid(form)
        return response

    def form_invalid(self, form):
        response = super(ContainerRunCreate, self).form_invalid(form)
        return response

    def get_success_url(self):
        return reverse('container_runs')
