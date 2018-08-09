# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os
from io import BytesIO

from django.contrib.auth.decorators import login_required, user_passes_test
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.views.generic.base import TemplateView
from django.views.generic.edit import CreateView, UpdateView

from container.forms import ContainerFamilyForm, ContainerForm, ContainerUpdateForm
from container.models import ContainerFamily, Container
from file_access_utils import compute_md5
from portal.views import admin_check, developer_check

decorators = [login_required, user_passes_test(developer_check)]


@method_decorator(decorators, name='dispatch')
class ContainerFamilyList(TemplateView):
    template_name = 'container/containerfamily_list.html'

    def get_context_data(self, **kwargs):
        context = super(ContainerFamilyList, self).get_context_data(**kwargs)
        context['is_user_admin'] = admin_check(self.request.user)
        return context


@method_decorator(decorators, name='dispatch')
class ContainerFamilyUpdate(UpdateView):
    model = ContainerFamily
    form_class = ContainerFamilyForm

    def form_valid(self, form):
        response = super(ContainerFamilyUpdate, self).form_valid(form)
        self.object.grant_from_json(form.cleaned_data["permissions"])
        return response

    def get_success_url(self):
        return reverse('container_families')

    def get_context_data(self, **kwargs):
        context = super(ContainerFamilyUpdate, self).get_context_data(**kwargs)
        context['is_user_admin'] = admin_check(self.request.user)
        return context


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
class ContainerCreate(CreateView):
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
        context['is_user_admin'] = admin_check(self.request.user)
        context['family_id'] = self.kwargs['family_id']
        return context

    def get_form_kwargs(self):
        kwargs = super(ContainerCreate, self).get_form_kwargs()
        access_limits = kwargs.setdefault('access_limits', [])
        access_limits.append(
            ContainerFamily.objects.get(pk=self.kwargs['family_id']))
        return kwargs


@method_decorator(decorators, name='dispatch')
class ContainerUpdate(UpdateView):
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
        context['is_user_admin'] = admin_check(self.request.user)
        context['family_id'] = self.object.family_id
        context['download_url'] = reverse('container-download',
                                          kwargs=dict(pk=self.object.pk))
        context['file_name'] = self.object.file and os.path.relpath(
            self.object.file.name,
            Container.UPLOAD_DIR)
        return context

    def get_form_kwargs(self):
        kwargs = super(ContainerUpdate, self).get_form_kwargs()
        access_limits = kwargs.setdefault('access_limits', [])
        access_limits.append(self.object.family)
        return kwargs
