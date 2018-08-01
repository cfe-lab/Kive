# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib.auth.decorators import login_required, user_passes_test
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.views.generic.base import TemplateView
from django.views.generic.edit import CreateView, UpdateView

from container.forms import ContainerFamilyForm
from container.models import ContainerFamily
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
