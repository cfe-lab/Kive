"""
method.views
"""

from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, Context
from method.models import CodeResource, CodeResourceRevision, CodeResourceDependency, Method
from method.forms import *
#from django.shortcuts import render, render_to_response
from django.core.context_processors import csrf
from django.core.exceptions import ValidationError
from django.forms.util import ErrorList
from datetime import datetime
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile


def resources(request):
    """
    Display a list of all code resources (parents) in database
    """
    resources = CodeResource.objects.filter()

    t = loader.get_template('method/resources.html')
    c = Context({'resources': resources})
    c.update(csrf(request))
    return HttpResponse(t.render(c))



def return_crv_forms(request, exceptions, is_new):
    """
    A helper function for resource_revise() to populate forms with user-submitted values
    and form validation errors to be returned as HttpResponse.
    """
    query = request.POST.dict()
    file_in_memory = request.FILES['content_file']
    if is_new:
        crv_form = CodeResourcePrototypeForm(initial={'revision_name': query['revision_name'],
                                                      'revision_desc': query['revision_desc'],
                                                      'content_file': file_in_memory.name}) # FIXME: this doesn't work
    else:
        crv_form = CodeResourceRevisionForm(initial={'revision_name': query['revision_name'],
                                                     'revision_desc': query['revision_desc']})

    num_dep_forms = sum([1 for k in query.iterkeys() if k.startswith('coderesource_')])
    dep_forms = []
    for i in range(num_dep_forms):
        dep_form = CodeResourceDependencyForm(auto_id='id_%s_'+str(i),
                                              initial={'coderesource': query['coderesource_'+str(i)],
                                                       'revisions': query['revisions_'+str(i)]})
        dep_form.errors['Errors'] = ErrorList(exceptions.get(i, ''))
        dep_forms.append(dep_form)

    return crv_form, dep_forms


def resource_add(request):
    """
    Add a new code resource with a prototype (no revisions).  The FILENAME of the prototype will
    be used as the symbolic filename for all subsequent revisions of this code resource.
    The actual filename will be suffixed with date and time when saved to the filesystem.
    On execution, Shipyard will refer to a revision's CodeResource to get the original filename and
    copy the revision file over to the sandbox.
    NAME provides an opportunity to provide a more intuitive and user-accessible name.
    """
    t = loader.get_template('method/resource_add.html')

    if request.method == 'POST':
        query = request.POST.dict()
        exceptions = {}

        # check if a file has been uploaded
        try:
            file_in_memory = request.FILES['content_file']
        except:
            # no file specified
            crv_form, dep_forms = return_crv_forms(request, exceptions, True)
            crv_form.errors.update({'content_file': u'You must specify a file upload.'})
            c = Context({'resource_form': crv_form, 'dep_forms': dep_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # modify actual filename prior to saving revision object
        file_in_memory.name += '_' + datetime.now().strftime('%Y%m%d%H%M%S')

        # create new CodeResource
        new_code_resource = CodeResource(name=query['revision_name'],
                                         description=query['revision_desc'],
                                         filename=file_in_memory.name)
        try:
            new_code_resource.full_clean()
            new_code_resource.save()
        except ValidationError as e:
            crv_form, dep_forms = return_crv_forms(request, exceptions, True)
            crv_form.errors.update({'revision_name': e.message_dict.get('name', [u''])[0],
                                    'revision_desc': e.message_dict.get('description', [u''])[0]})

            c = Context({'resource_form': crv_form, 'dep_forms': dep_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # modify actual filename prior to saving revision object
        file_in_memory.name += '_' + datetime.now().strftime('%Y%m%d%H%M%S')

        prototype = CodeResourceRevision(revision_name=query['revision_name'],
                                         revision_desc=query['revision_desc'],
                                         coderesource=new_code_resource,
                                         content_file=file_in_memory)
        try:
            prototype.full_clean()
            prototype.save()
        except ValidationError as e:
            new_code_resource.delete()
            crv_form, dep_forms = return_crv_forms(request, exceptions, True)
            crv_form.errors = e.message_dict
            c = Context({'resource_form': crv_form, 'dep_forms': dep_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # bind CR dependencies
        num_dep_forms = sum([1 for k in query.iterkeys() if k.startswith('coderesource_')])
        to_save = []
        for i in range(num_dep_forms):
            on_revision = CodeResourceRevision.objects.get(pk=query['revisions_'+str(i)])
            dependency = CodeResourceDependency(coderesourcerevision=prototype,
                                                requirement = on_revision,
                                                depPath=query['depPath_'+str(i)],
                                                depFileName=query['depFileName_'+str(i)])
            try:
                dependency.full_clean()
                to_save.append(dependency)
            except ValidationError as e:
                exceptions.update({i: e.messages})

        if exceptions:
            new_code_resource.delete()
            prototype.delete() # roll back CodeResourceRevision object
            crv_form, dep_forms = return_crv_forms(request, exceptions, True)

            c = Context({'resource_form': crv_form, 'dep_forms': dep_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # success!
        for dependency in to_save:
            dependency.save()

        return HttpResponseRedirect('/resources')

    else:
        form = CodeResourcePrototypeForm()
        dep_forms = [CodeResourceDependencyForm(auto_id='id_%s_0')]

    c = Context({'resource_form': form, 'dep_forms': dep_forms})
    c.update(csrf(request))
    return HttpResponse(t.render(c))



def resource_revise(request, id):
    """
    Revise a code resource.  The form will initially be populated with values of the last
    revision to this code resource.
    """
    t = loader.get_template('method/resource_revise.html')

    # use POST information (id) to retrieve CodeResource being revised
    this_code_resource = CodeResource.objects.get(pk=id)
    all_revisions = CodeResourceRevision.objects.filter(coderesource=this_code_resource).order_by('-revision_DateTime')
    last_revision = all_revisions[0]

    if request.method == 'POST':
        query = request.POST.dict()
        exceptions = {}

        # check if a file has been uploaded
        try:
            file_in_memory = request.FILES['content_file']
        except:
            # no file specified
            crv_form, dep_forms = return_crv_forms(request, exceptions)
            crv_form._errors['content_file'] = ErrorList([u'You must specify a file upload.'])
            c = Context({'resource_form': crv_form, 'dependency_form': dep_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # modify actual filename prior to saving revision object
        file_in_memory.name += '_' + datetime.now().strftime('%Y%m%d%H%M%S')

        # create CRv object
        revision = CodeResourceRevision(revision_name=query['revision_name'],
                                        revision_desc=query['revision_desc'],
                                        coderesource=this_code_resource,
                                        content_file=file_in_memory)
        try:
            revision.full_clean()
            revision.save()
        except ValidationError as e:
            crv_form, dep_forms = return_crv_forms(request, exceptions)
            crv_form.errors['Errors'] = ErrorList(e.messages)
            c = Context({'resource_form': crv_form, 'dependency_form': dep_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # bind CR dependencies
        num_dep_forms = sum([1 for k in query.iterkeys() if k.startswith('coderesource_')])
        to_save = []
        for i in range(num_dep_forms):
            on_revision = CodeResourceRevision.objects.get(pk=query['revisions_'+str(i)])
            dependency = CodeResourceDependency(coderesourcerevision=revision,
                                                requirement = on_revision,
                                                depPath=query['depPath_'+str(i)],
                                                depFileName=query['depFileName_'+str(i)])
            try:
                dependency.full_clean()
                to_save.append(dependency)
            except ValidationError as e:
                exceptions.update({i: e.messages})

        if exceptions:
            revision.delete() # roll back CodeResourceRevision object
            crv_form, dep_forms = return_crv_forms(request, exceptions, True)

            c = Context({'resource_form': crv_form, 'dep_forms': dep_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # success!
        for dependency in to_save:
            dependency.save()

        return HttpResponseRedirect('/resources')

    else:
        # this CR is being revised
        form = CodeResourceRevisionForm(initial={'revision_desc': last_revision.revision_desc,
                                                 'revision_name': last_revision.revision_name})
        # TODO: populate this with values from last revision's dependencies
        dependencies = last_revision.dependencies.all()
        dep_forms = []
        for i, dependency in enumerate(dependencies):
            its_crv = dependency.coderesourcerevision
            its_cr = its_crv.coderesource
            dep_form = CodeResourceDependencyForm(auto_id='id_%s_'+str(i),
                                                  initial={'coderesource': its_cr.pk,
                                                           'revisions': its_crv.pk,
                                                           'depPath': dependency.depPath,
                                                           'depFileName': dependency.depFileName})
            dep_forms.append(dep_form)

    c = Context({'resource_form': form, 'coderesource': this_code_resource, 'dep_forms': dep_forms})
    c.update(csrf(request))
    return HttpResponse(t.render(c))




def methods(request):
    """
    Display a list of all Methods in database.
    """
    methods = Method.objects.filter(revision_parent=None)
    t = loader.get_template('method/methods.html')
    c = Context({'methods': methods})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def return_method_forms (request, exceptions):
    """
    Helper function for method_add()
    Send HttpResponse Context with forms filled out with previous values including error messages
    """
    query = request.POST.dict()
    family_form = MethodFamilyForm(initial={'name': query['name'],
                                            'description': query['description']})

    method_form = MethodForm(initial={'revision_name': query['revision_name'],
                                      'revision_desc': query['revision_desc'],
                                      'coderesource': query['coderesource'],
                                      'revisions': query['revisions'],
                                      'random': query.has_key('random')})
    num_xput_forms = sum([1 for k in query.iterkeys() if k.startswith('dataset_name_')])
    xput_forms = []
    for i in range(num_xput_forms):
        t_form = TransformationXputForm(auto_id='id_%s_'+str(i),
                                         initial={'dataset_name': query['dataset_name_'+str(i)],
                                                  'input_output': query['input_output_'+str(i)]})
        xs_form = XputStructureForm(auto_id='id_%s_'+str(i),
                                    initial={'compounddatatype': query['compounddatatype_'+str(i)],
                                             'min_row': query['min_row_'+str(i)],
                                             'max_row': query['max_row_'+str(i)]})
        xs_form.errors['Errors'] = exceptions.get(i, '')
        xput_forms.append((t_form, xs_form))

    return family_form, method_form, xput_forms



def method_add (request):
    """
    Generate forms for adding Methods, and validate and process POST data returned
    by the user.  Allows for an arbitrary number of input and output forms.
    """
    t = loader.get_template('method/method_add.html')
    if request.method == 'POST':
        query = request.POST.dict()

        num_xput_forms = sum([1 for k in query.iterkeys() if k.startswith('dataset_name_')])
        exceptions = {}

        # retrieve CodeResource revision as driver
        try:
            coderesource_revision = CodeResourceRevision.objects.get(pk=query['revisions'])
        except:
            family_form, method_form, xput_forms = return_method_forms(request, exceptions)
            c = Context({'family_form': family_form, 'method_form': method_form, 'xput_forms': xput_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))


        # create a new MethodFamily based on completed Family Form
        method_family = MethodFamily(name = query['name'], description = query['description'])
        try:
            method_family.full_clean()
            method_family.save()
        except ValidationError as e:
            family_form, method_form, xput_forms = return_method_forms(request, exceptions)
            family_form.errors['Errors'] = ErrorList(e.messages)
            c = Context({'family_form': family_form, 'method_form': method_form, 'xput_forms': xput_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # attempt to make Method object
        new_method = Method(family = method_family,
                            revision_name=query['revision_name'],
                            revision_desc=query['revision_desc'],
                            driver=coderesource_revision,
                            random=query.has_key('random'))
        try:
            new_method.full_clean()
            new_method.save()
        except ValidationError as e:
            if query['family'] == u'':
                # roll-back newly created MethodFamily
                method_family.delete()
            family_form, method_form, xput_forms = return_method_forms(request, exceptions)
            method_form.errors['Errors'] = ErrorList(e.messages)
            c = Context({'family_form': family_form, 'method_form': method_form, 'xput_forms': xput_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # attempt to make inputs and outputs
        for i in range(num_xput_forms):
            my_compound_datatype = CompoundDatatype.objects.get(pk=query['compounddatatype_'+str(i)])
            min_row = query['min_row_'+str(i)]
            max_row = query['max_row_'+str(i)]
            try:
                if query['input_output_'+str(i)] == 'input':
                    new_input = new_method.create_input(dataset_name = query['dataset_name_'+str(i)],
                                                        dataset_idx = i+1,
                                                        compounddatatype = my_compound_datatype,
                                                        min_row = min_row if min_row else None,
                                                        max_row = max_row if max_row else None)
                else:
                    new_output = new_method.create_output(dataset_name = query['dataset_name_'+str(i)],
                                                    dataset_idx = i+1,
                                                    compounddatatype = my_compound_datatype,
                                                    min_row = min_row if min_row else None,
                                                    max_row = max_row if max_row else None)
            except ValueError as e:
                exceptions.update({i: e.messages})

        if exceptions:
            if query['family'] == u'':
                method_family.delete()
            new_method.delete()
            family_form, method_form, input_forms, output_forms = return_method_forms(request, exceptions)
            c = Context({'family_form': family_form, 'method_form': method_form, 'xput_forms': xput_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # success!
        return HttpResponseRedirect('/methods')

    else:
        # first set of forms
        method_form = MethodForm()
        family_form = MethodFamilyForm()
        xput_forms = [(TransformationXputForm(auto_id='id_%s_0'),
                       XputStructureForm(auto_id='id_%s_0')),
                      (TransformationXputForm(auto_id='id_%s_1', initial={'input_output': 'output'}),
                       XputStructureForm(auto_id='id_%s_1'))]

    c = Context({'family_form': family_form,
                 'method_form': method_form,
                 'xput_forms': xput_forms})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def method_revise(request, id):
    """
    Add a revision of an existing Method.  revision_parent is defined by the
    previous version.
    """
    t = loader.get_template('method/method_revise.html')

    # retrieve the most recent member of this Method's family
    root = Method.objects.filter(pk=id)[0]
    family = root.family
    all_members = Method.objects.filter(family=family).order_by('-id')
    most_recent = all_members[0]

    # retrieve the most recent revision of the corresponding CR
    this_code_resource = most_recent.driver.coderesource
    all_revisions = CodeResourceRevision.objects.filter(coderesource=this_code_resource).order_by('-revision_DateTime')
    last_revision = all_revisions[0]

    if request.method == 'POST':
        query = request.POST.dict()

        num_xput_forms = sum([1 for k in query.iterkeys() if k.startswith('dataset_name_')])
        exceptions = {}

        # retrieve CodeResource revision as driver
        try:
            coderesource_revision = CodeResourceRevision.objects.get(pk=query['revisions'])
        except:
            family_form, method_form, xput_forms = return_method_forms(request, exceptions)
            c = Context({'method_form': method_form, 'xput_forms': xput_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # attempt to make Method object
        new_method = Method(family = family, # same family
                            revision_parent=most_recent,
                            revision_name=query['revision_name'],
                            revision_desc=query['revision_desc'],
                            driver=coderesource_revision,
                            random=query.has_key('random'))
        try:
            new_method.full_clean()
            new_method.save()
        except ValidationError as e:
            family_form, method_form, xput_forms = return_method_forms(request, exceptions)
            method_form.errors['Errors'] = ErrorList(e.messages)
            c = Context({'method_form': method_form, 'xput_forms': xput_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # attempt to make inputs and outputs
        for i in range(num_xput_forms):
            my_compound_datatype = CompoundDatatype.objects.get(pk=query['compounddatatype_'+str(i)])
            min_row = query['min_row_'+str(i)]
            max_row = query['max_row_'+str(i)]
            try:
                if query['input_output_'+str(i)] == 'input':
                    new_input = new_method.create_input(dataset_name = query['dataset_name_'+str(i)],
                                                        dataset_idx = i+1,
                                                        compounddatatype = my_compound_datatype,
                                                        min_row = min_row if min_row else None,
                                                        max_row = max_row if max_row else None)
                else:
                    new_output = new_method.create_output(dataset_name = query['dataset_name_'+str(i)],
                                                    dataset_idx = i+1,
                                                    compounddatatype = my_compound_datatype,
                                                    min_row = min_row if min_row else None,
                                                    max_row = max_row if max_row else None)
            except ValueError as e:
                exceptions.update({i: e.messages})

        if exceptions:
            if query['family'] == u'':
                method_family.delete()
            new_method.delete()
            family_form, method_form, input_forms, output_forms = return_method_forms(request, exceptions)
            c = Context({'method_form': method_form, 'xput_forms': xput_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # success!
        return HttpResponseRedirect('/methods')
    else:
        method_form = MethodReviseForm(initial={'revision_name': most_recent.revision_name,
                                          'revision_desc': most_recent.revision_desc,
                                          'coderesource': this_code_resource.pk,
                                          'revisions': last_revision.pk,
                                          'random': most_recent.random})
        xput_forms = []
        for input in most_recent.inputs.all():
            structure = input.structure.all()[0]
            tx_form = TransformationXputForm(auto_id='id_%s_'+str(len(xput_forms)),
                                            initial={'input_output': 'input',
                                                     'dataset_name': input.dataset_name,
                                                     'dataset_idx': input.dataset_idx})
            xs_form = XputStructureForm(auto_id='id_%s_'+str(len(xput_forms)),
                                        initial={'compounddatatype': structure.compounddatatype,
                                                 'min_row': structure.min_row,
                                                 'max_row': structure.max_row})
            xput_forms.append((tx_form, xs_form))

        for output in most_recent.outputs.all():
            structure = output.structure.all()[0]
            tx_form = TransformationXputForm(auto_id='id_%s_'+str(len(xput_forms)),
                                            initial={'input_output': 'output',
                                                     'dataset_name': output.dataset_name,
                                                     'dataset_idx': output.dataset_idx})
            xs_form = XputStructureForm(auto_id='id_%s_'+str(len(xput_forms)),
                                        initial={'compounddatatype': structure.compounddatatype,
                                                 'min_row': structure.min_row,
                                                 'max_row': structure.max_row})
            xput_forms.append((tx_form, xs_form))

    c = Context({'method_form': method_form, 'xput_forms': xput_forms})
    c.update(csrf(request))
    return HttpResponse(t.render(c))
