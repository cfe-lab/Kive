"""
method.views
"""

from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, Context
from method.models import CodeResource, CodeResourceRevision, CodeResourceDependency, Method
from method.forms import *
from transformation.models import *
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
    resources = CodeResource.objects.all()

    t = loader.get_template('method/resources.html')
    c = Context({'resources': resources})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def resource_revisions(request, id):
    """
    Display a list of all revisions of a specific Code Resource in database.
    """
    coderesource = CodeResource.objects.get(pk=id)
    revisions = CodeResourceRevision.objects.filter(coderesource=coderesource).order_by('-revision_number')
    t = loader.get_template('method/resource_revisions.html')
    c = Context({'coderesource': coderesource, 'revisions': revisions})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def return_crv_forms(request, exceptions, is_new):
    """
    A helper function for resource_revise() to populate forms with user-submitted values
    and form validation errors to be returned as HttpResponse.
    NOTE: cannot set default value of FileField due to security.
    """
    print exceptions
    query = request.POST.dict()
    if is_new:
        # creating a new CodeResource
        crv_form = CodeResourcePrototypeForm(initial={'resource_name': query['resource_name'],
                                                      'resource_desc': query['resource_desc']})
        crv_form.errors.update({'content_file': exceptions.get('content_file', ''),
                                'resource_name': exceptions.get('name', ''),
                                'resource_desc': exceptions.get('description', '')})
    else:
        # revising a code resource
        crv_form = CodeResourceRevisionForm(initial={'revision_name': query['revision_name'],
                                                     'revision_desc': query['revision_desc']})
        crv_form.errors.update({'content_file': exceptions.get('content_file', ''),
                                'revision_name': exceptions.get('revision_name', ''),
                                'revision_desc': exceptions.get('revision_desc', '')})

    num_dep_forms = sum([1 for k in query.iterkeys() if k.startswith('coderesource_')])
    dep_forms = []
    for i in range(num_dep_forms):
        if query['coderesource_'+str(i)]:
            dep_form = CodeResourceDependencyForm(auto_id='id_%s_'+str(i),
                                                  initial={'coderesource': query['coderesource_'+str(i)],
                                                           'revisions': query['revisions_'+str(i)],
                                                           'depPath': query['depPath_'+str(i)],
                                                           'depFileName': query['depFileName_'+str(i)]})
        else:
            dep_form = CodeResourceDependencyForm(auto_id='id_%s_'+str(i))
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
        new_code_resource = None
        prototype = None

        try:
            try:
                file_in_memory = request.FILES['content_file']
            except:
                exceptions.update({'content_file': 'You must specify a file upload.'})
                raise  # content_file required for next steps

            try:
                new_code_resource = CodeResource(name=query['resource_name'],
                                                 description=query['resource_desc'],
                                                 filename=file_in_memory.name)
                new_code_resource.full_clean()
                new_code_resource.save()
            except ValidationError as e:
                for key, msg in e.message_dict.iteritems():
                    exceptions.update({key: str(msg[0])})
                raise  # CodeResource object required for next steps

            # modify actual filename prior to saving revision object
            file_in_memory.name += '_' + datetime.now().strftime('%Y%m%d%H%M%S')

            try:
                prototype = CodeResourceRevision(revision_name='Prototype', revision_desc=query['resource_desc'],
                        coderesource=new_code_resource, content_file=file_in_memory)
                prototype.full_clean()
                prototype.save()
            except ValidationError as e:
                for key, msg in e.message_dict.iteritems():
                    exceptions.update({key: str(msg[0])})
                raise  # CodeResourceRevision object required for next steps

            # bind CR dependencies
            num_dep_forms = sum([1 for k in query.iterkeys() if k.startswith('coderesource_')])
            to_save = []
            for i in range(num_dep_forms):
                this_cr = query['coderesource_'+str(i)]  # PK of CodeResource
                if this_cr == '':
                    # ignore blank CR dependency forms
                    continue
                try:
                    on_revision = CodeResourceRevision.objects.get(pk=query['revisions_'+str(i)])
                    dependency = CodeResourceDependency(coderesourcerevision=prototype,
                                                    requirement = on_revision,
                                                    depPath=query['depPath_'+str(i)],
                                                    depFileName=query['depFileName_'+str(i)])
                    dependency.full_clean()
                    to_save.append(dependency)
                except ValidationError as e:
                    exceptions.update({i: e.messages})

        except:
            if hasattr(new_code_resource, 'id') and new_code_resource.id is not None:
                new_code_resource.delete()
            if hasattr(prototype, 'id') and prototype.id is not None:
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


def resource_revision_add(request, id):
    """
    Add a code resource revision.  The form will initially be populated with values of the last
    revision to this code resource.
    """
    t = loader.get_template('method/resource_revision_add.html')

    # use POST information (id) to retrieve CRv being revised
    parent_revision = CodeResourceRevision.objects.get(pk=id)
    coderesource = parent_revision.coderesource

    if request.method == 'POST':
        query = request.POST.dict()
        exceptions = {}
        revision = None

        try:
            try:
                file_in_memory = request.FILES['content_file']
                # modify actual filename prior to saving revision object
                file_in_memory.name += datetime.now().strftime('_%Y%m%d%H%M%S')
            except:
                exceptions.update({'content_file': 'You must specify a file upload.'})
                raise  # content_file required for next steps

            # is this file identical to another CodeResourceRevision?
            try:
                revision = CodeResourceRevision(revision_parent=parent_revision,
                                                revision_name=query['revision_name'],
                                                revision_desc=query['revision_desc'],
                                                coderesource=coderesource,
                                                content_file=file_in_memory)
                revision.full_clean()
                revision.save()
            except ValidationError as e:
                for key, msg in e.message_dict.iteritems():
                    exceptions.update({key: str(msg[0])})
                raise  # CodeResourceRevision object required for next steps

            # bind CR dependencies
            num_dep_forms = sum([1 for k in query.iterkeys() if k.startswith('coderesource_')])
            to_save = []
            for i in range(num_dep_forms):
                crv_id = query['revisions_'+str(i)]
                if crv_id == '':
                    # blank form, ignore
                    continue
                try:
                    on_revision = CodeResourceRevision.objects.get(pk=crv_id)
                    dependency = CodeResourceDependency(coderesourcerevision=revision,
                                                        requirement = on_revision,
                                                        depPath=query['depPath_'+str(i)],
                                                        depFileName=query['depFileName_'+str(i)])
                    dependency.full_clean()
                    to_save.append(dependency)
                except ValidationError as e:
                    exceptions.update({i: e.messages})

            if len(exceptions) > 0:
                # one or more ValidationErrors were raised
                raise

            # success!
            for dependency in to_save:
                dependency.save()

            return HttpResponseRedirect('/resources')

        except:
            if hasattr(revision, 'id') and revision.id is not None:
                revision.delete()

            crv_form, dep_forms = return_crv_forms(request, exceptions, False)
            # fall through to return statement below

    else:
        # this CR is being revised
        crv_form = CodeResourceRevisionForm()

        # TODO: do not allow CR to depend on itself
        dependencies = parent_revision.dependencies.all()
        dep_forms = []
        for i, dependency in enumerate(dependencies):
            its_crv = dependency.requirement
            its_cr = its_crv.coderesource
            if its_cr:
                dep_form = CodeResourceDependencyForm(auto_id='id_%s_'+str(i),
                                                      initial={'coderesource': its_cr.pk,
                                                               'revisions': its_crv.pk,
                                                               'depPath': dependency.depPath,
                                                               'depFileName': dependency.depFileName},
                                                      parent=coderesource.id)
            else:
                dep_form = CodeResourceDependencyForm(auto_id='id_%s_'+str(i))
            dep_forms.append(dep_form)

        # in case the parent revision has no CR dependencies, add a blank form
        if len(dep_forms) == 0:
            dep_forms.append(CodeResourceDependencyForm(auto_id='id_%s_0',
                                                        parent=coderesource.id))

    c = Context({'resource_form': crv_form, 'parent_revision': parent_revision,
                 'coderesource': coderesource, 'dep_forms': dep_forms})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def method_families(request):
    """
    Display a list of all MethodFamily objects in database.
    """
    families = MethodFamily.objects.all()
    t = loader.get_template('method/method_families.html')
    c = Context({'families': families})
    c.update(csrf(request))
    return HttpResponse(t.render(c))

def methods(request, id):
    """
    Display a list of all Methods within a given MethodFamily.
    """
    family = MethodFamily.objects.filter(pk=id)[0]
    its_methods = Method.objects.filter(family=family)

    t = loader.get_template('method/methods.html')
    c = Context({'methods': its_methods, 'family': family})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def return_method_forms (request, exceptions):
    """
    Helper function for method_add()
    Send HttpResponse Context with forms filled out with previous values including error messages
    """
    query = request.POST.dict()

    if 'name' in query:
        family_form = MethodFamilyForm(initial={'name': query['name'],
                                                'description': query['description']})
    else:
        family_form = MethodFamilyForm()

    for key, msg in exceptions.iteritems():
        family_form.errors.update({key: msg})

    # populate main form with submitted values
    method_form = MethodForm(initial={'revision_name': query['revision_name'],
                                      'revision_desc': query['revision_desc'],
                                      'coderesource': query['coderesource'],
                                      'revisions': query['revisions'],
                                      'deterministic': query.has_key('deterministic')})

    for key, msg in exceptions.iteritems():
        method_form.errors.update({key: msg})

    # populate input forms with submitted values
    num_input_forms = sum([1 for k in query.iterkeys() if k.startswith('dataset_name_in_')])
    input_forms = []
    for i in range(num_input_forms):
        t_form = TransformationXputForm(auto_id='id_%s_in_'+str(i),
                                         initial={'dataset_name': query['dataset_name_in_'+str(i)]})
        if i in exceptions['inputs'] and 'dataset_name' in exceptions['inputs'][i]:
            t_form.errors.update({'dataset_name': exceptions['inputs'][i]['dataset_name']})

        xs_form = XputStructureForm(auto_id='id_%s_in_'+str(i),
                                    initial={'compounddatatype': query['compounddatatype_in_'+str(i)],
                                             'min_row': query['min_row_in_'+str(i)],
                                             'max_row': query['max_row_in_'+str(i)]})
        if i in exceptions['inputs'] and 'compounddatatype' in exceptions['inputs'][i]:
            xs_form.errors.update({'compounddatatype': exceptions['inputs'][i]['compounddatatype']})

        input_forms.append((t_form, xs_form))

    # populate output forms with submitted values
    num_output_forms = sum([1 for k in query.iterkeys() if k.startswith('dataset_name_out_')])
    output_forms = []
    for i in range(num_output_forms):
        t_form = TransformationXputForm(auto_id='id_%s_out_'+str(i),
                                         initial={'dataset_name': query['dataset_name_out_'+str(i)]})
        if i in exceptions['outputs'] and 'dataset_name' in exceptions['outputs'][i]:
            t_form.errors.update({'dataset_name': exceptions['outputs'][i]['dataset_name']})

        xs_form = XputStructureForm(auto_id='id_%s_out_'+str(i),
                                    initial={'compounddatatype': query['compounddatatype_out_'+str(i)],
                                             'min_row': query['min_row_out_'+str(i)],
                                             'max_row': query['max_row_out_'+str(i)]})
        if i in exceptions['outputs'] and 'compounddatatype' in exceptions['outputs'][i]:
            xs_form.errors.update({'compounddatatype': exceptions['outputs'][i]['compounddatatype']})

        output_forms.append((t_form, xs_form))

    return family_form, method_form, input_forms, output_forms


def method_add(request, id=None):
    """
    Generate forms for adding Methods, and validate and process POST data returned
    by the user.  Allows for an arbitrary number of input and output forms.

    [id] : User is not creating a new MethodFamily, but adding to an existing family
            without a specified parent Method (different CodeResource)
            If ID is not specified, then user is creating a new MethodFamily.
    """
    if id:
        this_family = MethodFamily.objects.get(pk=id)
        header = "Add a new Method to MethodFamily '%s'" % this_family.name
    else:
        this_family = None
        header = 'Start a new MethodFamily with an initial Method'

    t = loader.get_template('method/method_add.html')
    if request.method == 'POST':
        query = request.POST.dict()

        num_input_forms = sum([1 for k in query.iterkeys() if k.startswith('dataset_name_in_')])
        num_output_forms = sum([1 for k in query.iterkeys() if k.startswith('dataset_name_out_')])
        exceptions = {'inputs': {}, 'outputs': {}}

        new_method = None
        new_inputs = []
        new_outputs = []

        try:
            # retrieve CodeResource revision as driver
            try:
                coderesource_revision = CodeResourceRevision.objects.get(pk=query['revisions'])
            except:
                exceptions.update({'coderesource': 'Must specify code resource'})
                raise

            # use this prototype Method's name and description to initialize the MethodFamily
            try:
                if this_family is None:
                    this_family = MethodFamily(name=query['name'],
                                                 description=query['description'])
                    this_family.full_clean()
                    this_family.save()
            except ValidationError as e:
                for key, msg in e.message_dict.iteritems():
                    exceptions.update({key: str(msg[0])})
                raise  # we cannot continue with a broken MethodFamily

            # attempt to make Method object
            try:
                new_method = Method(family = this_family,
                                    revision_name=query['revision_name'],
                                    revision_desc=query['revision_desc'],
                                    driver=coderesource_revision,
                                    deterministic=query.has_key('deterministic'))
                new_method.full_clean()
                new_method.save()
            except ValidationError as e:
                for key, msg in e.message_dict.iteritems():
                    exceptions.update({key: str(msg[0])})
                raise  # we can't continue with a broken Method

            # attempt to make inputs
            for i in range(num_input_forms):
                dataset_name = query['dataset_name_in_'+str(i)]
                cdt_id = query['compounddatatype_in_'+str(i)]

                if dataset_name == '' and cdt_id == '':
                    # ignore blank form
                    continue

                try:
                    if cdt_id == '__raw__':
                        # request for unstructured input/output
                        new_input = new_method.create_input(dataset_name = query['dataset_name_in_'+str(i)],
                                                            dataset_idx = i+1)
                    else:
                        my_compound_datatype = CompoundDatatype.objects.get(pk=cdt_id)
                        min_row = query['min_row_in_'+str(i)]
                        max_row = query['max_row_in_'+str(i)]
                        new_input = new_method.create_input(dataset_name=query['dataset_name_in_'+str(i)],
                                                            dataset_idx=i+1,
                                                            compounddatatype=my_compound_datatype,
                                                            min_row=(min_row if min_row else None),
                                                            max_row=(max_row if max_row else None))
                    new_inputs.append(new_input)
                except ValidationError as e:
                    exceptions['inputs'].update({i: {}})
                    for key, msg in e.message_dict.iteritems():
                        exceptions['inputs'][i].update({key: str(msg[0])})
                    # pass through to check the other forms
                except:
                    # I -think- the only other exception is if we failed to retrieve the code resource
                    if i not in exceptions['inputs']:
                        exceptions['inputs'].update({i: {}})
                    exceptions['inputs'][i].update({'compounddatatype': 'You must select a Compound Datatype.'})

            # attempt to make outputs
            for i in range(num_output_forms):
                dataset_name = query['dataset_name_out_'+str(i)]
                cdt_id = query['compounddatatype_out_'+str(i)]

                if dataset_name == '' and cdt_id == '':
                    # ignore blank form
                    continue

                try:
                    if cdt_id == '__raw__':
                        new_output = new_method.create_output(dataset_name = query['dataset_name_out_'+str(i)],
                                                              dataset_idx = i+1)
                    else:
                        my_compound_datatype = CompoundDatatype.objects.get(pk=cdt_id)
                        min_row = query['min_row_out_'+str(i)]
                        max_row = query['max_row_out_'+str(i)]
                        new_output = new_method.create_output(dataset_name=query['dataset_name_out_'+str(i)],
                                                              dataset_idx=i+1,
                                                              compounddatatype=my_compound_datatype,
                                                              min_row=(min_row if min_row else None),
                                                              max_row=(max_row if max_row else None))
                    new_outputs.append(new_output)
                except ValidationError as e:
                    exceptions['outputs'].update({i: {}})
                    for key, msg in e.message_dict.iteritems():
                        exceptions['outputs'][i].update({key: str(msg[0])})
                except:
                    if i not in exceptions['outputs']:
                        exceptions['outputs'].update({i: {}})
                    exceptions['outputs'][i].update({'compounddatatype': 'You must select a Compound Datatype.'})

            if len(new_outputs) == 0 and len(exceptions['outputs']) == 0:
                exceptions['outputs'].update({0: {'dataset_name': 'You must specify at least one output.'}})

            if len(exceptions['inputs']) > 0 or len(exceptions['outputs']) > 0 or len(exceptions) > 2:
                # if there are more keys than 'inputs' and 'outputs' then
                # one or more input/output form exceptions have been raised
                raise

            # success!
            if id:
                return HttpResponseRedirect('/methods/%d' % int(id))
            else:
                return HttpResponseRedirect('/method_families')

        except:
            print 'cleaning up'
            # clean up after ourselves
            if hasattr(new_method, 'id') and new_method.id is not None:
                new_method.delete()
            if id is None and hasattr(this_family, 'id') and this_family.id is not None:
                # only delete if MethodFamily was created for this case only
                this_family.delete()

            family_form, method_form, input_forms, output_forms = return_method_forms(request, exceptions)
            c = Context({'family_form': family_form,
                         'method_form': method_form,
                         'input_forms': input_forms,
                         'output_forms': output_forms,
                         'family': this_family,
                         'header': header})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

    else:
        # first set of forms
        family_form = MethodFamilyForm()
        method_form = MethodForm()
        input_forms = [(TransformationXputForm(auto_id='id_%s_in_0'),
                       XputStructureForm(auto_id='id_%s_in_0'))]
        output_forms = [(TransformationXputForm(auto_id='id_%s_out_0'),
                       XputStructureForm(auto_id='id_%s_out_0'))]

    c = Context({'family_form': family_form,
                 'method_form': method_form,
                 'input_forms': input_forms,
                 'output_forms': output_forms,
                 'family': this_family,
                 'header': header})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def method_revise(request, id):
    """
    Add a revision of an existing Method.  revision_parent is defined by the
    previous version.
    """
    t = loader.get_template('method/method_revise.html')

    # retrieve the most recent member of this Method's family
    parent_method = Method.objects.filter(pk=id)[0]
    family = parent_method.family
    #all_members = Method.objects.filter(family=family).order_by('-id')
    #most_recent = all_members[0]  # always tack on revisions to most recent version (no trees)

    # retrieve the most recent revision of the corresponding CR
    parent_revision = parent_method.driver
    this_code_resource = parent_revision.coderesource
    all_revisions = CodeResourceRevision.objects.filter(coderesource=this_code_resource).order_by('-revision_DateTime')
    #last_revision = all_revisions[0]

    if request.method == 'POST':
        query = request.POST.dict()
        query.update({u'coderesource': this_code_resource})  # so we can pass it back to the form

        num_input_forms = sum([1 for k in query.iterkeys() if k.startswith('dataset_name_in_')])
        num_output_forms = sum([1 for k in query.iterkeys() if k.startswith('dataset_name_out_')])
        exceptions = {'inputs': {}, 'outputs': {}}
        new_method = None
        new_inputs = []
        new_outputs = []

        try:
            try:
                # retrieve CodeResource revision as driver
                coderesource_revision = CodeResourceRevision.objects.get(pk=query['revisions'])
            except:
                exceptions.update({'coderesource': 'Must specify code resource'})
                raise

            try:
                # attempt to make Method object
                new_method = Method(family = family, # same family
                                    revision_parent=parent_method,
                                    revision_name=query['revision_name'],
                                    revision_desc=query['revision_desc'],
                                    driver=coderesource_revision,
                                    deterministic=query.has_key('deterministic'))
                new_method.full_clean()
                new_method.save()
            except ValidationError as e:
                for key, msg in e.message_dict.iteritems():
                    exceptions.update({key: str(msg[0])})
                raise

            # attempt to make inputs
            for i in range(num_input_forms):
                dataset_name = query['dataset_name_in_'+str(i)]
                cdt_id = query['compounddatatype_in_'+str(i)]

                if dataset_name == '' and cdt_id == '':
                    # ignore blank form
                    continue

                try:
                    if cdt_id == '__raw__':
                        # request for unstructured input/output
                        new_input = new_method.create_input(dataset_name = query['dataset_name_in_'+str(i)],
                                                            dataset_idx = i+1)
                    else:
                        my_compound_datatype = CompoundDatatype.objects.get(pk=cdt_id)
                        min_row = query['min_row_in_'+str(i)]
                        max_row = query['max_row_in_'+str(i)]
                        new_input = new_method.create_input(dataset_name=query['dataset_name_in_'+str(i)],
                                                            dataset_idx=i+1,
                                                            compounddatatype=my_compound_datatype,
                                                            min_row=(min_row if min_row else None),
                                                            max_row=(max_row if max_row else None))
                    new_inputs.append(new_input)
                except ValidationError as e:
                    exceptions['inputs'].update({i: {}})
                    for key, msg in e.message_dict.iteritems():
                        exceptions['inputs'][i].update({key: str(msg[0])})
                    # pass through to check the other forms
                except:
                    # I -think- the only other exception is if we failed to retrieve the code resource
                    if i not in exceptions['inputs']:
                        exceptions['inputs'].update({i: {}})
                    exceptions['inputs'][i].update({'compounddatatype': 'You must select a Compound Datatype.'})

            # attempt to make outputs
            for i in range(num_output_forms):
                dataset_name = query['dataset_name_out_'+str(i)]
                cdt_id = query['compounddatatype_out_'+str(i)]

                if dataset_name == '' and cdt_id == '':
                    # ignore blank form
                    continue

                try:
                    if cdt_id == '__raw__':
                        new_output = new_method.create_output(dataset_name = query['dataset_name_out_'+str(i)],
                                                              dataset_idx = i+1)
                    else:
                        my_compound_datatype = CompoundDatatype.objects.get(pk=cdt_id)
                        min_row = query['min_row_out_'+str(i)]
                        max_row = query['max_row_out_'+str(i)]
                        new_output = new_method.create_output(dataset_name=query['dataset_name_out_'+str(i)],
                                                              dataset_idx=i+1,
                                                              compounddatatype=my_compound_datatype,
                                                              min_row=(min_row if min_row else None),
                                                              max_row=(max_row if max_row else None))
                    new_outputs.append(new_output)
                except ValidationError as e:
                    exceptions['outputs'].update({i: {}})
                    for key, msg in e.message_dict.iteritems():
                        exceptions['outputs'][i].update({key: str(msg[0])})
                except:
                    if i not in exceptions['outputs']:
                        exceptions['outputs'].update({i: {}})
                    exceptions['outputs'][i].update({'compounddatatype': 'You must select a Compound Datatype.'})

            if len(new_inputs) == 0:
                exceptions['inputs'].update({0: {'dataset_name': 'You must specify at least one input.'}})
            if len(new_outputs) == 0:
                exceptions['outputs'].update({0: {'dataset_name': 'You must specify at least one output.'}})
            if len(exceptions) > 2:
                # if there are more keys than 'inputs' and 'outputs' then
                # one or more input/output form exceptions have been raised
                raise

            # success!
            return HttpResponseRedirect('/methods/%d' % family.pk)

        except:
            # do not delete method_family!!
            if hasattr(new_method, 'id') and new_method.id is not None:
                new_method.delete()

            family_form, method_form, input_forms, output_forms = return_method_forms(request, exceptions)
            c = Context({'method_form': method_form,
                         'input_forms': input_forms,
                         'output_forms': output_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

    else:
        # initialize forms with values of parent Method
        method_form = MethodReviseForm(initial={#'revision_name': parent_method.revision_name,
                                                'revision_desc': parent_method.revision_desc,
                                                'revisions': parent_revision.pk,
                                                'deterministic': parent_method.deterministic})
        method_form.fields['revisions'].choices = [(x.id, '%d: %s' % (x.revision_number, x.revision_name))
                                                   for x in all_revisions]

        input_forms = []
        output_forms = []
        for input in parent_method.inputs.all():
            tx_form = TransformationXputForm(auto_id='id_%s_in_'+str(len(input_forms)),
                                            initial={'dataset_name': input.dataset_name,
                                                     'dataset_idx': input.dataset_idx})
            if input.has_structure:
                structure = input.structure
                xs_form = XputStructureForm(auto_id='id_%s_in_'+str(len(input_forms)),
                                            initial={'compounddatatype': structure.compounddatatype.id,
                                                     'min_row': structure.min_row,
                                                     'max_row': structure.max_row})
            else:
                xs_form = XputStructureForm(auto_id='id_%s_in_'+str(len(input_forms)),
                                            initial={'compounddatatype': '__raw__'})

            input_forms.append((tx_form, xs_form))

        # if previous Method has no inputs, provide blank forms
        if len(input_forms) == 0:
            tx_form = TransformationXputForm(auto_id='id_%s_in_0')
            xs_form = XputStructureForm(auto_id='id_%s_in_0')
            input_forms.append((tx_form, xs_form))

        for output in parent_method.outputs.all():
            tx_form = TransformationXputForm(auto_id='id_%s_out_'+str(len(output_forms)),
                                            initial={'dataset_name': output.dataset_name,
                                                     'dataset_idx': output.dataset_idx})
            if output.has_structure:
                structure = output.structure
                xs_form = XputStructureForm(auto_id='id_%s_out_'+str(len(output_forms)),
                                            initial={'compounddatatype': structure.compounddatatype.id,
                                                     'min_row': structure.min_row,
                                                     'max_row': structure.max_row})
            else:
                xs_form = XputStructureForm(auto_id='id_%s_out_'+str(len(output_forms)),
                                            initial={'compounddatatype': '__raw__'})

            output_forms.append((tx_form, xs_form))

    c = Context({'coderesource': this_code_resource,
                 'method_form': method_form,
                 'input_forms': input_forms,
                 'output_forms': output_forms,
                 'family': family,
                 'parent': parent_method})
    c.update(csrf(request))
    return HttpResponse(t.render(c))
