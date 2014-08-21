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
    revisions = coderesource.revisions.order_by('-revision_number')
    t = loader.get_template('method/resource_revisions.html')
    c = Context({'coderesource': coderesource, 'revisions': revisions})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def return_crv_forms(query, exceptions, is_new):
    """
    A helper function for resource_revise() to populate forms with user-submitted values
    and form validation errors to be returned as HttpResponse.
    NOTE: cannot set default value of FileField due to security.
    """

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

            crv_form, dep_forms = return_crv_forms(query, exceptions, False)
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
    family = MethodFamily.objects.get(pk=id)
    its_methods = family.members.all()

    t = loader.get_template('method/methods.html')
    c = Context({'methods': its_methods, 'family': family})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def return_method_forms (query, exceptions):
    """
    Helper function for method_add()
    Send HttpResponse Context with forms filled out with previous values including error messages
    """    
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

    # populate in/output forms with submitted values
    xput_forms = []
    for xput_type in ["in", "out"]:
        num_forms = sum(k.startswith('dataset_name_{}_'.format(xput_type)) for k in query)
        forms = []
        error_key = "{}puts".format(xput_type)
        for i in range(num_forms):
            auto_id = "id_%s_{}_{}".format(xput_type, i)
            t_form = TransformationXputForm(auto_id=auto_id,
                    initial={'dataset_name': query['dataset_name_{}_{}'.format(xput_type, i)]})
            xs_form = XputStructureForm(auto_id=auto_id,
                    initial={'compounddatatype': query['compounddatatype_{}_{}'.format(xput_type, i)],
                             'min_row': query['min_row_{}_{}'.format(xput_type, i)],
                             'max_row': query['max_row_{}_{}'.format(xput_type, i)]})

            if i in exceptions['inputs']: 
                if 'dataset_name' in exceptions[error_key][i]:
                    t_form.errors.update({'dataset_name': exceptions[error_key][i]['dataset_name']})
                if 'compounddatatype' in exceptions[error_key][i]:
                    xs_form.errors.update({'compounddatatype': exceptions[error_key][i]['compounddatatype']})

            forms.append((t_form, xs_form))
        xput_forms.append(forms)

    return family_form, method_form, xput_forms[0], xput_forms[1]


def parse_method_form(query, family=None, parent_method=None):
    """Parse user input for adding or revising Methods.
    
    Return a dictionary of exceptions, or None if everything
    went smoothly.

    PARAMETERS
    family          MethodFamily to add new Method to (None for new
                    family)
    parent_method   parent revision for new Method (None for no parent)
    """
    exceptions = {"inputs": {}, "outputs": {}}
    num_input_forms = sum(k.startswith('dataset_name_in_') for k in query)
    num_output_forms = sum(k.startswith('dataset_name_out_') for k in query)

    # retrieve CodeResource revision as driver
    try:
        coderesource_revision = CodeResourceRevision.objects.get(pk=query['revisions'])
    except ValueError:
        exceptions.update({'coderesource': 'Must specify code resource'})

    # attempt to make in/outputs
    names = []
    compounddatatypes = []
    row_limits = []
    num_inputs = 0
    num_outputs = 0
    for i in range(num_input_forms + num_output_forms):
        xput_type = "in" if i < num_input_forms else "out"
        xput_idx = i-num_input_forms if i>=num_input_forms else i
        dataset_name = query['dataset_name_{}_{}'.format(xput_type, xput_idx)]
        cdt_id = query['compounddatatype_{}_{}'.format(xput_type, xput_idx)]

        if dataset_name == '' and cdt_id == '':
            # ignore blank form
            continue

        if i < num_input_forms:
            num_inputs += 1
        else:
            num_outputs += 1

        names.append(dataset_name)
        my_compound_datatype = None
        min_row = None
        max_row = None
        if cdt_id != '__raw__':
            try:
                my_compound_datatype = CompoundDatatype.objects.get(pk=cdt_id)
            except ValueError:
                error_key = "{}puts".format(xput_type)
                exceptions[error_key][i].update({'compounddatatype': 'You must select a Compound Datatype.'})
            min_row = query['min_row_{}_{}'.format(xput_type, xput_idx)]
            max_row = query['max_row_{}_{}'.format(xput_type, xput_idx)]

        compounddatatypes.append(my_compound_datatype)
        row_limits.append((min_row or None, max_row or None))

    if num_outputs == 0 and len(exceptions['outputs']) == 0:
        exceptions['outputs'].update({0: {'dataset_name': 'You must specify at least one output.'}})

    if len(exceptions['inputs']) > 0 or len(exceptions['outputs']) > 0 or len(exceptions) > 2:
        # if there are more keys than 'inputs' and 'outputs' then
        # one or more input/output form exceptions have been raised
        return exceptions

    try:
        with transaction.atomic():
            if family is None:
                family = MethodFamily.create(name=query['name'], description=query['description'])
            new_method = Method.create(names, 
                    compounddatatypes=compounddatatypes, 
                    row_limits=row_limits,
                    num_inputs=num_inputs,
                    family=family,
                    revision_name=query['revision_name'],
                    revision_desc=query['revision_desc'],
                    revision_parent=parent_method,
                    driver=coderesource_revision,
                    deterministic=query.has_key('deterministic'))
            return None

    except ValidationError as e:
        if hasattr(e, "error_dict"):
            for key, msg in e.message_dict.iteritems():
                exceptions.update({key: str(msg[0])})
        else:
            # TODO: where to display our own (non-Django) ValidationErrors?
            if parent_method:
                exceptions["name"] = e.messages[0]
            else:
                exceptions["revision_name"] = e.messages[0]

    return exceptions

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
        exceptions = parse_method_form(query, this_family)
        if exceptions is None:
            # success!
            if id:
                return HttpResponseRedirect('/methods/{}'.format(id))
            else:
                return HttpResponseRedirect('/method_families')
        family_form, method_form, input_forms, output_forms = return_method_forms(query, exceptions)
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
    parent_method = Method.objects.get(pk=id)
    family = parent_method.family

    # retrieve the most recent revision of the corresponding CR
    parent_revision = parent_method.driver
    this_code_resource = parent_revision.coderesource
    all_revisions = this_code_resource.revisions.order_by('-revision_DateTime')

    if request.method == 'POST':
        query = request.POST.dict()
        query.update({u'coderesource': this_code_resource})  # so we can pass it back to the form

        exceptions = parse_method_form(query, family=family, parent_method=parent_method)
        if exceptions is None:
            # success!
            return HttpResponseRedirect('/methods/%d' % family.pk)
        family_form, method_form, input_forms, output_forms = return_method_forms(query, exceptions)
    else:
        # initialize forms with values of parent Method
        method_form = MethodReviseForm(initial={#'revision_name': parent_method.revision_name,
                                                'revision_desc': parent_method.revision_desc,
                                                'revisions': parent_revision.pk,
                                                'deterministic': parent_method.deterministic})
        xput_forms = []
        inputs = parent_method.inputs.order_by("dataset_idx")
        outputs = parent_method.outputs.order_by("dataset_idx")
        for xput_type, xputs in (("in", inputs), ("out", outputs)):
            forms = []
            for xput in xputs:
                tx_form = TransformationXputForm(auto_id='id_%s_{}_{}'.format(xput_type, len(forms)),
                                                initial={'dataset_name': xput.dataset_name,
                                                         'dataset_idx': xput.dataset_idx})
                if xput.has_structure:
                    structure = xput.structure
                    xs_form = XputStructureForm(auto_id='id_%s_{}_{}'.format(xput_type, len(forms)),
                                            initial={'compounddatatype': structure.compounddatatype.id,
                                                     'min_row': structure.min_row,
                                                     'max_row': structure.max_row})
                else:
                    xs_form = XputStructureForm(auto_id='id_%s_{}_{}'.format(xput_type, len(forms)),
                                            initial={'compounddatatype': '__raw__'})

                forms.append((tx_form, xs_form))
            xput_forms.append(forms)

        input_forms, output_forms = xput_forms
        # if previous Method has no inputs, provide blank forms
        if len(input_forms) == 0:
            tx_form = TransformationXputForm(auto_id='id_%s_in_0')
            xs_form = XputStructureForm(auto_id='id_%s_in_0')
            input_forms.append((tx_form, xs_form))

    method_form.fields['revisions'].choices = [(x.id, '%d: %s' % (x.revision_number, x.revision_name))
                                               for x in all_revisions]
    c = Context({'coderesource': this_code_resource,
                 'method_form': method_form,
                 'input_forms': input_forms,
                 'output_forms': output_forms,
                 'family': family,
                 'parent': parent_method})
    c.update(csrf(request))
    return HttpResponse(t.render(c))

def resource_revision_view(request, id):
    revision = CodeResourceRevision.objects.get(pk=id)
    t = loader.get_template("method/resource_revision_view.html")
    c = Context({"revision": revision})
    c.update(csrf(request))
    return HttpResponse(t.render(c))
