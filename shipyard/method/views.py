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
from django.contrib.auth.models import Group

from constants import groups, users

everyone = Group.objects.get(pk=groups.EVERYONE_PK)
shipyard_user = User.objects.get(pk=users.SHIPYARD_USER_PK)

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


def return_crv_forms(query, field_errors, backend_exceptions, is_new):
    """
    Populates forms with user-submitted values and form validation errors.

    This is a helper function for resource_add() and resource_revision_add(); its results are
    to be returned as HttpResponse.

    NOTE: cannot set default value of FileField due to security.
    """

    if is_new:
        # creating a new CodeResource
        crv_form = CodeResourcePrototypeForm(initial={'resource_name': query['resource_name'],
                                                      'resource_desc': query['resource_desc']})
        crv_form.errors.update(field_errors)
        crv_form.errors.update({'backend_content_file': exceptions.get('content_file', ''),
                                'backend_resource_name': exceptions.get('name', ''),
                                'backend_resource_desc': exceptions.get('description', '')})
    else:
        # revising a code resource
        crv_form = CodeResourceRevisionForm(initial={'revision_name': query['revision_name'],
                                                     'revision_desc': query['revision_desc']})
        crv_form.errors.update(field_errors)
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


@transaction.atomic
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
        # Using a form here provides validation and better parsing of parameters in the request.
        resource_form = CodeResourcePrototypeForm(request.POST, request.FILES)
        query = request.POST.dict()

        # Also validate the CR dependencies using forms.
        num_dep_forms = sum([1 for k in query.iterkeys() if k.startswith('coderesource_')])
        dep_forms = []
        for i in range(num_dep_forms):
            this_cr = query['coderesource_'+str(i)]  # PK of CodeResource
            if this_cr == '':
                # ignore blank CR dependency forms
                dep_forms.append(None)
                continue

            dep_forms.append(
                CodeResourceDependencyForm(
                    {
                        'coderesource': query['coderesource_'+str(i)],
                        'revisions': query['revisions_'+str(i)],
                        'depPath': query['depPath_'+str(i)],
                        'depFileName': query['depFileName_'+str(i)]
                    },
                    auto_id='id_%s_'+str(i)
                )
            )

        all_valid = True

        if not resource_form.is_valid():
            all_valid = False
        for dep_form in dep_forms:
            if dep_form is None:
                continue
            if not dep_form.is_valid():
                all_valid = False

        if not all_valid:
            # The forms are already pre-populated with the appropriate errors, so we don't need return_crv_forms.
            c = Context({'resource_form': resource_form, 'dep_forms': dep_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # Now we can try to create objects in the database, catching backend-raised exceptions as we go.

        # FIXME we can't simply use request.user because that's an AnonymousUser.
        creating_user = shipyard_user
        new_code_resource = None
        prototype = None

        try:
            file_in_memory = request.FILES["content_file"]

            new_code_resource = CodeResource(
                name=resource_form.cleaned_data['resource_name'],
                description=resource_form.cleaned_data['resource_desc'],
                filename=file_in_memory.name,
                user=creating_user
            )
            # Skip the clean until later; after all, we're protected by a transaction here.
            new_code_resource.save()

            # Modify actual filename prior to saving revision object.
            file_in_memory.name += '_' + datetime.now().strftime('%Y%m%d%H%M%S')

            prototype = CodeResourceRevision(
                revision_name='Prototype',
                revision_desc=resource_form.cleaned_data['resource_desc'],
                coderesource=new_code_resource,
                content_file=file_in_memory,
                user=creating_user
            )
            prototype.save()

            for user_pk in resource_form.cleaned_data["users_allowed"]:
                new_code_resource.users_allowed.add(user_pk)
                prototype.users_allowed.add(user_pk)
            for group_pk in resource_form.cleaned_data["groups_allowed"]:
                new_code_resource.groups_allowed.add(group_pk)
                prototype.groups_allowed.add(group_pk)

            new_code_resource.full_clean()
            new_code_resource.save()
            prototype.full_clean()
            prototype.save()


        except ValidationError as e:
            resource_form.add_error(None, e)
            c = Context({'resource_form': resource_form, 'dep_forms': dep_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # bind CR dependencies
        any_dep_exceptions = False
        for i in range(num_dep_forms):
            if dep_forms[i] is None:
                continue
            try:
                on_revision = CodeResourceRevision.objects.get(pk=dep_forms[i].cleaned_data["revisions"])
                dependency = CodeResourceDependency(
                    coderesourcerevision=prototype,
                    requirement = on_revision,
                    depPath=dep_forms[i].cleaned_data["depPath"],
                    depFileName=dep_forms[i].cleaned_data["depFileName"]
                    )
                dependency.full_clean()
                dependency.save()
            except ValidationError as e:
                dep_forms[i].add_error(None, e)
                any_dep_exceptions = True

        if any_dep_exceptions:
            c = Context({'resource_form': resource_form, 'dep_forms': dep_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # Success -- return to the resources root page.
        return HttpResponseRedirect('/resources')

    else:
        resource_form = CodeResourcePrototypeForm()
        dep_forms = [CodeResourceDependencyForm(auto_id='id_%s_0')]

    c = Context({'resource_form': resource_form, 'dep_forms': dep_forms})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


# FIXME continue from here and refactor this as above
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
        # Use a form here, just as in resource_add.
        revision_form = CodeResourcePrototypeForm(request.POST, request.FILES)
        query = request.POST.dict()

        # Also validate the CR dependencies using forms.
        num_dep_forms = sum([1 for k in query.iterkeys() if k.startswith('coderesource_')])
        dep_forms = []
        for i in range(num_dep_forms):
            this_cr = query['coderesource_'+str(i)]  # PK of CodeResource
            if this_cr == '':
                # ignore blank CR dependency forms
                dep_forms.append(None)
                continue

        all_valid = True

        if not revision_form.is_valid():
            all_valid = False
        for dep_form in dep_forms:
            if dep_form is None:
                continue
            if not dep_form.is_valid():
                all_valid = False

        if not all_valid:
            # The forms are already pre-populated with the appropriate errors, so we don't need return_crv_forms.
            c = Context({'revision_form': revision_form, 'parent_revision': parent_revision,
                         'coderesource': coderesource, 'dep_forms': dep_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        revision = None

        # FIXME as above, for now, everything happens as the Shipyard user.
        creating_user = shipyard_user

        try:
            file_in_memory = request.FILES['content_file']
            # Modify actual filename prior to saving revision object.
            file_in_memory.name += datetime.now().strftime('_%Y%m%d%H%M%S')

            # is this file identical to another CodeResourceRevision?
            revision = CodeResourceRevision(
                revision_parent=parent_revision,
                revision_name=query['revision_name'],
                revision_desc=query['revision_desc'],
                coderesource=coderesource,
                content_file=file_in_memory,
                user=creating_user
            )
            revision.save()

            for user_pk in revision_form.cleaned_data["users_allowed"]:
                revision.users_allowed.add(user_pk)
            for group_pk in revision_form.cleaned_data["groups_allowed"]:
                revision.groups_allowed.add(group_pk)

            revision.full_clean()
            revision.save()

        except ValidationError as e:
            revision_form.add_error(None, e)
            c = Context({'revision_form': revision_form, 'parent_revision': parent_revision,
                         'coderesource': coderesource, 'dep_forms': dep_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c)) # CodeResourceRevision object required for next steps

        # bind CR dependencies
        any_dep_exceptions = False
        for i in range(num_dep_forms):
            if dep_forms[i] is None:
                continue

            try:
                on_revision = CodeResourceRevision.objects.get(pk=dep_forms[i].cleaned_data["revisions"])
                dependency = CodeResourceDependency(
                    coderesourcerevision=revision,
                    requirement = on_revision,
                    depPath=dep_forms[i].cleaned_data["depPath"],
                    depFileName=dep_forms[i].cleaned_data["depFileName"]
                )
                dependency.full_clean()
                dependency.save()
            except ValidationError as e:
                dep_forms[i].add_error(None, e)
                any_dep_exceptions = True

        if any_dep_exceptions:
            c = Context({'revision_form': revision_form, 'parent_revision': parent_revision,
                         'coderesource': coderesource, 'dep_forms': dep_forms})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # Success; return to the resources page.
        return HttpResponseRedirect('/resources')

    # Having reached here, we know that this CR is being revised.  Return a form pre-populated
    # with default info.
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
        dep_forms.append(CodeResourceDependencyForm(auto_id='id_%s_0', parent=coderesource.id))

    c = Context({'revision_form': crv_form, 'parent_revision': parent_revision,
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
                family = MethodFamily.create(name=query['name'], description=query['description'],
                                             user=query["user"])
            new_method = Method.create(
                names,
                compounddatatypes=compounddatatypes,
                row_limits=row_limits,
                num_inputs=num_inputs,
                family=family,
                revision_name=query['revision_name'],
                revision_desc=query['revision_desc'],
                revision_parent=parent_method,
                driver=coderesource_revision,
                deterministic=query.has_key('deterministic'),
                user=query["user"]
            )

            for user in query["users_allowed"]:
                family.users_allowed.add(user)
                new_method.users_allowed.add(user)
            for group in query["groups_allowed"]:
                family.groups_allowed.add(group)
                new_method.groups_allowed.add(group)
            family.save()
            new_method.save()

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

        # Add the requesting user to the query.
        query["user"] = request.user
        # FIXME for the moment everyone is allowed access.
        query["users_allowed"] = []
        if query["shared"]:
            query["groups_allowed"] = [everyone]

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
        query["user"] = request.user
        # FIXME same issue as in method_add where everyone is allowed access.
        query["users_allowed"] = []
        if query["shared"]:
            query["groups_allowed"] = [everyone]

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
