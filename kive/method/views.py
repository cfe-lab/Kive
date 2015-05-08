"""
method.views
"""

from django.db import transaction
from django.core.exceptions import ValidationError
from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import HttpResponse, HttpResponseRedirect, Http404
from django.template import loader, RequestContext

from datetime import datetime
import json

import metadata.models
from metadata.models import CompoundDatatype, AccessControl
from method.models import CodeResource, CodeResourceDependency, Method, \
    MethodFamily, CodeResourceRevision
from method.forms import CodeResourceDependencyForm, \
    CodeResourcePrototypeForm, CodeResourceRevisionForm, MethodFamilyForm, \
    MethodForm, MethodReviseForm, TransformationXputForm, XputStructureForm
from portal.views import developer_check, admin_check
from method.serializers import MethodFamilySerializer, MethodSerializer, \
    CodeResourceSerializer, CodeResourceRevisionSerializer


@login_required
@user_passes_test(developer_check)
def resources(request):
    """
    Display a list of all code resources (parents) in database
    """
    resources = CodeResource.filter_by_user(request.user)

    resource_json = json.dumps(
        CodeResourceSerializer(resources, many=True, context={"request": request}).data
    )

    t = loader.get_template('method/resources.html')
    c = RequestContext(request,
                       {
                           'resources': resources,
                           'coderesources': resource_json,
                           "is_user_admin": admin_check(request.user)
                       })

    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def resource_revisions(request, id):
    """
    Display a list of all revisions of a specific Code Resource in database.
    """
    c = RequestContext(request)
    four_oh_four = False
    try:
        coderesource = CodeResource.objects.get(pk=id)
        if not coderesource.can_be_accessed(request.user):
            four_oh_four = True
    except CodeResource.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        # Redirect back to the resources page.
        raise Http404("ID {} cannot be accessed".format(id))

    # Cast request.user to class KiveUser.
    curr_user = metadata.models.KiveUser.kiveify(request.user)
    revisions = coderesource.revisions.filter(curr_user.access_query()).distinct().order_by(
        '-revision_number')
    t = loader.get_template('method/resource_revisions.html')
    c.update({'coderesource': coderesource, 'revisions': revisions})
    return HttpResponse(t.render(c))


def _make_dep_forms(query_dict, user):
    """
    Helper for resource_add and resource_revision_add that creates the CodeResourceDependencyForms.
    """
    num_dep_forms = sum([1 for k in query_dict.iterkeys() if k.startswith('coderesource_')])
    dep_forms = []
    for i in range(num_dep_forms):
        this_cr = query_dict['coderesource_'+str(i)]  # PK of CodeResource
        if this_cr == '':
            # Ignore blank CR dependency forms.
            dep_forms.append(None)
            continue

        dep_forms.append(
            CodeResourceDependencyForm(
                {
                    'coderesource': query_dict['coderesource_'+str(i)],
                    'revisions': query_dict['revisions_'+str(i)],
                    'depPath': query_dict['depPath_'+str(i)],
                    'depFileName': query_dict['depFileName_'+str(i)]
                },
                user=user,
                auto_id='id_%s_'+str(i)
            )
        )
    return dep_forms


@transaction.atomic
def _make_crv(file_in_memory, creating_user, crv_form, dep_forms, parent_revision=None):
    """
    Helper that creates a CodeResourceRevision (and a CodeResource as well if appropriate).
    """
    assert isinstance(crv_form, (CodeResourcePrototypeForm, CodeResourceRevisionForm))
    # If parent_revision is specified, we are only making a CodeResourceRevision and not its parent CodeResource.
    assert not (parent_revision is None and isinstance(crv_form, CodeResourceRevision))
    for dep_form in dep_forms:
        assert isinstance(dep_form, CodeResourceDependencyForm) or dep_form is None

    if parent_revision is None:
        # crv_form is a CodeResourcePrototypeForm.
        code_resource = CodeResource(
            name=crv_form.cleaned_data['resource_name'],
            description=crv_form.cleaned_data['resource_desc'],
            filename=file_in_memory.name,
            user=creating_user
        )
        # Skip the clean until later; after all, we're protected by a transaction here.
        code_resource.save()

        for user in crv_form.cleaned_data["users_allowed"]:
            code_resource.users_allowed.add(user)
        for group in crv_form.cleaned_data["groups_allowed"]:
            code_resource.groups_allowed.add(group)

        rev_name = "Prototype"
        rev_desc = crv_form.cleaned_data["resource_desc"]
    else:
        code_resource = parent_revision.coderesource
        rev_name = crv_form.cleaned_data["revision_name"]
        rev_desc = crv_form.cleaned_data["revision_desc"]

    # Modify actual filename prior to saving revision object.
    file_in_memory.name += '_' + datetime.now().strftime('%Y%m%d%H%M%S')

    revision = CodeResourceRevision(
        revision_parent=parent_revision,
        revision_name=rev_name,
        revision_desc=rev_desc,
        coderesource=code_resource,
        content_file=file_in_memory,
        user=creating_user
    )
    # This sets the MD5.

    try:
        revision.clean()
    except ValidationError as e:
        crv_form.add_error(None, e)
        raise e

    revision.save()

    for user in crv_form.cleaned_data["users_allowed"]:
        revision.users_allowed.add(user)
    for group in crv_form.cleaned_data["groups_allowed"]:
        revision.groups_allowed.add(group)

    revision.save()

    # Bind CR dependencies.
    for i in range(len(dep_forms)):
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
            raise e

    try:
        code_resource.full_clean()
        revision.full_clean()
    except ValidationError as e:
        crv_form.add_error(None, e)
        raise e

    return revision


@login_required
@user_passes_test(developer_check)
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
    c = RequestContext(request)
    creating_user = request.user

    if request.method == 'POST':
        # Using forms here provides validation and better parsing of parameters in the request.
        resource_form = CodeResourcePrototypeForm(request.POST, request.FILES)
        dep_forms = _make_dep_forms(request.POST.dict(), creating_user)

        # Note that entries of dep_forms may be None -- we simply skip these.
        all_good = True
        if not resource_form.is_valid():
            all_good = False
        for dep_form in [x for x in dep_forms if x is not None]:
            if not dep_form.is_valid():
                all_good = False

        if not all_good:
            c.update({'resource_form': resource_form, 'dep_forms': dep_forms})
            return HttpResponse(t.render(c))

        # Now we can try to create objects in the database, catching backend-raised exceptions as we go.
        try:
            _make_crv(request.FILES["content_file"], creating_user, resource_form, dep_forms)
        except ValidationError:
            # All forms have the appropriate errors attached.
            c.update({'resource_form': resource_form, 'dep_forms': dep_forms})
            return HttpResponse(t.render(c))

        # Success -- return to the resources root page.
        return HttpResponseRedirect('/resources')
    else:
        resource_form = CodeResourcePrototypeForm()
        dep_forms = [CodeResourceDependencyForm(user=creating_user, auto_id='id_%s_0')]

    c.update({'resource_form': resource_form, 'dep_forms': dep_forms})
    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def resource_revision_add(request, id):
    """
    Add a code resource revision.  The form will initially be populated with values of the last
    revision to this code resource.
    """
    t = loader.get_template('method/resource_revision_add.html')
    c = RequestContext(request)
    creating_user = request.user

    # Use POST information (id) to retrieve the CRv being revised.
    four_oh_four = False
    try:
        parent_revision = CodeResourceRevision.objects.get(pk=id)
        if not parent_revision.can_be_accessed(creating_user):
            four_oh_four = True
    except CodeResourceRevision.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        # Redirect back to the resources page.
        raise Http404("ID {} cannot be accessed".format(id))

    coderesource = parent_revision.coderesource

    if request.method == 'POST':
        # Use forms here, just as in resource_add.  Again note that entries of dep_forms may be None.
        revision_form = CodeResourceRevisionForm(request.POST, request.FILES)
        dep_forms = _make_dep_forms(request.POST.dict(), creating_user)

        all_good = True
        if not revision_form.is_valid():
            all_good = False
        for dep_form in [x for x in dep_forms if x is not None]:
            if not dep_form.is_valid():
                all_good = False

        if not all_good:
            c.update({
                'revision_form': revision_form,
                'parent_revision': parent_revision,
                'coderesource': coderesource,
                'dep_forms': dep_forms
            })
            return HttpResponse(t.render(c))


        try:
            _make_crv(request.FILES['content_file'], creating_user, revision_form, dep_forms,
                      parent_revision=parent_revision)
        except ValidationError:
            # The forms have all been updated with the appropriate errors.
            c.update(
                {
                    'revision_form': revision_form,
                    'parent_revision': parent_revision,
                    'coderesource': coderesource,
                    'dep_forms': dep_forms
                })
            return HttpResponse(t.render(c)) # CodeResourceRevision object required for next steps

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
            dep_form = CodeResourceDependencyForm(
                user=creating_user,
                auto_id='id_%s_'+str(i),
                initial={
                    'coderesource': its_cr.pk,
                    'revisions': its_crv.pk,
                    'depPath': dependency.depPath,
                    'depFileName': dependency.depFileName
                },
                parent=coderesource.id)
        else:
            dep_form = CodeResourceDependencyForm(user=creating_user, auto_id='id_%s_'+str(i))
        dep_forms.append(dep_form)

    # in case the parent revision has no CR dependencies, add a blank form
    if len(dep_forms) == 0:
        dep_forms.append(CodeResourceDependencyForm(user=creating_user, auto_id='id_%s_0', parent=coderesource.id))

    c.update(
        {
            'revision_form': crv_form,
            'parent_revision': parent_revision,
            'coderesource': coderesource,
            'dep_forms': dep_forms
        }
    )
    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def resource_revision_view(request, id):
    four_oh_four = False
    try:
        revision = CodeResourceRevision.objects.get(pk=id)
        if not revision.can_be_accessed(request.user):
            four_oh_four = True
    except CodeResourceRevision.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        raise Http404("ID {} is not accessible".format(id))

    t = loader.get_template("method/resource_revision_view.html")
    c = RequestContext(request, {"revision": revision})
    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def method_families(request):
    """
    Display a list of all MethodFamily objects in database.
    """
    families = MethodFamily.filter_by_user(request.user)
    families_json = json.dumps(
        MethodFamilySerializer(
            families,
            context={"request": request},
            many=True).data
    )

    t = loader.get_template("method/method_families.html")
    c = RequestContext(
        request,
        {
            "method_families": families_json,
            "is_user_admin": admin_check(request.user)
            })
    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def methods(request, id):
    """
    Display a list of all Methods within a given MethodFamily.
    """
    four_oh_four = False
    try:
        family = MethodFamily.objects.get(pk=id)
        if not family.can_be_accessed(request.user) and not admin_check(request.user):
            four_oh_four = True
    except MethodFamily.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        # Redirect back to the resources page.
        raise Http404("ID {} cannot be accessed".format(id))

    member_methods = AccessControl.filter_by_user(
        request.user,
        is_admin=False,
        queryset=family.members.all())

    methods_json = json.dumps(
        MethodSerializer(member_methods, many=True, context={"request": request}).data
    )

    t = loader.get_template('method/methods.html')
    c = RequestContext(request,
                       {
                           'family': family,
                           "methods": methods_json,
                           "is_user_admin": admin_check(request.user)
                       })
    return HttpResponse(t.render(c))


def create_method_forms(request_post, user, family=None):
    """
    Helper function for method_add() that creates Forms from the provided information and validates them.
    """
    query_dict = request_post.dict()
    if 'name' in query_dict:
        assert family is None
        family_form = MethodFamilyForm(request_post)
    else:
        assert family is not None
        family_form = MethodFamilyForm({"name": family.name, "description": family.description})
    family_form.is_valid()

    # Populate main form with submitted values.
    if "coderesource" in query_dict:
        method_form = MethodForm(request_post, user=user)
    else:
        method_form = MethodReviseForm(request_post)
    method_form.is_valid()

    # Populate in/output forms with submitted values.
    input_forms = []
    output_forms = []
    for xput_type in ["in", "out"]:
        num_forms = sum(k.startswith('dataset_name_{}_'.format(xput_type)) for k in query_dict)

        for i in range(num_forms):
            auto_id = "id_%s_{}_{}".format(xput_type, i)
            t_form = TransformationXputForm(
                {'dataset_name': query_dict['dataset_name_{}_{}'.format(xput_type, i)]},
                auto_id=auto_id)
            t_form.is_valid()

            xs_form = XputStructureForm(
                {'compounddatatype': query_dict['compounddatatype_{}_{}'.format(xput_type, i)],
                 'min_row': query_dict['min_row_{}_{}'.format(xput_type, i)],
                 'max_row': query_dict['max_row_{}_{}'.format(xput_type, i)]},
                user=user,
                auto_id=auto_id)
            xs_form.is_valid()

            if xput_type == "in":
                input_forms.append((t_form, xs_form))
            else:
                output_forms.append((t_form, xs_form))

    return family_form, method_form, input_forms, output_forms


def create_method_from_forms(family_form, method_form, input_forms, output_forms, creating_user,
                             family=None, parent_method=None):
    """
    Given Forms representing the MethodFamily, Method, inputs, and outputs, create a Method.

    Return the same forms, updated with errors if there are any.
    """
    # This assures that not both family_form and family are None.
    assert family is not None or family_form is not None

    # Retrieve the CodeResource revision as driver.
    try:
        coderesource_revision = CodeResourceRevision.objects.get(pk=method_form.cleaned_data['revisions'])
    except (ValueError, CodeResourceRevision.DoesNotExist) as e:
        method_form.add_error("revisions", e)
        return None

    new_method = None
    try:
        # Note how the except blocks re-raise their exception: that is to terminate
        # this transaction.
        with transaction.atomic():
            if family is None:
                try:
                    family = MethodFamily.create(
                        name=family_form.cleaned_data["name"],
                        description=family_form.cleaned_data['description'],
                        user=creating_user)

                    for user in method_form.cleaned_data["users_allowed"]:
                        family.users_allowed.add(user)
                    for group in method_form.cleaned_data["groups_allowed"]:
                        family.groups_allowed.add(group)

                except ValidationError as e:
                    family_form.add_error(None, e)
                    raise e

            new_method = Method(
                family=family,
                revision_name=method_form.cleaned_data['revision_name'],
                revision_desc=method_form.cleaned_data['revision_desc'],
                revision_parent=parent_method,
                driver=coderesource_revision,
                reusable=method_form.cleaned_data['reusable'],
                user=creating_user
            )
            new_method.save()

            for user in method_form.cleaned_data["users_allowed"]:
                new_method.users_allowed.add(user)
            for group in method_form.cleaned_data["groups_allowed"]:
                new_method.groups_allowed.add(group)

            # Attempt to make in/outputs.
            num_outputs = len(output_forms)
            if num_outputs == 0:
                method_form.add_error(None, "You must specify at least one output.")
                raise ValidationError("You must specify at least one output.")

            for xput_type in ("in", "out"):
                curr_forms = input_forms
                if xput_type == "out":
                    curr_forms = output_forms

                for form_tuple in curr_forms:
                    t_form = form_tuple[0]
                    xs_form = form_tuple[1]
                    dataset_name = t_form.cleaned_data["dataset_name"]
                    cdt_id = xs_form.cleaned_data["compounddatatype"]

                    if dataset_name == '' and cdt_id == '':
                        # ignore blank form
                        continue

                    my_compound_datatype = None
                    min_row = None
                    max_row = None
                    if cdt_id != '__raw__':
                        try:
                            my_compound_datatype = CompoundDatatype.objects.get(pk=cdt_id)
                            min_row = xs_form.cleaned_data["min_row"]
                            max_row = xs_form.cleaned_data["max_row"]
                        except (ValueError, CompoundDatatype.DoesNotExist) as e:
                            xs_form.add_error("compounddatatype", e)
                            raise e

                    curr_xput = new_method.create_xput(
                        dataset_name=dataset_name,
                        compounddatatype=my_compound_datatype,
                        row_limits=(min_row, max_row),
                        input=(xput_type == "in"),
                        clean=False
                    )

                    if cdt_id != "__raw__":
                        try:
                            curr_xput.structure.clean()
                        except ValidationError as e:
                            xs_form.add_error(None, e)
                            raise e

                    try:
                        curr_xput.clean()
                    except ValidationError as e:
                        t_form.add_error(None, e)
                        raise e

            try:
                new_method.complete_clean()
            except ValidationError as e:
                method_form.add_error(None, e)
                raise e

    except ValidationError:
        return None

    return new_method


def _method_forms_check_valid(family_form, method_form, input_form_tuples, output_form_tuples):
    """
    Helper that validates all forms returned from create_method_forms.
    """
    in_xput_forms, in_struct_forms = zip(*input_form_tuples)
    out_xput_forms, out_struct_forms = zip(*output_form_tuples)
    all_forms = ([family_form] + [method_form] + list(in_xput_forms) + list(in_struct_forms) +
                 list(out_xput_forms) + list(out_struct_forms))
    return all(x.is_valid() for x in all_forms)


@login_required
@user_passes_test(developer_check)
def method_add(request, id=None):
    """
    Generate forms for adding Methods, and validate and process POST data returned
    by the user.  Allows for an arbitrary number of input and output forms.

    [id] : User is adding a new Method to an existing family
           without a specified parent Method (different CodeResource)
           If id is None, then user is creating a new MethodFamily.
    """
    creating_user = request.user
    if id:
        four_oh_four = False
        try:
            this_family = MethodFamily.objects.get(pk=id)
            if not this_family.can_be_accessed(creating_user):
                four_oh_four = True
        except MethodFamily.DoesNotExist:
            four_oh_four = True
        if four_oh_four:
            raise Http404("ID {} is inaccessible".format(id))

        header = "Add a new Method to MethodFamily '%s'" % this_family.name
    else:
        this_family = None
        header = 'Start a new MethodFamily with an initial Method'

    t = loader.get_template('method/method_add.html')
    c = RequestContext(request)
    if request.method == 'POST':
        family_form, method_form, input_form_tuples, output_form_tuples = create_method_forms(
            request.POST, creating_user, family=this_family)
        if not _method_forms_check_valid(family_form, method_form, input_form_tuples, output_form_tuples):
            # Bail out now if there are any problems.
            c.update(
                {
                    'family_form': family_form,
                    'method_form': method_form,
                    'input_forms': input_form_tuples,
                    'output_forms': output_form_tuples,
                    'family': this_family,
                    'header': header
                })
            return HttpResponse(t.render(c))

        # Next, attempt to build the Method and its associated MethodFamily (if necessary),
        # inputs, and outputs.
        create_method_from_forms(
            family_form, method_form, input_form_tuples, output_form_tuples, creating_user,
            family=this_family
        )

        if _method_forms_check_valid(family_form, method_form, input_form_tuples, output_form_tuples):
            # Success!
            if id:
                return HttpResponseRedirect('/methods/{}'.format(id))
            else:
                return HttpResponseRedirect('/method_families')

    else:
        # Prepare a blank set of forms for rendering.
        family_form = MethodFamilyForm()
        method_form = MethodForm(user=creating_user)
        input_form_tuples = [
            (TransformationXputForm(auto_id='id_%s_in_0'), XputStructureForm(user=creating_user,
                                                                             auto_id='id_%s_in_0'))
        ]
        output_form_tuples = [
            (TransformationXputForm(auto_id='id_%s_out_0'), XputStructureForm(user=creating_user,
                                                                              auto_id='id_%s_out_0'))
        ]

    c.update(
        {
            'family_form': family_form,
            'method_form': method_form,
            'input_forms': input_form_tuples,
            'output_forms': output_form_tuples,
            'family': this_family,
            'header': header
        })
    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def method_revise(request, id):
    """
    Add a revision of an existing Method.  revision_parent is defined by the
    previous version.
    """
    t = loader.get_template('method/method_revise.html')
    c = RequestContext(request)
    creating_user = request.user

    # Retrieve the most recent member of this Method's family.
    four_oh_four = False
    try:
        parent_method = Method.objects.get(pk=id)
        if not parent_method.can_be_accessed(creating_user):
            four_oh_four = True
    except Method.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        raise Http404("ID {} is inaccessible".format(id))

    family = parent_method.family

    # Retrieve the most recent revision of the corresponding CR.
    parent_revision = parent_method.driver
    this_code_resource = parent_revision.coderesource
    # Filter the available revisions by user.
    user_plus = metadata.models.KiveUser.kiveify(creating_user)
    all_revisions = this_code_resource.revisions.filter(user_plus.access_query()).order_by('-revision_DateTime')

    if request.method == 'POST':
        # Because there is no CodeResource specified, the second value is of type MethodReviseForm.
        family_form, method_revise_form, input_form_tuples, output_form_tuples = create_method_forms(
            request.POST, creating_user, family=family)
        if not _method_forms_check_valid(family_form, method_revise_form, input_form_tuples, output_form_tuples):
            # Bail out now if there are any problems.
            c.update(
                {
                    'coderesource': this_code_resource,
                    'method_revise_form': method_revise_form,
                    'input_forms': input_form_tuples,
                    'output_forms': output_form_tuples,
                    'family': family,
                    'family_form': family_form,
                    'parent': parent_method
                })
            return HttpResponse(t.render(c))

        # Next, attempt to build the Method and add it to family.
        create_method_from_forms(
            family_form, method_revise_form, input_form_tuples, output_form_tuples, creating_user,
            family=family, parent_method=parent_method
        )
        if _method_forms_check_valid(family_form, method_revise_form, input_form_tuples, output_form_tuples):
            # Success!
            return HttpResponseRedirect('/methods/{}'.format(family.pk))

    else:
        # initialize forms with values of parent Method
        family_form = MethodFamilyForm(request.POST)
        method_revise_form = MethodReviseForm(
            initial={
                'revision_desc': parent_method.revision_desc,
                'revisions': parent_revision.pk,
                'reusable': parent_method.reusable
            })
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
                    xs_form = XputStructureForm(user=creating_user,
                                                auto_id='id_%s_{}_{}'.format(xput_type, len(forms)),
                                                initial={'compounddatatype': structure.compounddatatype.id,
                                                         'min_row': structure.min_row,
                                                         'max_row': structure.max_row})
                else:
                    xs_form = XputStructureForm(user=creating_user,
                                                auto_id='id_%s_{}_{}'.format(xput_type, len(forms)),
                                                initial={'compounddatatype': '__raw__'})

                forms.append((tx_form, xs_form))
            xput_forms.append(forms)

        input_form_tuples, output_form_tuples = xput_forms
        # if previous Method has no inputs, provide blank forms
        if len(input_form_tuples) == 0:
            tx_form = TransformationXputForm(auto_id='id_%s_in_0')
            xs_form = XputStructureForm(user=creating_user, auto_id='id_%s_in_0')
            input_form_tuples.append((tx_form, xs_form))

    method_revise_form.fields['revisions'].widget.choices = [
        (str(x.id), '{}: {}'.format(x.revision_number, x.revision_name)) for x in all_revisions
    ]
    c.update({'coderesource': this_code_resource,
              'method_revise_form': method_revise_form,
              'input_forms': input_form_tuples,
              'output_forms': output_form_tuples,
              'family': family,
              'family_form': family_form,
              'parent': parent_method})
    return HttpResponse(t.render(c))
