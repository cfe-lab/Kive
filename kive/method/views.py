"""
method.views
"""

from django.db import transaction
from django.core.exceptions import ValidationError, ObjectDoesNotExist
from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import HttpResponse, HttpResponseRedirect, Http404
from django.template import loader

from datetime import datetime
import logging
import itertools

import metadata.models
from metadata.models import CompoundDatatype
from method.models import CodeResource, Method, MethodDependency,\
    MethodFamily, CodeResourceRevision
from method.forms import CodeResourcePrototypeForm, CodeResourceRevisionForm, \
    CodeResourceDetailsForm, CodeResourceRevisionDetailsForm, \
    MethodFamilyForm, MethodForm, MethodReviseForm, MethodDependencyForm, \
    MethodDetailsForm, TransformationXputForm, XputStructureForm
from portal.views import developer_check, admin_check


LOGGER = logging.getLogger(__name__)


@login_required
@user_passes_test(developer_check)
def resources(request):
    """
    Display a list of all code resources (parents) in database
    """
    t = loader.get_template('method/resources.html')
    c = {
        "is_user_admin": admin_check(request.user)
    }

    return HttpResponse(t.render(c, request))


@login_required
@user_passes_test(developer_check)
def resource_revisions(request, id):
    """
    Display a list of all revisions of a specific Code Resource in database.
    """
    four_oh_four = False
    try:
        coderesource = CodeResource.objects.get(pk=id)
        if not coderesource.can_be_accessed(request.user):
            four_oh_four = True
    except ObjectDoesNotExist:
        four_oh_four = True

    if four_oh_four:
        # Redirect back to the resources page.
        raise Http404("ID {} cannot be accessed".format(id))

    addable_users, addable_groups = coderesource.other_users_groups()

    if request.method == 'POST':
        # We are attempting to update the CodeResource's metadata/permissions.
        resource_form = CodeResourceDetailsForm(
            request.POST,
            addable_users=addable_users,
            addable_groups=addable_groups,
            instance=coderesource
        )

        if resource_form.is_valid():
            try:
                coderesource.name = resource_form.cleaned_data["name"]
                coderesource.description = resource_form.cleaned_data["description"]
                coderesource.clean()
                coderesource.save()
                coderesource.grant_from_json(resource_form.cleaned_data["permissions"])

                # Success -- go back to the resources page.
                return HttpResponseRedirect('/resources')
            except (AttributeError, ValidationError, ValueError) as e:
                LOGGER.exception(e.message)
                resource_form.add_error(None, e)

    else:
        resource_form = CodeResourceDetailsForm(
            addable_users=addable_users,
            addable_groups=addable_groups,
            initial={"name": coderesource.name, "description": coderesource.description}
        )

    # Cast request.user to class KiveUser & grab data
    curr_user = metadata.models.KiveUser.kiveify(request.user)
    revisions = coderesource.revisions.filter(curr_user.access_query()).\
        distinct().order_by('-revision_number')
    if len(revisions) == 0:
        # Go to the resource_revision_add page to create a first revision.
        t = loader.get_template('method/resource_revision_add.html')
        crv_form = CodeResourceRevisionForm()

        c = {
            'revision_form': crv_form,
            'parent_revision': None,
            'coderesource': coderesource,
        }
        return HttpResponse(t.render(c, request))

    # Load template, setup context
    t = loader.get_template('method/resource_revisions.html')
    c = {
        'coderesource': coderesource,
        "resource_form": resource_form,
        'revisions': revisions,
        'is_admin': admin_check(request.user),
        "is_owner": request.user == coderesource.user
    }
    return HttpResponse(t.render(c, request))


@transaction.atomic
def _make_crv(file_in_memory,
              creating_user,
              crv_form,
              parent_revision=None,
              code_resource=None):
    """
    Helper that creates a CodeResourceRevision (and a CodeResource as well if appropriate).
    """
    assert isinstance(crv_form, (CodeResourcePrototypeForm, CodeResourceRevisionForm))
    # If parent_revision is specified, we are only making a CodeResourceRevision and not its parent CodeResource.
    assert not (parent_revision is None and isinstance(crv_form, CodeResourceRevision))

    cr_filename = "" if file_in_memory is None else file_in_memory.name

    if code_resource is None and parent_revision is not None:
        code_resource = parent_revision.coderesource
    if code_resource is None:
        # crv_form is a CodeResourcePrototypeForm.
        code_resource = CodeResource(
            name=crv_form.cleaned_data['resource_name'],
            description=crv_form.cleaned_data['resource_desc'],
            filename=cr_filename,
            user=creating_user
        )
        try:
            code_resource.full_clean()
            # Skip the clean until later; after all, we're protected by a transaction here.
            code_resource.save()
        except ValidationError as e:
            crv_form.add_error('content_file', e.error_dict.get('filename', []))
            crv_form.add_error('resource_name', e.error_dict.get('name', []))
            crv_form.add_error('resource_desc', e.error_dict.get('description', []))
            raise e

        code_resource.grant_from_json(crv_form.cleaned_data["permissions"])

        rev_name = "Prototype"
        rev_desc = crv_form.cleaned_data["resource_desc"]
    else:
        rev_name = crv_form.cleaned_data["revision_name"]
        rev_desc = crv_form.cleaned_data["revision_desc"]

    # Modify actual filename prior to saving revision object.
    if file_in_memory is not None:
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
    revision.grant_from_json(crv_form.cleaned_data["permissions"])

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
    creating_user = request.user

    if request.method != 'POST':
        resource_form = CodeResourcePrototypeForm()
    else:
        # Using forms here provides validation and better parsing of parameters in the request.
        resource_form = CodeResourcePrototypeForm(request.POST, request.FILES)

        if resource_form.is_valid():
            # Now we can try to create objects in the database, catching backend-raised exceptions as we go.
            try:
                _make_crv(request.FILES.get("content_file", None),
                          creating_user,
                          resource_form)

                # Success -- return to the resources root page.
                return HttpResponseRedirect('/resources')
            except ValidationError:
                # All forms have the appropriate errors attached.
                pass

    t = loader.get_template('method/resource_add.html')
    c = {
        'resource_form': resource_form,
    }
    return HttpResponse(t.render(c, request))


@login_required
@user_passes_test(developer_check)
def resource_revision_add(request, id):
    """
    Add a code resource revision.  The form will initially be populated with values of the last
    revision to this code resource.
    """
    t = loader.get_template('method/resource_revision_add.html')
    c = {}
    creating_user = request.user

    # Use POST information (id) to retrieve the CRv being revised.
    four_oh_four = False
    try:
        parent_revision = CodeResourceRevision.objects.get(pk=id)
        if not parent_revision.can_be_accessed(creating_user):
            four_oh_four = True
    except ObjectDoesNotExist:
        four_oh_four = True

    if four_oh_four:
        # Redirect back to the resources page.
        raise Http404("ID {} cannot be accessed".format(id))

    coderesource = parent_revision.coderesource

    if request.method == 'POST':
        # Use forms here, just as in resource_add.  Again note that entries of dep_forms may be None.
        revision_form = CodeResourceRevisionForm(request.POST, request.FILES)
        if not revision_form.is_valid():
            c.update({
                'revision_form': revision_form,
                'parent_revision': parent_revision,
                'coderesource': coderesource
            })
            return HttpResponse(t.render(c, request))

        try:
            _make_crv(request.FILES.get('content_file', None), creating_user, revision_form,
                      parent_revision=parent_revision)
        except ValidationError:
            # The forms have all been updated with the appropriate errors.
            c.update(
                {
                    'revision_form': revision_form,
                    'parent_revision': parent_revision,
                    'coderesource': coderesource
                })
            return HttpResponse(t.render(c, request))  # CodeResourceRevision object required for next steps

        # Success; return to the resources page.
        return HttpResponseRedirect('/resources')

    # Having reached here, we know that this CR is being revised.  Return a form pre-populated
    # with default info.
    parent_users_allowed = [x.username for x in parent_revision.users_allowed.all()]
    parent_groups_allowed = [x.name for x in parent_revision.groups_allowed.all()]
    crv_form = CodeResourceRevisionForm(
        initial={
            "permissions": [parent_users_allowed, parent_groups_allowed]
        }
    )

    c.update(
        {
            'revision_form': crv_form,
            'parent_revision': parent_revision,
            'coderesource': coderesource
        }
    )
    return HttpResponse(t.render(c, request))


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

    addable_users, addable_groups = revision.other_users_groups()

    if request.method == 'POST':
        # We are attempting to update the CodeResourceRevision's metadata/permissions.
        revision_form = CodeResourceRevisionDetailsForm(
            request.POST,
            addable_users=addable_users,
            addable_groups=addable_groups,
            instance=revision
        )

        if revision_form.is_valid():
            try:
                revision.revision_name = revision_form.cleaned_data["revision_name"]
                revision.revision_desc = revision_form.cleaned_data["revision_desc"]
                revision.save()
                revision.grant_from_json(revision_form.cleaned_data["permissions"])
                revision.clean()

                # Success -- go back to the CodeResource page.
                return HttpResponseRedirect('/resource_revisions/{}'.format(revision.coderesource.pk))
            except (AttributeError, ValidationError, ValueError) as e:
                LOGGER.exception(e.message)
                revision_form.add_error(None, e)

    else:
        revision_form = CodeResourceRevisionDetailsForm(
            addable_users=addable_users,
            addable_groups=addable_groups,
            initial={
                "revision_name": revision.revision_name,
                "revision_desc": revision.revision_desc
            }
        )

    t = loader.get_template("method/resource_revision_view.html")
    c = {
        "revision": revision,
        "revision_form": revision_form,
        "is_owner": revision.user == request.user,
        "is_admin": admin_check(request.user)
    }
    return HttpResponse(t.render(c, request))


@login_required
@user_passes_test(developer_check)
def method_families(request):
    """
    Display a list of all MethodFamily objects in database.
    """
    t = loader.get_template("method/method_families.html")
    c = {
        "is_user_admin": admin_check(request.user)
    }
    return HttpResponse(t.render(c, request))


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
    except ObjectDoesNotExist:
        four_oh_four = True

    if four_oh_four:
        # Redirect back to the resources page.
        raise Http404("ID {} cannot be accessed".format(id))

    addable_users, addable_groups = family.other_users_groups()

    if request.method == 'POST':
        # We are attempting to update the MethodFamily's metadata/permissions.
        mf_form = MethodFamilyForm(
            request.POST,
            addable_users=addable_users,
            addable_groups=addable_groups,
            instance=family
        )

        if mf_form.is_valid():
            try:
                family.name = mf_form.cleaned_data["name"]
                family.description = mf_form.cleaned_data["description"]
                family.save()
                family.grant_from_json(mf_form.cleaned_data["permissions"])
                family.clean()

                # Success -- go back to the resources page.
                return HttpResponseRedirect('/method_families')
            except (AttributeError, ValidationError, ValueError) as e:
                LOGGER.exception(e.message)
                mf_form.add_error(None, e)

    else:
        mf_form = MethodFamilyForm(
            addable_users=addable_users,
            addable_groups=addable_groups,
            initial={"name": family.name, "description": family.description}
        )

    t = loader.get_template('method/methods.html')
    c = {
        "family": family,
        "family_form": mf_form,
        "is_admin": admin_check(request.user),
        "is_owner": request.user == family.user
    }
    return HttpResponse(t.render(c, request))


def _make_dep_forms(query_dict, user):
    """
    Helper for resource_add and resource_revision_add that creates the MethodDependencyForms.
    """
    num_dep_forms = sum([1 for k in query_dict.iterkeys() if k.startswith('coderesource_')])
    dep_forms = []
    for i in range(num_dep_forms):
        this_cr = query_dict['coderesource_'+str(i)]  # PK of the Method
        if this_cr == '':
            # Ignore blank CR dependency forms.
            continue

        dep_forms.append(
            MethodDependencyForm(
                {
                    'coderesource': query_dict['coderesource_'+str(i)],
                    'revisions': query_dict['revisions_'+str(i)],
                    'path': query_dict['path_'+str(i)],
                    'filename': query_dict['filename_'+str(i)]
                },
                user=user,
                auto_id='id_%s_'+str(i)
            )
        )
    return dep_forms


def create_method_forms(request_post, user, family=None):
    """
    Helper function for method_add() that creates Forms from the
    provided information and validates them.
    """
    query_dict = request_post.dict()
    if 'name' in query_dict:
        assert family is None
        family = MethodFamily(user=user)
        family_form = MethodFamilyForm(request_post, instance=family)
    else:
        assert family is not None
        family_form = MethodFamilyForm(
            {
                "name": family.name,
                "description": family.description
            },
            instance=family
        )
    family_form.is_valid()

    # Populate main form with submitted values.
    if "coderesource" in query_dict:
        method_form = MethodForm(request_post, user=user)
    else:
        method_form = MethodReviseForm(request_post)
    method_form.is_valid()
    # Determine whether the confirm_shebang button has been clicked.
    # NOTE: confirm_shebang is a checkbox html element; according to HTML spec, it will only
    # be present iff its true. ==> we cannot rely on it being present, and set the value
    # to false if it is absent.
    has_override = (request_post.get("confirm_shebang", 'off') == 'on')
    # NOTE: for shebang_val, we must differentiate between yes, no and undefined
    shebang_val = _get_shebang_code(method_form)
    has_shebang = (shebang_val == SHEBANG_YES)
    show_shebang_field = query_dict["SHOW_SHEBANG_FIELD"] = (shebang_val == SHEBANG_NO)
    query_dict["SHEBANG_OK"] = (has_shebang or has_override)

    etxt = """The code resource should be executable, which means that the file usually starts
with a shebang: '#!'. The currently selected code resource does not.
If you know what you are doing, you can override this requirement here."""
    if show_shebang_field and not has_override:
        method_form.add_error("confirm_shebang", etxt)

    # Populate in/output forms with submitted values.
    input_forms = []
    output_forms = []
    for xput_type, formlst in [("in", input_forms), ("out", output_forms)]:
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
            formlst.append((t_form, xs_form))

    dep_forms = _make_dep_forms(request_post.dict(), user)
    # the methods must have at least one input and output each.
    if len(input_forms) == 0:
        tx_form = TransformationXputForm(auto_id='id_%s_in_0')
        xs_form = XputStructureForm(user=user, auto_id='id_%s_in_0')
        input_forms.append((tx_form, xs_form))
    if len(output_forms) == 0:
        tx_form = TransformationXputForm(auto_id='id_%s_out_0')
        xs_form = XputStructureForm(user=user, auto_id='id_%s_out_0')
        output_forms.append((tx_form, xs_form))

    return family_form, method_form, dep_forms, input_forms, output_forms, query_dict

SHEBANG_YES = 1
SHEBANG_NO = 2
SHEBANG_UNDEF = 3


def _get_shebang_code(method_form):
    # Retrieve the CodeResource revision and return whether it has a #! on its first line or not.
    # This routine can return three different values:
    # SHEBANG_UNDEF: resource driver is defined
    # SHEBANG_YES, SHEBANG_NO: code resource is defined, and 'code resource has a shebang'
    # This logic is required because a form can be submitted with an empty code_resource
    # field. In that case, the whole form will fail, but we need to know, independently of
    # that form failing, whether we should display the 'no shebang override' button to the user.
    try:
        coderesource_revision = CodeResourceRevision.objects.get(pk=method_form.cleaned_data['driver_revisions'])
    except (KeyError, ValueError, CodeResourceRevision.DoesNotExist):
        return SHEBANG_UNDEF
    # We do have a code resource defined;
    # now check to see whether the driver code begins with a shebang
    try:
        coderesource_revision.content_file.open()
        first_line = coderesource_revision.content_file.file.readline()
        coderesource_revision.content_file.close()
    except Exception as exc:
        method_form.add_error("driver_revisions", exc)
        return SHEBANG_UNDEF
    tdct = {True: SHEBANG_YES, False: SHEBANG_NO}
    return tdct[first_line.startswith("#!")]


def create_method_from_forms(family_form, method_form, dep_forms, input_forms, output_forms, creating_user,
                             family=None, parent_method=None):
    """
    Given Forms representing the MethodFamily, Method, inputs, and outputs, create a Method.

    Warning: this routine has side effects (it can mod its arguments):
    If an error occurs:
        return None and update the forms with errors.
    else:
        return the new method and the forms are returned without modification.
    """
    # This assures that not both family_form and family are None.
    assert family is not None or family_form is not None

    for dep_form in dep_forms:
        assert isinstance(dep_form, MethodDependencyForm) or dep_form is None

    # Retrieve the CodeResource revision as driver.
    try:
        coderesource_revision = CodeResourceRevision.objects.get(pk=method_form.cleaned_data['driver_revisions'])
    except (KeyError, ValueError, CodeResourceRevision.DoesNotExist) as e:
        method_form.add_error("driver_revisions", e)
        return None

    new_method = None
    try:
        # Note how the except blocks re-raise their exception: that is to terminate
        # this transaction.
        with transaction.atomic():
            if family is None:
                try:
                    family = family_form.save()
                    family.grant_from_json(method_form.cleaned_data["permissions"])
                    family.full_clean()

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
                user=creating_user,
                threads=method_form.cleaned_data["threads"]
            )
            new_method.save()

            new_method.grant_from_json(method_form.cleaned_data["permissions"])

            # Bind dependencies.
            for i in range(len(dep_forms)):
                if dep_forms[i] is None:
                    continue
                try:
                    on_revision = CodeResourceRevision.objects.get(
                        pk=dep_forms[i].cleaned_data["revisions"])
                    dependency = MethodDependency(
                        method=new_method,
                        requirement=on_revision,
                        path=dep_forms[i].cleaned_data["path"],
                        filename=dep_forms[i].cleaned_data["filename"]
                    )
                    dependency.full_clean()
                    dependency.save()
                except ValidationError as e:
                    dep_forms[i].add_error(None, e)
                    raise e

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


def _method_forms_check_valid(family_form, method_form, dep_forms,
                              input_form_tuples, output_form_tuples):
    """
    Helper that validates all forms returned from create_method_forms.
    """
    in_xput_forms, in_struct_forms = zip(*input_form_tuples)
    out_xput_forms, out_struct_forms = zip(*output_form_tuples)
    all_forms = ([family_form] + [method_form] + dep_forms +
                 list(in_xput_forms) + list(in_struct_forms) +
                 list(out_xput_forms) + list(out_struct_forms))
    return all(x.is_valid() for x in all_forms)


@login_required
@user_passes_test(developer_check)
def method_view(request, id):
    """
    View a Method or edit its metadata/permissions.
    """
    four_oh_four = False
    try:
        method = Method.objects.get(pk=id)
        if not method.can_be_accessed(request.user):
            four_oh_four = True
    except Method.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        raise Http404("ID {} is not accessible".format(id))

    addable_users, addable_groups = method.other_users_groups()
    addable_users, addable_groups = method.family.intersect_permissions(addable_users, addable_groups)
    if method.revision_parent is not None:
        addable_users, addable_groups = method.revision_parent.intersect_permissions(addable_users, addable_groups)
    addable_users, addable_groups = method.driver.intersect_permissions(addable_users, addable_groups)
    for dep in method.dependencies.all():
        addable_users, addable_groups = dep.requirement.intersect_permissions(addable_users, addable_groups)
    for xput in itertools.chain(method.inputs.all(), method.outputs.all()):
        xput_cdt = xput.get_cdt()
        if xput_cdt is not None:
            addable_users, addable_groups = xput_cdt.intersect_permissions(addable_users, addable_groups)

    if request.method == 'POST':
        # We are attempting to update the Method's metadata/permissions.
        method_form = MethodDetailsForm(
            request.POST,
            addable_users=addable_users,
            addable_groups=addable_groups,
            instance=method
        )

        if method_form.is_valid():
            try:
                method.revision_name = method_form.cleaned_data["revision_name"]
                method.revision_desc = method_form.cleaned_data["revision_desc"]
                method.save()
                method.grant_from_json(method_form.cleaned_data["permissions"])
                method.clean()

                # Success -- go back to the CodeResource page.
                return HttpResponseRedirect('/methods/{}'.format(method.family.pk))
            except (AttributeError, ValidationError, ValueError) as e:
                LOGGER.exception(e.message)
                method_form.add_error(None, e)

    else:
        method_form = MethodDetailsForm(
            addable_users=addable_users,
            addable_groups=addable_groups,
            initial={
                "revision_name": method.revision_name,
                "revision_desc": method.revision_desc
            }
        )

    t = loader.get_template("method/method_view.html")
    c = {
        "method": method,
        "method_form": method_form,
        "is_owner": method.user == request.user,
        "is_admin": admin_check(request.user)
    }
    return HttpResponse(t.render(c, request))


@login_required
@user_passes_test(developer_check)
def method_new(request):
    """
    Generate/validate/process forms creating a new MethodFamily and initial Method.

    Allows for an arbitrary number of input and output forms.
    """
    return _method_creation_helper(request, method_family=None)


@login_required
@user_passes_test(developer_check)
def method_add(request, id):
    """
    Generate/validate/process forms for adding a Method to an existing MethodFamily.

    Allows for an arbitrary number of input and output forms.

    [id] : primary key of the MethodFamily that this Method is being added to.
    """
    creating_user = request.user

    four_oh_four = False
    try:
        this_family = MethodFamily.objects.get(pk=id)
        if not this_family.can_be_accessed(creating_user):
            four_oh_four = True
    except MethodFamily.DoesNotExist:
        four_oh_four = True
    if four_oh_four:
        raise Http404("ID {} is inaccessible".format(id))

    return _method_creation_helper(request, method_family=this_family)


def _method_creation_helper(request, method_family=None):
    """
    Helper for method_new and method_add.

    [request]: the request that actually came in.
    [method_family]: the MethodFamily to add to if applicable, None otherwise.
    """
    creating_user = request.user

    if method_family:
        header = "Add a new Method to MethodFamily '%s'" % method_family.name
    else:
        header = 'Start a new MethodFamily with an initial Method'
    t = loader.get_template('method/method.html')
    if request.method == 'POST':
        family_form, method_form, dep_forms,\
            input_form_tuples, output_form_tuples,\
            query_dict = create_method_forms(request.POST, creating_user, family=method_family)
        forms_are_valid = _method_forms_check_valid(family_form,
                                                    method_form, dep_forms,
                                                    input_form_tuples, output_form_tuples)
        display_shebang_button = query_dict["SHOW_SHEBANG_FIELD"]
        shebang_ok = query_dict["SHEBANG_OK"]
        if not (forms_are_valid and shebang_ok):
            # Bail out now if a) the forms are invalid, or we need to allow
            # the user to override a missing shebang in the driver code_resource
            if not dep_forms:
                dep_forms = [MethodDependencyForm(user=creating_user, auto_id='id_%s_0')]
            c = {'show_shebang_button': display_shebang_button,
                 'family_form': family_form,
                 'method_form': method_form,
                 'dep_forms': dep_forms,
                 'input_forms': input_form_tuples,
                 'output_forms': output_form_tuples,
                 'family': method_family,
                 'header': header
                 }
            return HttpResponse(t.render(c, request))
        else:
            # We are happy with the input, now attempt to build the Method and
            # its associated MethodFamily (if necessary), inputs, and outputs.
            just_created = create_method_from_forms(family_form, method_form,
                                                    dep_forms, input_form_tuples,
                                                    output_form_tuples, creating_user,
                                                    family=method_family)
            if just_created is None:
                # creation failed: the forms have been modded by create_method_from_forms
                # so just show the user these.
                if not dep_forms:
                    dep_forms = [MethodDependencyForm(user=creating_user, auto_id='id_%s_0')]
                c = {'show_shebang_button': display_shebang_button,
                     'family_form': family_form,
                     'method_form': method_form,
                     'dep_forms': dep_forms,
                     'input_forms': input_form_tuples,
                     'output_forms': output_form_tuples,
                     'family': method_family,
                     'header': header + ": ERROR"
                     }
                return HttpResponse(t.render(c, request))
            else:
                # Success!
                return HttpResponseRedirect('/methods/{}'.format(just_created.family.pk), request)
    else:
        # not a POST: Just prepare a blank set of forms for rendering.
        family_form = MethodFamilyForm()
        method_form = MethodForm(user=creating_user)

        dep_forms = [MethodDependencyForm(user=creating_user, auto_id='id_%s_0')]
        input_form_tuples = [
            (TransformationXputForm(auto_id='id_%s_in_0'),
             XputStructureForm(user=creating_user, auto_id='id_%s_in_0'))
        ]
        output_form_tuples = [
            (TransformationXputForm(auto_id='id_%s_out_0'),
             XputStructureForm(user=creating_user, auto_id='id_%s_out_0'))
        ]
        c = {'show_shebang_button': False,
             'family_form': family_form,
             'method_form': method_form,
             'dep_forms': dep_forms,
             'input_forms': input_form_tuples,
             'output_forms': output_form_tuples,
             'family': method_family,
             'header': header
             }
        return HttpResponse(t.render(c, request))
    # ---should not fall off the end of this routine
    raise RuntimeError("error in METHOD")


@login_required
@user_passes_test(developer_check)
def method_revise(request, id):
    """
    Add a revision of an existing Method.  revision_parent is defined by the
    previous version.
    """
    t = loader.get_template('method/method.html')
    c = {}
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
        family_form, method_revise_form,\
            dep_forms, input_form_tuples,\
            output_form_tuples, _ = create_method_forms(request.POST, creating_user, family=family)
        if not _method_forms_check_valid(family_form, method_revise_form, dep_forms,
                                         input_form_tuples, output_form_tuples):
            # Bail out now if there are any problems.
            c.update(
                {
                    'coderesource': this_code_resource,
                    'method_revise_form': method_revise_form,
                    'dep_forms': dep_forms,
                    'input_forms': input_form_tuples,
                    'output_forms': output_form_tuples,
                    'family': family,
                    'family_form': family_form,
                    'parent': parent_method
                })
            return HttpResponse(t.render(c, request))

        # Next, attempt to build the Method and add it to family.
        create_method_from_forms(
            family_form, method_revise_form, dep_forms, input_form_tuples, output_form_tuples, creating_user,
            family=family, parent_method=parent_method
        )
        if _method_forms_check_valid(family_form, method_revise_form, dep_forms,
                                     input_form_tuples, output_form_tuples):
            # Success!
            return HttpResponseRedirect('/methods/{}'.format(family.pk))

    else:
        # initialize forms with values of parent Method
        family_form = MethodFamilyForm({"name": family.name, "description": family.description})
        parent_users_allowed = [x.username for x in parent_method.users_allowed.all()]
        parent_groups_allowed = [x.name for x in parent_method.groups_allowed.all()]
        method_revise_form = MethodReviseForm(
            initial={
                "revision_desc": parent_method.revision_desc,
                "driver_revisions": parent_revision.pk,
                "reusable": parent_method.reusable,
                "threads": parent_method.threads,
                "permissions": [parent_users_allowed, parent_groups_allowed]
            })

        dependencies = parent_method.dependencies.all()
        dep_forms = []
        for i, dependency in enumerate(dependencies):
            its_crv = dependency.requirement
            its_cr = its_crv.coderesource
            dep_form = MethodDependencyForm(
                user=creating_user,
                auto_id='id_%s_'+str(i),
                initial={
                    'coderesource': its_cr.pk,
                    'revisions': its_crv.pk,
                    'path': dependency.path,
                    'filename': dependency.filename
                }
            )
            dep_forms.append(dep_form)
        # If the parent Method has no dependencies, add a blank form.
        if len(dep_forms) == 0:
            dep_forms.append(MethodDependencyForm(user=creating_user, auto_id='id_%s_0'))

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

    method_revise_form.fields['driver_revisions'].widget.choices = [
        (str(x.id), '{}: {}'.format(x.revision_number, x.revision_name)) for x in all_revisions
    ]
    c.update(
        {
            'coderesource': this_code_resource,
            'method_form': method_revise_form,
            'dep_forms': dep_forms,
            'input_forms': input_form_tuples,
            'output_forms': output_form_tuples,
            'family': family,
            'family_form': family_form,
            'parent': parent_method
        }
    )
    return HttpResponse(t.render(c, request))
