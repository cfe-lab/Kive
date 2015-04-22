"""
metadata.views
"""

from django.http import HttpResponse, HttpResponseRedirect, Http404
from django.template import loader, RequestContext
from django.core.exceptions import ValidationError
from django.utils import timezone
from django.db import transaction
from django.contrib.auth.decorators import login_required, user_passes_test

import re

from constants import datatypes as dt_pks
from metadata.forms import CompoundDatatypeForm, CompoundDatatypeMemberForm, \
    DatatypeForm, IntegerConstraintForm, StringConstraintForm
from metadata.models import BasicConstraint, CompoundDatatype, \
    CompoundDatatypeMember, Datatype, get_builtin_types
from portal.views import developer_check


@login_required
@user_passes_test(developer_check)
def datatypes(request):
    """
    Render table and form on user request for datatypes.html
    """
    # Re-cast request.user to our proxy class.
    accessible_dts = Datatype.filter_by_user(request.user)
    t = loader.get_template('metadata/datatypes.html')
    c = RequestContext(request, {'datatypes': accessible_dts})
    return HttpResponse(t.render(c))
    #return render_to_response('datatypes.html', {'form': form})


@login_required
@user_passes_test(developer_check)
def datatype_add(request):
    """
    Render form for creating a new Datatype
    """
    t = loader.get_template('metadata/datatype_add.html')
    c = RequestContext(request)

    # A dummy Datatype to be used for filling in a DatatypeForm.
    dt = Datatype(user=request.user, date_created=timezone.now())

    if request.method == 'POST':
        # dt = Datatype(user=request.user, date_created=timezone.now())
        dform = DatatypeForm(request.POST, instance=dt) #  create form bound to POST data
        icform = IntegerConstraintForm(request.POST)
        scform = StringConstraintForm(request.POST)
        query = request.POST.dict()

        Python_type = None
        bail_now = False
        if not (dform.is_valid() and icform.is_valid() and scform.is_valid()):
            # Bail out: return to the same page, but now error messages will now display.
            bail_now = True
        else:
            Python_type = get_builtin_types(dform.cleaned_data["restricts"])
            if len(Python_type) != 1:
                dform.add_error("restricts", ValidationError("Incompatible restriction of Datatypes"))
                bail_now = True

        if bail_now:
            c.update({'datatype_form': dform, 'int_con_form': icform, 'str_con_form': scform})
            return HttpResponse(t.render(c))

        Python_type = Python_type.pop()
        # At this point we know all the fields are valid.
        try:
            with transaction.atomic():
                new_datatype = dform.save() #  this has to be saved to database to be passed to BasicConstraint()

                # Manually create and validate BasicConstraint objects.

                # The Shipyard builtins.
                STR = Datatype.objects.get(pk=dt_pks.STR_PK)
                INT = Datatype.objects.get(pk=dt_pks.INT_PK)
                FLOAT = Datatype.objects.get(pk=dt_pks.FLOAT_PK)
                BOOL = Datatype.objects.get(pk=dt_pks.BOOL_PK)

                if Python_type in [STR, BOOL]:
                    try:
                        for bc_type in ("minlen", "maxlen"):
                            if scform.cleaned_data[bc_type]:
                                bc = BasicConstraint(datatype=new_datatype, ruletype=bc_type,
                                                     rule=scform.cleaned_data[bc_type])
                                bc.full_clean()
                                bc.save()

                        if scform.cleaned_data["regexp"]:
                            # Check if there are multiple regexps, as in "<pattern 1>","<pattern 2>",...
                            # using regex from http://stackoverflow.com/questions/18144431/regex-to-split-a-csv
                            groups = re.findall('(?:^|,)(?=[^"]|(")?)"?((?(1)[^"]*|[^,"]*))"?(?=,|$)',
                                                scform.cleaned_data["regexp"])
                            for _quoted, group in groups:
                                regexp = BasicConstraint(datatype=new_datatype, ruletype="regexp", rule=group)
                                regexp.full_clean()
                                regexp.save()

                    except ValidationError as e:
                        scform.add_error("regexp", e)
                        # Raise e to break the transaction.
                        raise e

                elif Python_type in [INT, FLOAT]:
                    try:
                        for bc_type in ("minval", "maxval"):
                            if icform.cleaned_data[bc_type]:
                                bc = BasicConstraint(datatype=new_datatype, ruletype=bc_type, rule=query[bc_type])
                                bc.full_clean()
                                bc.save()
                    except ValidationError as e:
                        icform.add_error(bc_type, e)
                        raise e

                # Re-check Datatype object.
                try:
                    new_datatype.full_clean()
                    new_datatype.save()
                except ValidationError as e:
                    dform.add_error(None, e)
                    raise e

            # Success!
            return HttpResponseRedirect('/datatypes')

        except ValidationError as e:
            pass

    else:
        dform = DatatypeForm()  # unbound
        icform = IntegerConstraintForm()
        scform = StringConstraintForm()

    c.update({'datatype_form': dform, 'int_con_form': icform, 'str_con_form': scform})
    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def datatype_detail(request, id):
    # retrieve the Datatype object from database by PK
    four_oh_four = False
    try:
        this_datatype = Datatype.objects.get(pk=id)
        if not this_datatype.can_be_accessed(request.user):
            four_oh_four = True
    except Datatype.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        raise Http404("ID {} cannot be accessed".format(id))

    t = loader.get_template('metadata/datatype_detail.html')
    c = RequestContext(
        request,
        {
            "datatype": this_datatype,
            "constraints": this_datatype.basic_constraints.all()
        })
    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def compound_datatypes(request):
    """
    Render list of all CompoundDatatypes
    """
    compound_datatypes = CompoundDatatype.filter_by_user(request.user)
    compound_datatypes = sorted(compound_datatypes, key=str)
    t = loader.get_template('metadata/compound_datatypes.html')
    c = RequestContext(request, {'compound_datatypes': compound_datatypes})
    return HttpResponse(t.render(c))


# FIXME make this use a formset!
def make_cdm_forms(request, cdt):
    """
    Helper function for initializing forms with posted values and exceptions.

    If cdt is None, then the form returned is incomplete and not to be treated as a valid ModelForm.
    """
    query = request.POST.dict()
    num_forms = sum([1 for k in query.iterkeys() if k.startswith('datatype')])
    cdm_forms = []
    for i in range(num_forms):
        data = {'datatype': query['datatype_'+str(i)],
                'column_name': query['column_name_'+str(i)],
                'blankable': query.get('blankable_'+str(i))}
        auto_id = 'id_%s_' + str(i)

        if cdt is None:
            cdm_form = CompoundDatatypeMemberForm(user=request.user, auto_id=auto_id, initial=data)
        else:
            dummy_member = CompoundDatatypeMember(compounddatatype=cdt, column_idx=i+1)
            cdm_form = CompoundDatatypeMemberForm(data, user=request.user, auto_id=auto_id, instance=dummy_member)

        # Note: do not validate here!
        cdm_forms.append(cdm_form)
    return cdm_forms


class CDTDefException(Exception):
    def _init_(self, msg):
        self.msg = msg


@login_required
@user_passes_test(developer_check)
def compound_datatype_add(request):
    """
    Add CompoundDatatype from a dynamic set of CompoundDatatypeMember forms.
    """
    c = RequestContext(request)
    if request.method == 'POST':
        # Create a parent CDT object so we can define its members.
        dummy_cdt = CompoundDatatype(user=request.user)
        cdt_form = CompoundDatatypeForm(request.POST, instance=dummy_cdt)
        member_forms = make_cdm_forms(request, cdt=None)
        try:
            with transaction.atomic():
                try:
                    if not cdt_form.is_valid():
                        # Note that this has already done all the hard work for us of annotating
                        # cdt_form with errors.
                        raise CDTDefException()
                except ValidationError as e:
                    cdt_form.add_error(None, e)
                    raise CDTDefException()

                compound_datatype = cdt_form.save()
                compound_datatype.full_clean()

                # Having reached here, we can now make proper forms.
                member_forms = make_cdm_forms(request, compound_datatype)
                all_good = True
                for member_form in member_forms:
                    try:
                        if member_form.is_valid():
                            member_form.save()
                        else:
                            all_good = False
                    except ValidationError as e:
                        member_form.add_error(None, e)
                        raise e

                if not all_good:
                    raise CDTDefException()

                compound_datatype.full_clean()

            # Success!
            return HttpResponseRedirect('/compound_datatypes')
        except CDTDefException as e:
            pass
        except ValidationError as e:
            cdt_form.add_error(None, e)
    else:
        # Make initial blank forms.
        cdt_form = CompoundDatatypeForm()
        member_forms = [CompoundDatatypeMemberForm(user=request.user, auto_id='id_%s_0')]

    # Note that even if there were exceptions thrown the forms have been properly annotated with errors.
    t = loader.get_template('metadata/compound_datatype_add.html')
    c.push({"cdt_form": cdt_form,
            'cdm_forms': member_forms,
            'first_form': member_forms and member_forms[0]})

    return HttpResponse(t.render(c))

