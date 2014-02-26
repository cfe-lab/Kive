"""
metadata.views
"""

from django.http import HttpResponse, HttpResponseRedirect
from metadata.models import Datatype, CompoundDatatype
from metadata.forms import *
from django.template import loader, Context
from django.core.context_processors import csrf
from django.core.exceptions import ValidationError
from django.forms.util import ErrorList
from django.db import transaction, IntegrityError
import re

def datatypes(request):
    """
    Render table and form on user request for datatypes.html
    """
    datatypes = Datatype.objects.all()
    t = loader.get_template('metadata/datatypes.html')
    c = Context({'datatypes': datatypes})
    c.update(csrf(request))
    return HttpResponse(t.render(c))
    #return render_to_response('datatypes.html', {'form': form})


def datatype_add(request):
    """
    Render form for creating a new Datatype
    """
    exceptions = []

    if request.method == 'POST':
        dform = DatatypeForm(request.POST) # create form bound to POST data
        query = request.POST.dict()

        if dform.is_valid():
            new_datatype = dform.save() # this has to be saved to database to be passed to BasicConstraint()
            minlen, maxlen, regexp, minval, maxval = None, None, None, None, None

            if new_datatype.Python_type == 'str':
                # manually create and validate BasicConstraint objects
                if query['minlen']:
                    minlen = BasicConstraint(datatype=new_datatype, ruletype='minlen', rule=query['minlen'])
                    try:
                        minlen.full_clean()
                    except ValidationError as e:
                        exceptions.extend(e.messages)
                        pass

                if query['maxlen']:
                    maxlen = BasicConstraint(datatype=new_datatype, ruletype='maxlen', rule=query['maxlen'])
                    try:
                        maxlen.full_clean()
                    except ValidationError as e:
                        exceptions.extend(e.messages)
                        pass

                if query['regexp']:
                    # check if there are multiple regexps, as in "<pattern 1>","<pattern 2>",...
                    # using regex from http://stackoverflow.com/questions/18144431/regex-to-split-a-csv
                    groups = re.findall('(?:^|,)(?=[^"]|(")?)"?((?(1)[^"]*|[^,"]*))"?(?=,|$)', query['regexp'])
                    try:
                        for quoted, group in groups:
                            regexp = BasicConstraint(datatype=new_datatype, ruletype='regexp', rule=group)
                            regexp.full_clean()
                    except ValidationError as e:
                        exceptions.extend(e.messages)
                        pass

            elif new_datatype.Python_type in ['int', 'float']:
                if query['minval']:
                    minval = BasicConstraint(datatype=new_datatype, ruletype='minval', rule=query['minval'])
                    try:
                        minval.full_clean()
                    except ValidationError as e:
                        exceptions.extend(e.messages)
                        pass

                if query['maxval']:
                    maxval = BasicConstraint(datatype=new_datatype, ruletype='maxval', rule=query['maxval'])
                    try:
                        maxval.full_clean()
                    except ValidationError as e:
                        exceptions.extend(e.messages)
                        pass

            if exceptions:
                new_datatype.delete() # delete object from database
            else:
                # save basic constraint objects if they are defined
                if minlen: minlen.save()
                if maxlen: maxlen.save()
                if regexp: regexp.save()
                if minval: minval.save()
                if maxval: maxval.save()

                # re-check Datatype object
                new_datatype.full_clean()
                new_datatype.save()
                return HttpResponseRedirect('/datatypes')


        # populate forms for display (one or more invalid forms)
        icform = IntegerConstraintForm({'minval': query.get('minval', None),
            'maxval': query.get('maxval', None)})
        if exceptions:
            icform.errors['Basic Constraint Errors'] = ErrorList(exceptions)

        scform = StringConstraintForm({'minlen': query.get('minlen', None),
            'maxlen': query.get('maxlen', None),
            'regexp': query.get('regexp', None)})

    else:
        dform = DatatypeForm() # unbound
        #cform = BasicConstraintForm()
        icform = IntegerConstraintForm()
        scform = StringConstraintForm()

    t = loader.get_template('metadata/datatype_add.html')
    c = Context({'datatype_form': dform,
                'int_con_form': icform,
                'str_con_form': scform})
    c.update(csrf(request))

    return HttpResponse(t.render(c))




def datatype_detail(request, id):
    # retrieve the Datatype object from database by PK
    this_datatype = Datatype.objects.get(pk=id)
    t = loader.get_template('metadata/datatype_detail.html')
    c = Context({'datatype': this_datatype,
                'constraints': this_datatype.basic_constraints.all()})
    c.update(csrf(request))
    return HttpResponse(t.render(c))



def compound_datatypes(request):
    """
    Render list of all CompoundDatatypes
    """
    compound_datatypes = CompoundDatatype.objects.all()
    t = loader.get_template('metadata/compound_datatypes.html')
    c = Context({'compound_datatypes': compound_datatypes})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def return_cdm_forms(request, exceptions):
    """
    Helper function for initializing forms with posted values and exceptions.
    """
    query = request.POST.dict()
    num_forms = sum([1 for k in query.iterkeys() if k.startswith('datatype')])
    cdm_forms = []
    for i in range(num_forms):
        cdm_form = CompoundDatatypeMemberForm(auto_id = 'id_%s_' + str(i),
                                              initial = {'datatype': query['datatype_'+str(i)],
                                                         'column_name': query['column_name_'+str(i)]})
        cdm_form.errors['Errors'] = ErrorList(exceptions.get(i, ''))
        cdm_forms.append(cdm_form)
    return cdm_forms


def compound_datatype_add (request):
    """
    Add compound datatype from a dynamic set of CompoundDatatypeMember forms.
    """
    if request.method == 'POST':
        query = request.POST.dict()

        # an empty parent CDT object to build relations
        compound_datatype = CompoundDatatype()
        compound_datatype.save()

        num_forms = sum([1 for k in query.iterkeys() if k.startswith('datatype')])
        exceptions = {}
        to_save = []

        for i in range(num_forms):
            try:
                its_datatype = Datatype.objects.get(pk=query['datatype_'+str(i)])
            except:
                exceptions.update({i: [u'Datatype must be selected']})
                continue

            member = CompoundDatatypeMember (compounddatatype = compound_datatype,
                                             datatype = its_datatype,
                                             column_name = query['column_name_'+str(i)],
                                             column_idx = i+1) # indices are 1-index
            try:
                member.full_clean()
            except ValidationError as e:
                exceptions.update({i: e.messages})
                continue
            to_save.append(member)

        if exceptions:
            # one or more problems with CompoundDatatypeMember forms
            compound_datatype.delete()
            cdm_forms = return_cdm_forms(request, query, num_forms, exceptions)
        else:
            for member in to_save:
                member.save()
            try:
                compound_datatype.full_clean() # check again
                compound_datatype.save()

                return HttpResponseRedirect('/compound_datatypes') # success!
            except:
                # problem with CompoundDatatype
                for member in to_save: # explicit rollback
                    member.delete()
                compound_datatype.delete()
                cdm_forms = return_cdm_forms(request, query, num_forms, exceptions)
                pass
    else:
        # initial blank form
        cdm_forms = [CompoundDatatypeMemberForm(auto_id='id_%s_0')]

    t = loader.get_template('metadata/compound_datatype_add.html')
    c = Context({'cdm_forms': cdm_forms})
    c.update(csrf(request))

    return HttpResponse(t.render(c))

