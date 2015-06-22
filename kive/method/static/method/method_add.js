$(document).ready(function(){ // wait for page to finish loading before executing jQuery code

    noXSS();

    // trigger ajax on CR drop-down to populate revision select
    $("#id_coderesource").on('change', function() {
        cr_id = this.value;
        if (cr_id != "") {
            $.getJSON(
                "/get_revisions/",
                { cr_id: cr_id }, // specify data as an object
                function(result) {
                    var options = [];
                    $.each(result, function(index,value) {
                        options.push($('<option>').attr('value', value.pk).text(
                                value.fields.revision_number + ': ' +
                                value.fields.revision_name));
                    });
                    $("#id_revisions").empty().append(options);
                }
            );
        }
        else {
            $("#id_revisions").html('<option value="">--- select a CodeResource first ---</option>');
        }
    }).change(); // trigger on load

    // add or subtract input forms
    var numberOfInputForms = $('#extraInputForms > tr').length;
    var options = document.getElementById("id_compounddatatype_in_0").options;

    // modify name attributes for extra input forms received from server
    for (var i = 0; i < numberOfInputForms; i++) {
        $('#id_dataset_name_in_'+i).attr('name', 'dataset_name_in_'+i);
        $('#id_compounddatatype_in_'+i).attr('name', 'compounddatatype_in_'+i);
        $('#id_min_row_in_'+i).attr('name', 'min_row_in_'+i);
        $('#id_max_row_in_'+i).attr('name', 'max_row_in_'+i);
    }

    // append row to table
    $("#addInputForm").click(
        function () {
            numberOfInputForms += 1;
            i = numberOfInputForms - 1; // 0-indexing
            htmlStr = "<tr>"
            htmlStr += "<td>"+numberOfInputForms+"</td>";

            htmlStr += "<td><input id=\"id_dataset_name_in_" + i + "\" maxlength=\"128\" name=\"dataset_name_in_" + i + "\" type=\"text\" /></td>";
            htmlStr += "<td><select id=\"id_compounddatatype_in_" + i + "\" name=\"compounddatatype_in_" + i + "\">";
            for (var j = 0; j < options.length; j++) {
                htmlStr += "<option value=\"" + options[j].value + "\">" + options[j].text + "</option>";
            }
            htmlStr += "</select></td>";
            htmlStr += "<td><input id=\"id_min_row_in_" + i + "\" class=\"shortIntField\" name=\"min_row_in_" + i + "\" type=\"number\" /></td>";
            htmlStr += "<td><input id=\"id_max_row_in_" + i + "\" class=\"shortIntField\" name=\"max_row_in_" + i + "\" type=\"number\" /></td>";
            htmlStr += "</tr>"

            $('#extraInputForms').find('tr:last').after(htmlStr);
        }
    );
    $("#removeInputForm").click(
        function() {
            if (numberOfInputForms > 1) {
                numberOfInputForms -= 1;
                $('#extraInputForms').find('tr:last').remove();
            }
        }
    );


    // add or subtract output forms
    var numberOfOutputForms = $('#extraOutputForms > tr').length;
    // we can reuse options

    // modify name attributes for extra input forms received from server
    for (var i = 0; i < numberOfOutputForms; i++) {
        $('#id_dataset_name_out_'+i).attr('name', 'dataset_name_out_'+i);
        $('#id_compounddatatype_out_'+i).attr('name', 'compounddatatype_out_'+i);
        $('#id_min_row_out_'+i).attr('name', 'min_row_out_'+i);
        $('#id_max_row_out_'+i).attr('name', 'max_row_out_'+i);
    }

    // append row to table
    $("#addOutputForm").click(
        function () {
            numberOfOutputForms += 1;
            i = numberOfOutputForms - 1; // 0-indexing
            htmlStr = "<tr>"
            htmlStr += "<td>"+numberOfOutputForms+"</td>";

            htmlStr += "<td><input id=\"id_dataset_name_out_" + i + "\" maxlength=\"128\" name=\"dataset_name_out_" + i + "\" type=\"text\" /></td>";
            htmlStr += "<td><select id=\"id_compounddatatype_out_" + i + "\" name=\"compounddatatype_out_" + i + "\">";
            for (var j = 0; j < options.length; j++) {
                htmlStr += "<option value=\"" + options[j].value + "\">" + options[j].text + "</option>";
            }
            htmlStr += "</select></td>";
            htmlStr += "<td><input id=\"id_min_row_out_" + i + "\" class=\"shortIntField\" name=\"min_row_out_" + i + "\" type=\"number\" /></td>";
            htmlStr += "<td><input id=\"id_max_row_out_" + i + "\" class=\"shortIntField\" name=\"max_row_out_" + i + "\" type=\"number\" /></td>";
            htmlStr += "</tr>"

            $('#extraOutputForms').find('tr:last').after(htmlStr);
        }
    );
    $("#removeOutputForm").click(
        function() {
            if (numberOfOutputForms > 1) {
                numberOfOutputForms -= 1;
                $('#extraOutputForms').find('tr:last').remove();
            }
        }
    );


    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        var $this = $(this);
        $this.wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
    });
    
    $('a[rel="ctrl"]').on('click', function (e) {
        $(this).siblings('.fulltext').show().css({ top: e.pageY, left: e.pageX, 'z-index': 3 });
        setTimeout("$('.fulltext').fadeOut(300);", 2000);
    });

    // hide Method Family form if a pre-existing family is selected
    $('#id_family').on('change', function() {
        this_family = $(this).val();
        if (this_family == "") {
            $('#id_name').prop('disabled', false);
            $('#id_description').prop('disabled', false);
        } else {
            // TODO: ajax query populates these fields with values from DB
            $('#id_name').prop('disabled', true);
            $('#id_description').prop('disabled', true);
        }
    }).change(); // trigger on load

    // set default MethodFamily name to name of CodeResource
    $("#id_coderesource").on("change", function () {
        $("#id_revision_name").val($(this).children("option:selected").text());
    });
});
