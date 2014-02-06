
$(document).ready(function(){ // wait for page to finish loading before executing jQuery code
    $("#id_Python_type").on('change', function() {
        if (this.value == 'str') {
            $("#int_constraints").hide();
            $("#str_constraints").show();
        }
        else if (this.value == 'int' || this.value == 'float') {
            $("#int_constraints").show();
            $("#str_constraints").hide();
        }
        else {
            $("#int_constraints").show();
            $("#str_constraints").show();
        }
    }
    ).change(); // trigger on load

    var options = document.getElementById("id_datatype").options;
    var numberOfForms = $('#extraForms > tr').length;

    // modify name attribute of extraForms if there are any
    if (numberOfForms > 0) {
        for (var i=0; i < numberOfForms; i++) {
            $('#id_datatype_'+i).attr('name', 'datatype_'+i);
            $('#id_column_name_'+i).attr('name', 'column_name_'+i);
            $('#id_column_idx_'+i).attr('name', 'column_idx_'+i);
        }
    }

    $("#addForm").click(  // query button by id selector
        function () {   // anonymous function
            numberOfForms += 1;
            renderForms(numberOfForms);
        }
    )
    $("#removeForm").click(
        function() {
            if (numberOfForms > 0) {
                numberOfForms -= 1;
                renderForms(numberOfForms);
            }
        }
    )

    // render forms in div
    // TODO: adding or removing forms blanks out all extra forms, another way to do this?
    var renderForms = function($nForms) {
        var htmlStr = "";
        for (var i = 0; i < $nForms; i++) {
            // generate drop-down menus
            htmlStr += "<tr>";
            htmlStr += "<td><select id=\"id_datatype_" + i + "\" name=\"datatype_" + i + "\">";
            for (var j = 0; j < options.length; j++) {
                htmlStr += "<option value=\"" + options[j].value + "\">" + options[j].text + "</option>";
            }
            htmlStr += "</select></td>";

            htmlStr += "<td><input id=\"id_column_name_"+i+"\" type=\"text\" name=\"column_name_"+i+"\" maxlength=\"128\" /></td>";
            htmlStr += "<td><input type=\"text\" name=\"column_idx_"+i+"\" id=\"id_column_idx_"+i+"\" /></td>"
            htmlStr += "</tr>"
        }
        $("#extraForms").html(htmlStr);
    };


    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        var $this = $(this);
        $this.wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
    });
    
    $('a[rel="ctrl"]').on('click', function (e) {
        $(this).siblings('.fulltext').show().css({ top: e.pageY, left: e.pageX });
        setTimeout("$('.fulltext').fadeOut(300);", 2000);
    });
});

