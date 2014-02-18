
$(document).ready(function(){ // wait for page to finish loading before executing jQuery code
    var options = document.getElementById("id_datatype_0").options;
    var nForms = $('#cdmForms > tr').length;

    // modify name attribute of forms
    for (var i = 0; i < 1 + nForms; i++) {
        $('#id_datatype_'+i).attr('name', 'datatype_'+i);
        $('#id_column_name_'+i).attr('name', 'column_name_'+i);
        $('#id_column_idx_'+i).attr('name', 'column_idx_'+i);
    }

    $("#addForm").click(  // query button by id selector
        function () {
            nForms += 1;
            i = nForms - 1; // 0-based index
            var htmlStr = "";
            htmlStr += "<tr>";
            htmlStr += "<td>"+nForms+"</td>"; // column index
            htmlStr += "<td><select id=\"id_datatype_" + i + "\" name=\"datatype_" + i + "\">";
            for (var j = 0; j < options.length; j++) {
                htmlStr += "<option value=\"" + options[j].value + "\">" + options[j].text + "</option>";
            }
            htmlStr += "</select></td>";
            htmlStr += "<td><input id=\"id_column_name_"+i+"\" type=\"text\" name=\"column_name_"+i+"\" maxlength=\"128\" /></td>";
            htmlStr += "</tr>";

            $('#cdmForms').find('tr:last').after(htmlStr);
        }
    )
    $("#removeForm").click(
        function() {
            if (nForms > 0) {
                nForms -= 1;
                $('#cdmForms').find('tr:last').remove();
            }
        }
    )

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

