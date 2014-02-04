
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
    var numberOfForms = 0;
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

        // repeated within this class-based event handler for the dynamic HTML elements
        $("select.coderesource").on('change',
            function() {
                var suffix = $(this).attr('id').split('_')[2];
                cr_id = $(this).val();
                if (cr_id != "") {
                    $.ajax({
                        type: "POST",
                        url: "get_revisions/",
                        data: {cr_id: cr_id}, // specify data as an object
                        datatype: "json", // type of data expected back from server
                        success: function(result) {
                            //console.log(result);
                            var options = [];
                            var arr = JSON.parse(result)
                            $.each(arr, function(index,value) {
                                options.push('<option value="', value.pk, '">', value.fields.revision_name, '</option>');
                            });
                            $("#id_revisions_"+suffix).html(options.join(''));
                        },
                    })
                }
                else {
                    // reset the second drop-down
                    $("#id_revisions_"+suffix).html('<option value="">--- select a CodeResource first ---</option>');
                }
            }
        )
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

