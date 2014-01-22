$(document).ready(function(){ // wait for page to finish loading before executing jQuery code

    $(document).ajaxSend(function(event, xhr, settings) {
        /*
            from https://docs.djangoproject.com/en/1.3/ref/contrib/csrf/#csrf-ajax
            On each XMLHttpRequest, set a custom X-CSRFToken header to the value of the CSRF token.
            ajaxSend is a function to be executed before an Ajax request is sent.
        */
        //console.log('ajaxSend triggered');

        function getCookie(name) {
            var cookieValue = null;
            if (document.cookie && document.cookie != '') {
                var cookies = document.cookie.split(';');
                for (var i = 0; i < cookies.length; i++) {
                    var cookie = jQuery.trim(cookies[i]);
                    // Does this cookie string begin with the name we want?
                    if (cookie.substring(0, name.length + 1) == (name + '=')) {
                        cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                        break;
                    }
                }
            }
            return cookieValue;
        }
        function sameOrigin(url) {
            // url could be relative or scheme relative or absolute
            var host = document.location.host; // host + port
            var protocol = document.location.protocol;
            var sr_origin = '//' + host;
            var origin = protocol + sr_origin;
            // Allow absolute or scheme relative URLs to same origin
            return (url == origin || url.slice(0, origin.length + 1) == origin + '/') ||
                (url == sr_origin || url.slice(0, sr_origin.length + 1) == sr_origin + '/') ||
                // or any other URL that isn't scheme relative or absolute i.e relative.
                !(/^(\/\/|http:|https:).*/.test(url));
        }
        function safeMethod(method) {
            return (/^(GET|HEAD|OPTIONS|TRACE)$/.test(method));
        }

        if (!safeMethod(settings.type) && sameOrigin(settings.url)) {
            xhr.setRequestHeader("X-CSRFToken", getCookie('csrftoken'));
        }
    });

    var options = document.getElementById("id_coderesource").options;
    var numberOfForms = 0;
    $("#addDependencyForm").click(  // query button by id selector
        function () {   // anonymous function
            numberOfForms += 1;
            renderForms(numberOfForms);
        }
    )
    $("#removeDependencyForm").click(
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
            htmlStr += "<td><select class=\"coderesource\" id=\"id_coderesource_" + i + "\" name=\"coderesource_" + i + "\">";
            for (var j = 0; j < options.length; j++) {
                htmlStr += "<option value=\"" + options[j].value + "\">" + options[j].text + "</option>";
            }
            htmlStr += "</select></td>";
            htmlStr += "<td><select class=\"revisions\" id=\"id_revisions_" + i + "\" name=\"revisions_" + i + "\">";
            htmlStr += "<option value=\"\" selected=\"selected\">--- select a CodeResource first ---</option></select></td>";

            // generate char fields
            htmlStr += "<td><input id=\"id_depPath_" + i + "\" maxlength=\"255\" name=\"depPath_" + i + "\" type=\"text\" /></td>";
            htmlStr += "<td><input id=\"id_depFileName_" + i + "\" maxlength=\"255\" name=\"depFileName_" + i + "\" type=\"text\" /></td>";
            htmlStr += "</tr>"
        }
        $("#extraDependencyForms").html(htmlStr);

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
                    $("#id_revisions_"+suffix).html('<option value=\"\">--- select a CodeResource first ---</option>');
                }
            }
        )
    };

    // trigger ajax on CR drop-down to populate revision select
    $("#id_coderesource").on('change',
        function() {
            cr_id = $(this).val();
            if (cr_id != "") {
                $.ajax({
                    type: "POST",
                    url: "get_revisions/",
                    data: {cr_id: cr_id}, // specify data as an object
                    datatype: "json", // type of data expected back from server
                    success: function(result) {
                        var options = [];
                        var arr = JSON.parse(result)
                        $.each(arr, function(index,value) {
                            options.push('<option value="', value.pk, '">', value.fields.revision_name, '</option>');
                        });
                        $("#id_revisions").html(options.join(''));
                    }
                })
            }
            else {
                $("#id_revisions").html('<option value=\"\">--- select a CodeResource first ---</option>');
            }
        }
    )

});
