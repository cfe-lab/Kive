$(document).ready(function(){ // wait for page to finish loading before executing jQuery code


    // trigger ajax on CR drop-down to populate revision select
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

    // populate CR revision dropdown on selection of CodeResource
    $("[id^='id_coderesource_']").on('change',
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
                        var options = [];
                        var arr = JSON.parse(result)
                        $.each(arr, function(index,value) {
                            options.push('<option value="', value.pk, '">', value.fields.revision_name, '</option>');
                        });
                        $("#id_revisions_"+suffix).html(options.join(''));
                    }
                })
            }
            else {
                $("#id_revisions_"+suffix).html('<option value="">--- select a CodeResource first ---</option>');
            }
        }
    ).change() // trigger on load

    var options = document.getElementById("id_coderesource_0").options;
    var numberOfForms = $('#dependencyForms > tr').length;

    // modify name attributes for extra input forms received from server
    for (var i = 0; i < numberOfForms; i++) {
        $('#id_coderesource_'+i).attr('name', 'coderesource_'+i);
        $('#id_revisions_'+i).attr('name', 'revisions_'+i);
        $('#id_depPath_'+i).attr('name', 'depPath_'+i);
        $('#id_depFileName_'+i).attr('name', 'depFileName_'+i);
    }

    $("#addDependencyForm").click(  // query button by id selector
        function () {   // anonymous function
            numberOfForms += 1;
            i = numberOfForms - 1; // zero-based index
            var htmlStr = "<tr>";
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
            htmlStr += "</tr>";

            $('#dependencyForms').find('tr:last').after(htmlStr);

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
        }
    );

    $("#removeDependencyForm").click(
        function() {
            if (numberOfForms > 1) {
                numberOfForms -= 1;
                $('#dependencyForms').find('tr:last').remove();
            }
        }
    );

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
