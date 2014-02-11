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
                $("#id_revisions").html('<option value="">--- select a CodeResource first ---</option>');
            }
        }
    );

    // add or subtract input forms
    var numberOfInputForms = $('#extraInputForms > tr').length;
    // modify name attributes for extra input forms received from server
    for (var i=0; i < numberOfInputForms; i++) {
        $('#id_dataset_name_in_'+i).attr('name', 'dataset_name_in_'+i);
        $('#id_dataset_idx_in_'+i).attr('name', 'dataset_idx_in_'+i);
        $('#id_compounddatatype_in_'+i).attr('name', 'compounddatatype_in_'+i);
        $('#id_min_row_in_'+i).attr('name', 'min_row_in_'+i);
        $('#id_max_row_in_'+i).attr('name', 'max_row_in_'+i);
    }

    $("#addInputForm").click(  // query button by id selector
        function () {   // anonymous function
            numberOfInputForms += 1;
            renderInputForms(numberOfInputForms);
        }
    );
    $("#removeInputForm").click(
        function() {
            if (numberOfInputForms > 0) {
                numberOfInputForms -= 1;
                renderInputForms(numberOfInputForms);
            }
        }
    );
    var options = document.getElementById("id_compounddatatype_in_0").options;
    console.log(options.length);
    var renderInputForms = function($nForms) {
        var htmlStr = "";
        for (var i = 0; i < $nForms; i++) {
            // generate char fields
            htmlStr += "<tr>"
            htmlStr += "<td><input id=\"id_dataset_name_in_" + i + "\" maxlength=\"128\" name=\"dataset_name_in_" + i + "\" type=\"text\" /></td>";
            htmlStr += "<td><input id=\"id_dataset_idx_in_" + i + "\" class=\"shortIntField\" name=\"dataset_idx_in_" + i + "\" type=\"number\" /></td>";
            htmlStr += "<td><select id=\"id_compounddatatype_in_" + i + "\" name=\"compounddatatype_in_" + i + "\">";
            for (var j = 0; j < options.length; j++) {
                htmlStr += "<option value=\"" + options[j].value + "\">" + options[j].text + "</option>";
            }
            htmlStr += "</select></td>";
            htmlStr += "<td><input id=\"id_min_row_in_" + i + "\" class=\"shortIntField\" name=\"min_row_in_" + i + "\" type=\"number\" /></td>";
            htmlStr += "<td><input id=\"id_max_row_in_" + i + "\" class=\"shortIntField\" name=\"max_row_in_" + i + "\" type=\"number\" /></td>";
            htmlStr += "</tr>"
        }
        $("#extraInputForms").html(htmlStr);
    };

    /*
    // add or subtract output forms
    var numberOfOutputForms = $('#extraInputForms > tr').length;
    if (numberOfOutputForms > 0) {
        for (var i=0; i < numberOfOutputForms; i++) {
            $('#id_dataset_name_'+i).attr('name', 'dataset_name_'+i);
            $('#id_dataset_idx_'+i).attr('name', 'dataset_idx_'+i);
        }
    }
    $("#addOutputForm").click(  // query button by id selector
        function () {   // anonymous function
            numberOfOutputForms += 1;
            renderInputForms(numberOfOutputForms);
        }
    );
    $("#removeInputForm").click(
        function() {
            if (numberOfOutputForms > 0) {
                numberOfOutputForms -= 1;
                renderInputForms(numberOfOutputForms);
            }
        }
    );
    // render forms in div
    var renderOutputForms = function($nForms) {
        var htmlStr = "";
        for (var i = 0; i < $nForms; i++) {
            // generate char fields
            htmlStr += "<tr>"
            htmlStr += "<td><input id=\"id_dataset_name_" + i + "\" maxlength=\"128\" name=\"dataset_name_" + i + "\" type=\"text\" /></td>";
            htmlStr += "<td><input id=\"id_dataset_idx_" + i + "\" name=\"dataset_idx_" + i + "\" type=\"text\" /></td>";
            htmlStr += "</tr>"
        }
        $("#extraOutputForms").html(htmlStr);
    };
    */

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
