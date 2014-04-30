
/*
    TODO: if shape (CDtNode, MethodNode) or Connector selected and user hits backspace, erase object
    TODO: make Connectors selectable by mouse
    TODO: Connector can link to output end-zone
    TODO: submit shape and connector info as POST data
    FIXME: figure out why extra lines are being drawn (non-closed path)?
*/

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
            return url == origin || url.slice(0, origin.length + 1) == origin + '/' ||
                url == sr_origin || url.slice(0, sr_origin.length + 1) == sr_origin + '/' ||
                // or any other URL that isn't scheme relative or absolute i.e relative.
                !(/^(\/\/|http:|https:).*/.test(url));
        }
        function safeMethod(method) {
            return [ 'GET', 'HEAD', 'OPTIONS', 'TRACE' ].indexOf(method) > -1;
        }

        if (!safeMethod(settings.type) && sameOrigin(settings.url)) {
            xhr.setRequestHeader("X-CSRFToken", getCookie('csrftoken'));
        }
    });

    // update method drop-down
//    $("[id^='id_select_method_family']").on('change', // Are these still being generated dynamically in quantity? —JN
    $("#id_select_method_family").on('change',
        function() {
            mf_id = this.value;// DOMElement.value is much faster than jQueryObject.val().
            if (mf_id != "") {
                $.ajax({
                    type: "POST",
                    url: "get_method_revisions/",
                    data: {mf_id: mf_id}, // specify data as an object
                    datatype: "json", // type of data expected back from server
                    success: function(result) {
                        var options = [];
                        var arr = JSON.parse(result)
                        $.each(arr, function(index,value) {
                            options.push('<option value="', value.pk, '">', value.fields.revision_name, '</option>');
                        });
                        $("#id_select_method").show().html(options.join(''));
                    }
                })
            }
            else {
                $("#id_select_method").hide();
            }
        }
    ).change() // trigger on load

    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        var $this = $(this);
        $this.wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
    });

    $('a[rel="ctrl"]').on('click', function (e) {
        $(this).siblings('.fulltext').show().css({ top: e.pageY, left: e.pageX });
        setTimeout("$('.fulltext').fadeOut(300);", 2000);
    });


    // initialize animated canvas
    var canvas = document.getElementById('pipeline_canvas');
    var canvasWidth = 800;
    var canvasHeight= 400;
    canvas.width = canvasWidth;
    canvas.height = canvasHeight;

    var canvasState = new CanvasState(canvas);

    $('#id_cdt_button').on('click', function() {
        var choice = $('#id_select_cdt option:selected');
        var node_label = $('#id_datatype_name').val();

        if (node_label == '') {
            // required field
            $('#id_cdt_error')[0].innerHTML = "Label is required";
        }
        else {
            $('#id_cdt_error')[0].innerHTML = "";
            var pk = choice.val(); // primary key
            if (pk == ""){
                canvasState.addShape(new RawNode(x = 100, y = 200 + 50 * Math.random(),
                    r = 20, fill='#88DD88', inset=10, offset=25, label=node_label
                ))
            } else {
                var node_label = choice.text();
                canvasState.addShape(new CDtNode(pk = pk, x = 100, y = 200 + 50 * Math.random(),
                    w = 40, fill = '#8888DD', inset = 10, offset = 10, label = node_label
                ));
            }
            $('#id_datatype_name').val("");  // reset text field
        }
    });

    $('#id_method_button').on('click', function() {
        var selected = $('#id_select_method option:selected');
        var mid = selected.val(); // pk of method

        if (mid != "") {
            var n_inputs = null;
            var n_outputs = null;
            var node_label = selected.text();
            $.ajax({
                type: "POST",
                url: "get_method_io/",
                data: {mid: mid}, // specify data as an object
                datatype: "json", // type of data expected back from server
                success: function(result) {
                    inputs = result['inputs'];
                    outputs = result['outputs'];
                    canvasState.addShape(new MethodNode(mid, 200, 200 + 50 * Math.random(), 80, 10, 20, '#999999', node_label, 14, inputs, outputs));
                    
                    // x, y, w, inset, spacing, fill, label, offset, inputs, outputs
                    //canvasState.addShape(new MethodNode(200, 200 + 50 * Math.random(), 45, 5, 20, '#CCCCCC', node_label, 0, inputs, outputs));
                }
            });
        }
    });

    $('#id_reset_button').on('click', function() {
        // remove all objects from canvas
        canvasState.clear();
        canvasState.shapes = [];
        canvasState.connectors = [];
    });

    $('#id_delete_button').on('click', function() {
        // remove selected object from canvas
        canvasState.deleteObject();
    });

    $(document).on('keydown', function(e) {
        // backspace or delete key also removes selected object
        if ([8,46].indexOf(e.which) > -1 && !$(e.target).is("input, textarea")) {
            // prevent backspace from triggering browser to navigate back one page
            e.preventDefault();
            canvasState.deleteObject();
        }
    })
    
    $('#id_revision_desc').on('keydown', function() {
        var $this = $(this),
            happy = -Math.min(15, Math.floor($this.val().length / 19)) * 32;
    
        $('.happy_indicator').css('background-position', happy + 'px 0px');
    })
        .trigger('keydown')
        .wrap('<div id="description_wrap">')
        .after('<div class="happy_indicator">')
        .after('<div class="happy_indicator_label">Write a great description to keep everyone happy!</div>')
        .on('focus', function() {
            $('.happy_indicator, .happy_indicator_label').show();
        }).on('blur', function() {
            $('.happy_indicator, .happy_indicator_label').hide();
        }).blur();


    $('form').submit(function(e) {
        e.preventDefault(); // override form submit action

        var form_data = {};

        // Pipeline (Transformation) variables
        form_data['revision_name'] = $('#id_revision_name').val();
        form_data['revision_desc'] = $('#id_revision_desc').val();
        // inputs (at start of pipeline)
        // outputs (at end of pipeline)

        // Pipeline derived class-specific variables
        // family - undefined, for pipeline_add this is first member of a new family
        // parent - undefined, similarly

        // PipelineStep [requires Pipeline]
        /*
            content_type (method or pipeline)
            object_id (pk to method/pipeline)
            transformation (foreign key to the method/pipeline)
            step_num (must be integer x > 0)
        */

        // TODO: sort methods into pipeline steps - start from terminal Connectors
        var sorted_elements = [];
        var seeds = []; // nodes without incoming edges
        var shape;
        for (var si = 0; si < shapes.length; si++) {
            shape = shapes[si];
            if (shape.constructor == CDtNode) {
                seeds.push(shape);
            }
        }



        var connectors = this.connectors;
        var connector;

        // seed array with MethodNodes that feed into final output
        for (var ci = 0; ci < connectors.length; ci++) {
            connector = connectors[ci];
            if (connector.in_magnet == '__output__') {
                // Connector terminates in final output
                // note only MethodNodes are permited to connect to final output
                method_ranks.push(connector.out_magnet.parent);
            }
        }

        while (method_ranks.length < shapes.length) {

        }

        var shapes = this.shapes;
        var shape;
        for (var i= 0; i < shapes.length; i++) {
            shape = shapes[i];
            if (shape.constructor == MethodNode) {

            }
        }

        // PipelineInputCable [requires PipelineStep]
        /*
            source - output hole (magnet) of Transformation

            dest - input hole (magnet) of Transformation
         */

        $.ajax({
            type: 'POST',
            url: 'pipeline_add',
            data: form_data,
            datatype: 'json',
            success: function(result) {
                console.log(result);
            }
        })
    })
});

