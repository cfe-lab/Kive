
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
                // label this new drop-drown
                $('#id_revision_label')[0].innerHTML = '<i>Revision: </i>';
            }
            else {
                $("#id_select_method").hide();
                $('#id_revision_label')[0].innerHTML = '';
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

        if (node_label === '') {
            // required field
            $('#id_dt_error')[0].innerHTML = "Label is required";
        }
        else {
            $('#id_dt_error')[0].innerHTML = "";
            var this_pk = choice.val(); // primary key
            if (this_pk == ""){
                canvasState.addShape(new RawNode(x = 100, y = 200 + 50 * Math.random(),
                    r = 20, fill='#88DD88', inset=10, offset=25, label=node_label
                ))
            } else {
                canvasState.addShape(new CDtNode(pk = this_pk, x = 100, y = 200 + 50 * Math.random(),
                    w = 40, fill = '#8888DD', inset = 10, offset = 10, label = node_label
                ));
            }
            $('#id_datatype_name').val("");  // reset text field
        }
    });

    $('#id_method_button').on('click', function() {
        var selected = $('#id_select_method option:selected');
        var mid = selected.val(); // pk of method

        if (mid === undefined) {
            $('#id_method_error')[0].innerHTML = "Select a Method";
        }
        else {
            // user selected valid Method Revision
            var node_label = $('#id_method_name').val();

            if (node_label === '') {
                // required field
                $('#id_method_error')[0].innerHTML = "Label is required";
            }
            else {
                $('#id_method_error')[0].innerHTML = "";
                var n_inputs = null;
                var n_outputs = null;

                // use AJAX to retrieve Revision inputs and outputs
                $.ajax({
                    type: "POST",
                    url: "get_method_io/",
                    data: {mid: mid}, // specify data as an object
                    datatype: "json", // type of data expected back from server
                    success: function(result) {
                        inputs = result['inputs'];
                        outputs = result['outputs'];
                        canvasState.addShape(new MethodNode(mid, 200, 200 + 50 * Math.random(), 80, 10, 20, '#999999', node_label, 10, inputs, outputs));

                        // x, y, w, inset, spacing, fill, label, offset, inputs, outputs
                        //canvasState.addShape(new MethodNode(200, 200 + 50 * Math.random(), 45, 5, 20, '#CCCCCC', node_label, 0, inputs, outputs));
                    }
                });

                $('#id_method_name').val('');
            }
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
        var submit_error = $('#id_submit_error')[0];

        var shapes = canvasState.shapes;

        // TODO: check graph integrity

        var this_shape;
        var magnets;
        var this_magnet;
        var i, j;
        var pipeline_inputs = [];  // collect data nodes
        var method_nodes = [];

        for (i = 0; i < shapes.length; i++) {
            this_shape = shapes[i];
            if (this_shape.constructor !== MethodNode) {
                pipeline_inputs.push(this_shape);

                // all CDtNodes or RawNodes (inputs) should feed into a MethodNode
                magnets = this_shape.out_magnets;
                this_magnet = magnets[0];  // only need one connection

                // is this magnet connected?
                if (this_magnet.connected === null) {
                    // unconnected input in graph, exit
                    submit_error.innerHTML = 'Unconnected input node';
                    return;
                }
            }
            else {
                method_nodes.push(this_shape);

                // all MethodNode in-magnets should have a Connector
                magnets = this_shape.in_magnets;
                for (j = 0; j < magnets.length; j++) {
                    this_magnet = magnets[j];
                    if (this_magnet.connected === null) {
                        submit_error.innerHTML = 'MethodNode with empty input socket';
                        return;
                    }
                }
            }
        }


        var form_data = {};

        // arguments to initialize new Pipeline Family
        // TODO: Disallow blank pipeline name and description fields
        form_data['family_name'] = $('#id_revision_name').val();
        form_data['family_desc'] = $('#id_revision_desc').val();

        // arguments to add first pipeline revision
        form_data['revision_name'] = '1';
        form_data['revision_desc'] = 'First version';

        // sort pipeline inputs by their Y-position on canvas
        function sortByYpos (a, b) {
            var ay = a.y;
            var by = b.y;
            return ((ay < by) ? -1 : ((ay > by) ? 1 : 0));
        }
        pipeline_inputs.sort(sortByYpos);

        // update form data with inputs
        var this_input;
        for (i = 0; i < pipeline_inputs.length; i++) {
            this_input = pipeline_inputs[i];
            form_data['create_input_'+i] = {'pk': this_input.pk,
            'dataset_name': this_input.label, 'dataset_idx': i+1,
            'x': this_input.x, 'y': this_input.y};
        }

        // append MethodNodes to sorted_elements Array in dependency order
        // see http://en.wikipedia.org/wiki/Topological_sorting#Algorithms
        var sorted_elements = [];
        var method_node;
        var this_parent;
        var okay_to_add;
        i = 0;
        while (method_nodes.length > 0) {
            for (j = 0; j < method_nodes.length; j++) {
                method_node = method_nodes[j];
                magnets = method_node.in_magnets;
                okay_to_add = true;

                for (var k = 0; k < magnets.length; k++) {
                    this_magnet = magnets[k];
                    // trace up the Connector
                    this_parent = this_magnet.connected.out_magnet.parent;
                    if (this_parent.constructor === MethodNode) {
                        if ($.inArray(this_parent, sorted_elements) < 0) {
                            // dependency not cleared
                            okay_to_add = false;
                            break;
                        }
                    }
                }

                if (okay_to_add) {
                    // either MethodNode has no dependencies
                    // or all dependencies already in sorted_elements
                    sorted_elements.push(method_nodes.splice(j, 1)[0]);
                }
            }
            i += 1;
            if (i > 5*shapes.length) {
                console.log('DEBUG: topological sort routine failed')
                return;
            }
        }

        var this_step;
        for (i = 0; i < sorted_elements.length; i++) {
            this_step = sorted_elements[i];
            // to create Method object
            form_data['pipeline_step_'+i] = {'pk': this_step.mid};
        }
        // this code written on Signal Hill, St. John's, Newfoundland
        // May 2, 2014 - afyp
        

        // PipelineStep [requires Pipeline]
        /*
            content_type (method or pipeline)
            object_id (pk to method/pipeline)
            transformation (foreign key to the method/pipeline)
            step_num (must be integer x > 0)
        */


        // PipelineInputCable [requires PipelineStep]
        /*
            source - output hole (magnet) of Transformation

            dest - input hole (magnet) of Transformation
         */

        $.ajax({
            type: 'POST',
            url: 'pipeline_add',
            data: JSON.stringify(form_data),
            datatype: 'json',
            success: function(result) {
                console.log(result);
            }
        })
    })
});

