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
                            options.push('<option value="', value.pk, '">', value.fields.revision_number, ': ', value.fields.revision_name, '</option>');
                        });
                        $("#id_select_method").show().html(options.join(''));
                    }
                })
                
                $('#id_method_revision_field').show().focus();
            }
            else {
                $("#id_method_revision_field").hide();
            }
        }
    ).change(); // trigger on load

    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        $(this).wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
    });
    
    // Labels go within their input fields until they are filled in
    $('input, textarea', '#pipeline_ctrl').each(function() {
    
        var lbl = $('label[for="#' + this.id +'"]', '#pipeline_ctrl');
        
        if (lbl.length) {
            $(this).on('focus', function() {
            
                if (this.value == lbl.html()) {
                    $(this).removeClass('input-label').val('');
                }
                
            }).on('blur', function() {
            
                if (this.value === '') {
                    $(this).addClass('input-label').val(lbl.html());
                }
                
            }).data('label', lbl.html()).addClass('input-label').val(lbl.html());
            lbl.remove();
        }
        
    });
    
    $('li', 'ul#id_ctrl_nav').on('click', function(e) {
        var $this = $(this),
            menu = $($this.data('rel'));
        $('li', 'ul#id_ctrl_nav').not(this).removeClass('clicked');
        $this.addClass('clicked');
        $('.ctrl_menu', '#pipeline_ctrl').hide();
        menu.show().css('left', $this.offset().left);
//            .find('input').eq(0).focus(); // INCOMPLETE: Focus on the first input field when menu opens. Needs to be a bit different... first empty field maybe?
        e.stopPropagation();
    });

    $('a[rel="ctrl"]').on('click', function (e) {
        $(this).siblings('.fulltext').show().css({ top: e.pageY, left: e.pageX });
        setTimeout("$('.fulltext').fadeOut(300);", 2000);
    });


    // initialize animated canvas
    var canvas = document.getElementById('pipeline_canvas');
    var canvasWidth = window.innerWidth;
    var canvasHeight= window.innerHeight - 180;
    canvas.width = canvasWidth;
    canvas.height = canvasHeight;

    // TODO: can canvas be dynamically redrawn to fit window when it is resized?
//    $(window).resize(function() {    });


    var canvasState = new CanvasState(canvas);

    $('form','#id_input_ctrl').on('submit', function(e) {
        e.preventDefault(); // stop default form submission behaviour
        
        var choice = $('#id_select_cdt option:selected', this);
        var node_label = $('#id_datatype_name', this).val();

        if (node_label === '' || node_label === "Label") {
            // required field
            $('#id_dt_error', this)[0].innerHTML = "Label is required";
        }
        else {
            $('#id_dt_error', this)[0].innerHTML = "";
            var this_pk = choice.val(); // primary key
            if (this_pk == ""){
                canvasState.addShape(new RawNode(100, 200 + 50 * Math.random(),
                    20, '#88DD88', 10, 25, node_label
                ))
            } else {
                canvasState.addShape(new CDtNode(this_pk, 100, 200 + 50 * Math.random(),
                    40, '#8888DD', 10, 10, node_label
                ));
            }
            $('#id_datatype_name').val("");  // reset text field
        }
    });

    $('form', '#id_method_ctrl').on('submit', function(e) {
        e.preventDefault(); // stop default form submission behaviour
        
        var method_name = $('#id_method_name', this),
            method_error = $('#id_method_error', this),
            method_family = $('#id_select_method_family', this),
            method = $('#id_select_method', this);
        var mid = method.val(); // pk of method

        if (mid === undefined || method_family.val() == '') {
            method_error[0].innerHTML = "Select a Method";
            
            if (method.is(':visible')) {
                method.focus();
            } else {
                method_family.focus();
            }
            
        } else {
            // user selected valid Method Revision
            var node_label = method_name.val();

            if (node_label === '' || node_label === 'Label') {
                // required field
                method_error[0].innerHTML = "Label is required";
                method_name.focus();
            }
            else {
                method_error[0].innerHTML = '';
                var n_inputs = null,
                    n_outputs = null;

                // use AJAX to retrieve Revision inputs and outputs
                $.ajax({
                    type: "POST",
                    url: "get_method_io/",
                    data: { mid: mid }, // specify data as an object
                    datatype: "json", // type of data expected back from server
                    success: function(result) {
                        var inputs = result['inputs'],
                            outputs = result['outputs'];
                        canvasState.addShape(new MethodNode(mid, 200, 200 + 50 * Math.random(), 80, 10, 20, '#999999',
                            node_label, 10, inputs, outputs));

                        // x, y, w, inset, spacing, fill, label, offset, inputs, outputs
                        //canvasState.addShape(new MethodNode(200, 200 + 50 * Math.random(), 45, 5, 20, '#CCCCCC',
                        // node_label, 0, inputs, outputs));
                    }
                });

                method_name.val('');
            }
        }
    });

    $('#id_example_button').on('click', function () {
        /*
        Populate canvasState with objects for an example pipeline.
         */
        canvasState.clear();
        canvasState.shapes = [];
        canvasState.connectors = [];

        canvasState.addShape(
            new RawNode(100, 250, 20, '#88DD88', 10, 25, 'Unstructured data')
        );
        canvasState.addShape(
            new CDtNode(1, 100, 150, 40, '#8888DD', 10, 10, 'Strings')
        );
    });

    $('#id_reset_button').on('click', function() {
        // remove all objects from canvas
        canvasState.clear();
        // reset containers to reflect canvas
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
        // escape key closes menus
        // TODO: also should deselect any selected objects
        else if (e.which == 27) {
            $('li', 'ul#id_ctrl_nav').removeClass('clicked');
            $('#id_meta_ctrl, #id_method_ctrl, #id_input_ctrl', '#pipeline_ctrl').hide();
        }
    })
    
    $('#id_revision_desc').on('keydown', function() {
        var getHappierEachXChars = 12,
            happy = -Math.min(15, Math.floor(this.value.length / getHappierEachXChars)) * 32;
    
        $('.happy_indicator').css('background-position', happy + 'px 0px');
    })
        .trigger('keydown')
        .wrap('<div id="description_wrap">')
        .after('<div class="happy_indicator">')
        .after('<div class="happy_indicator_label">Keep typing to make me happy!</div>')
        .on('focus keyup', function() {
            var desc_length = this.value.length,
                wrap = $(this).parent();
            
            if (desc_length == 0 || $(this).hasClass('input-label')) {
                $('.happy_indicator, .happy_indicator_label', wrap).hide();
            }
            else if (desc_length > 20) {
                $('.happy_indicator', wrap).show();
                $('.happy_indicator_label', wrap).hide();
            }
            else {
                $('.happy_indicator, .happy_indicator_label', wrap).show();
            }
        }).on('blur', function() {
            $(this).siblings('.happy_indicator, .happy_indicator_label').hide();
        }).blur()
    ;

    
    $('body').on('click', function(e) {
        var menus = $('.ctrl_menu');
        if (menus.is(':visible') && $(e.target).closest(menus).length === 0) {
            menus.hide();
            $('li', 'ul#id_ctrl_nav').add(menus).removeClass('clicked');
        }
    });

    /* submit form */
    $('form#pipeline_ctrl').submit(function(e) {
        /*
        Trigger AJAX transaction on submitting form.
         */
        e.preventDefault(); // override form submit action

        console.log(canvasState);

        // Since a field contains its label on pageload, a field's label as its value is treated as blank
        $('input, textarea', this).each(function() {
            var $this = $(this);
            if ($this.val() == $this.data('label'))
                $this.val('');
        });
        
        var submit_error = $('#id_submit_error')[0];

        var shapes = canvasState.shapes;

        // check graph integrity
        var this_shape;
        var magnets;
        var this_magnet;
        var i, j;
        var pipeline_inputs = [];  // collect data nodes
        var method_nodes = [];
        var num_connections;

        submit_error.innerHTML = '';

        for (i = 0; i < shapes.length; i++) {
            this_shape = shapes[i];
            if (this_shape.constructor !== MethodNode) {
                // this is a data node
                pipeline_inputs.push(this_shape);

                // all CDtNodes or RawNodes (inputs) should feed into a MethodNode
                magnets = this_shape.out_magnets;
                this_magnet = magnets[0];  // data nodes only ever have one magnet

                // is this magnet connected?
                if (this_magnet.connected.length == 0) {
                    // unconnected input in graph, exit
                    submit_error.innerHTML = 'Unconnected input node';
                    return;
                }
            }
            else {
                method_nodes.push(this_shape);

                // at least one out-magnet must be occupied
                magnets = this_shape.out_magnets;
                num_connections = 0;
                for (j = 0; j < magnets.length; j++) {
                    this_magnet = magnets[j];
                    num_connections += this_magnet.connected.length;
                }
                if (num_connections === 0) {
                    console.log(this_magnet);
                    submit_error.innerHTML = 'MethodNode with unused outputs';
                    return;
                }
            }
        }

        // at least one Connector must terminate as pipeline output
        var connectors = canvasState.connectors;
        var this_connector;
        var pipeline_has_output = false;
        for (i = 0; i < connectors.length; i++) {
            this_connector = connectors[i];
            if (this_connector.in_magnet === '__output__') {
                pipeline_has_output = true;
            }
        }

        if (!pipeline_has_output) {
            submit_error.innerHTML = 'Pipeline has no output';
            return;
        }

        var form_data = {};

        // arguments to initialize new Pipeline Family
        var revision_name = $('#id_revision_name').val();
        var revision_desc = $('#id_revision_desc').val();

        if (revision_name === '') {
            submit_error.innerHTML = 'Pipeline must be named';
            return;
        }
        if (revision_desc === '') {
            submit_error.innerHTML = 'Pipeline must have a description';
            return;
        }

        form_data['family_name'] = revision_name;
        form_data['family_desc'] = revision_desc;

        // arguments to add first pipeline revision
        form_data['revision_name'] = '1';
        form_data['revision_desc'] = 'First version';

        // sort pipeline inputs by their Y-position on canvas (top to bottom)
        function sortByYpos (a, b) {
            var ay = a.y;
            var by = b.y;
            return ((ay < by) ? -1 : ((ay > by) ? 1 : 0));
        }
        pipeline_inputs.sort(sortByYpos);

        // update form data with inputs
        var this_input;
        form_data['pipeline_inputs'] = {};
        for (i = 0; i < pipeline_inputs.length; i++) {
            this_input = pipeline_inputs[i];
            form_data['pipeline_inputs'][i] = {
                'pk': (this_input.constructor===CDtNode) ? this_input.pk : -1,
                'dataset_name': this_input.label,
                'dataset_idx': i+1,
                'x': this_input.x,
                'y': this_input.y
            }
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
                    if (this_magnet.connected.length == 0) {
                        // unconnected in-magnet, still okay to add this MethodNode
                        continue;
                    }
                    // trace up the Connector
                    this_parent = this_magnet.connected[0].out_magnet.parent;  // in-magnets only have 1 connector
                    if (this_parent.constructor === MethodNode) {  // ignore connections from data nodes
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

        // sort output cables by y-position (top to bottom)
        var output_cables = [];
        for (i = 0; i < connectors.length; i++) {
            this_connector = connectors[i];
            if (this_connector.in_magnet === '__output__') {
                // connector terminates in output end-zone
                output_cables.push(this_connector);
            }
        }
        output_cables.sort(sortByYpos);

        // add arguments for input cabling
        var this_step;
        var this_source;

        form_data['pipeline_step'] = {};

        for (i = 0; i < sorted_elements.length; i++) {
            this_step = sorted_elements[i];

            form_data['pipeline_step'][i] = {
                'transformation_pk': this_step.pk,  // to retrieve Method
                'step_num': i+1,  // 1-index (pipeline inputs are index 0)
                'x': this_step.x,
                'y': this_step.y
            };

            // retrieve Connectors
            magnets = this_step.in_magnets;
            form_data['pipeline_step'][i]['cables_in'] = {};

            for (j = 0; j < magnets.length; j++) {
                this_magnet = magnets[j];
                if (this_magnet.connected.length == 0) {
                    continue;
                }
                this_connector = this_magnet.connected[0];
                this_source = this_connector.out_magnet.parent;

                if (this_source.constructor === MethodNode) {
                    form_data['pipeline_step'][i]['cables_in'][j] = {
                        'source': 'Method',
                        'source_pk': this_source.pk,
                        'source_dataset_name': this_connector.out_magnet.label,
                        'source_step': sorted_elements.indexOf(this_source)+1,
                        'dest': this_step.pk,
                        'dest_dataset_name': this_connector.in_magnet.label
                    };
                }
                else {
                    // sourced by pipeline input
                    form_data['pipeline_step'][i]['cables_in'][j] = {
                        'source': this_source.constructor === RawNode ? 'raw' : 'CDT',
                        'source_pk': this_source.constructor === RawNode ? '' : this_source.pk,
                        'source_dataset_name': this_connector.out_magnet.label,
                        'source_step': 0,
                        'dest': this_step.pk,
                        'dest_dataset_name': this_connector.in_magnet.label
                    };
                }
            }

            form_data['pipeline_step'][i]['cables_out'] = {};

            for (j = 0; j < output_cables.length; j++) {
                this_connector = output_cables[j];
                this_source = this_connector.out_magnet.parent;
                if (this_source !== this_step) {
                    // Connector does not originate from this step
                    continue;
                }
                form_data['pipeline_step'][i]['cables_out'][j] = {
                    'output_idx': j+1,
                    'source': this_step.pk,
                    'source_step': sorted_elements.indexOf(this_step) + 1, // 1-index
                    'dataset_name': this_connector.out_magnet.label,  // magnet label
                    'output_name': this_connector.out_magnet.label  // use same for now
                };
            }
        }
        // this code written on Signal Hill, St. John's, Newfoundland
        // May 2, 2014 - afyp

        console.log(form_data);


        // do AJAX transaction
        $.ajax({
            type: 'POST',
            url: 'pipeline_add',
            data: JSON.stringify(form_data),
            datatype: 'json',
            success: function(result) {
                console.log(result);
                if (result['status'] == 'failure') {
                    submit_error.innerHTML = result['error_msg'];
                }
            }
        })
    })
});

