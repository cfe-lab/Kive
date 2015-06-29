// place in global namespace to access from other files
var canvas, canvasState;

$(function() {
    noXSS();

    /*
    @todo
    
    - move as much html into html template as possible
    - greater and greater modularization
    
    */
    
    // initialize animated canvas
    canvas = document.getElementById('pipeline_canvas');
    var canvasWidth  = canvas.width  = window.innerWidth,
        canvasHeight = canvas.height = window.innerHeight - $(canvas).offset().top - 5,
        redrawInterval = 50; // ms
    
    canvasState = new drydock.CanvasState(canvas, redrawInterval);

    // TODO: Move this into a parameter for the submit method
    //window.submit_to_url = '/pipeline_add';
    var submitError = function(errors) {
        var $errorDiv = $('#id_submit_error');
        if (errors instanceof Array) {
            $errorDiv.empty();
            for (var i = 0; i < errors.length; i++) {
                $errorDiv.append($('<p/>').text(errors[i]));
            }
        }
        else {
            $errorDiv.text(errors);
        }
        $errorDiv.show();
    };
    
    var pipelineCheckReadiness = function() {
        var $btn = $('#id_submit_button');
        if (!!canvasState.exec_order) {// exec_order is a 2D array if pipeline is executable, otherwise false
            $btn.addClass('pipeline-ready').removeClass('pipeline-not-ready');
        } else {
            $btn.removeClass('pipeline-ready').addClass('pipeline-not-ready');
        }
    };
    var showMenu = function(e) {
        e.stopPropagation();
        var $this = $(this),
            menu = $($this.data('rel')),
            inputs, input, preview_canvas, i, default_input_value;
        $('li', 'ul#id_ctrl_nav').not(this).removeClass('clicked');
        $this.addClass('clicked');
        $('.ctrl_menu', '#pipeline_ctrl').hide();
        menu.show().css('left', $this.offset().left);
        if (menu.is('#id_method_ctrl')) {
            $('#id_method_button', menu).val('Add Method');
        }
        if ($this.hasClass('new_ctrl')) {
            menu.css({ left: 100, top: 350 }).addClass('modal_dialog');
            preview_canvas = $('canvas', menu)[0];
            preview_canvas.width = menu.innerWidth();
            preview_canvas.height = 60;
            $('#id_select_cdt').change();
        }
        $('form', menu).trigger('reset');
        inputs = menu.find('input');
        for (i = 0; i < inputs.length; i++) {
            input = inputs.eq(i);
            if (input.val_() === '') {
                input.val_('').focus();
                break;
            }
        }
    };
    var submitOutputNodeName = function(e) {
        // override ENTER key, click Create output button on form
        e.preventDefault();
        var $dialog = $(this).closest('#dialog_form'),
            out_node = $dialog.data('node'),
            label = $('#output_name').val(),
            shape, i;
        for (i = 0; i < canvasState.shapes.length; i++) {
            shape = canvasState.shapes[i];
            if (shape == out_node) continue;
            if (shape instanceof drydock_objects.OutputNode && shape.label == label) {
                $('#output_name_error').show();
                return false;
            }
        }
        $('#output_name_error').hide();
        out_node.label = label;
        canvasState.selection = [ out_node ];
        canvasState.valid = false;
        $dialog.hide();
    };
    var cancelOutputNode = function() {
        $(this).closest('#dialog_form').hide();
        $('#output_name_error').hide();
        canvasState.connectors.pop();
        canvasState.valid = false;
    };
    var updateCDtPreviewCanvas = function(e) {
        // Update preview picture of node to show a CDtNode or RawNode appropriately
        var preview_canvas = $(this).closest('.modal_dialog').find('canvas'),
            ctx;
        
        if (preview_canvas.length > 0) {
            preview_canvas = preview_canvas[0];
            ctx = preview_canvas.getContext('2d');
            ctx.clearRect(0, 0, preview_canvas.width, preview_canvas.height);
            if (this.value === '') {
                (new drydock_objects.RawNode(preview_canvas.width/2, preview_canvas.height/2)).draw(ctx);
            } else {
                (new drydock_objects.CdtNode(this.value, preview_canvas.width/2, preview_canvas.height/2)).draw(ctx);
            }
        }
        e.stopPropagation();
    };
    var updateMethodPreviewCanvas = function(e) {
        // Update preview picture of node to show a CDtNode or RawNode appropriately
        var preview_canvas = $(this).closest('.modal_dialog').find('canvas'),
            val = this.value,
            ctx, filename, colour;
        
        if (preview_canvas.length) {
            preview_canvas = preview_canvas[0];
            ctx = preview_canvas.getContext('2d');
            filename = $(this).find('option:selected').attr('title');
            colour = $(this).closest('.modal_dialog').find('#id_select_colour').val();
            $('#id_method_name').val_(filename);
            
            // use AJAX to retrieve Revision inputs and outputs
            $.getJSON("/api/methods/" + val + "/").done(function(result) {
                ctx.clearRect(0, 0, preview_canvas.width, preview_canvas.height);
                var n_outputs = Object.keys(result.outputs).length * 8,
                    n_inputs  = Object.keys(result.inputs).length * 8;
                
                preview_canvas.height = (n_outputs + n_inputs) / 2 + 62;
                (new drydock_objects.MethodNode(
                    val,
                    null,//family
                    // Ensures node is centred perfectly on the preview canvas
                    // Makes assumptions that parameters like magnet radius and scoop length are default.
                    preview_canvas.width/2 - ( Math.min(-n_inputs - 14, 42 - n_outputs) + Math.max(n_inputs + 14, n_outputs + 48) ) * 0.4330127,// x
                    n_inputs / 2 + 27,// y
                    colour, 
                    null,//label
                    result.inputs,
                    result.outputs
                )).draw(ctx);
            });
        }
        e.stopPropagation();
    };
    var updateMethodRevisionsMenu = function() {
        var mf_id = this.value;
        if (mf_id !== '') {
            $.ajax({
                type: "POST",
                url: "/get_method_revisions/",
                data: { mf_id: mf_id }, // specify data as an object
                datatype: "json", // type of data expected back from server
                success: function(result) {
                    var options = [];
                    var arr = JSON.parse(result);
                    $.each(arr, function(index,value) {
                        var option = $("<option>");
                        option.attr({
                            value: value.pk,
                            title: value.fields.filename
                        }).text(
                            value.fields.method_number + ': ' + value.fields.method_name
                        );
                        options.push(option);
                    });
                    $("#id_select_method").show().empty().append(options).change();
                }
            });
            $('#id_method_revision_field').show().focus();
        }
        else {
            $("#id_method_revision_field").hide();
        }
    };
    var createNewInputNode = function(e) {
        e.preventDefault(); // stop default form submission behaviour
        
        var node_label = $('#id_datatype_name', this).val(),
            pos,
            dlg = $(this).closest('.modal_dialog'),
            preview_canvas = dlg.find('canvas'),
            dt_error = $('#id_dt_error', this)[0];
        
        if (dlg.length) {
            pos = preview_canvas.offset();
            pos.left += preview_canvas[0].width/2  - canvas.offsetLeft;
            pos.top  += preview_canvas[0].height/2 - canvas.offsetTop;
        } else {
            pos = { left: 100, top: 200 + Math.round(50 * Math.random()) };
        }
        
        // check for duplicate names
        for (var i = 0; i < canvasState.shapes.length; i++) {
            shape = canvasState.shapes[i];
            if ((shape instanceof drydock_objects.RawNode ||
                    shape instanceof drydock_objects.CdtNode) &&
                    shape.label === node_label) {
                dt_error.innerHTML = 'That name has already been used.';
                return false;
            }
        }
        
        if (node_label === '' || node_label === "Label") {
            // required field
            dt_error.innerHTML = "Label is required";
        } else {
            dt_error.innerHTML = "";
            var this_pk = $('#id_select_cdt', this).val(), // primary key
                shape;
            
            if (this_pk === ""){
                shape = new drydock_objects.RawNode(         pos.left, pos.top, node_label);
            } else {
                shape = new drydock_objects.CdtNode(this_pk, pos.left, pos.top, node_label);
            }
            canvasState.addShape(shape);
            canvasState.detectCollisions(shape, 0);// Second arg: Upon collision, move new shape 0% and move existing objects 100%
            $('#id_datatype_name').val('');  // reset text field
            dlg.removeClass('modal_dialog').hide();
        }
    };
    var submitMethodDialog = function(e) {
        e.preventDefault(); // stop default form submission behaviour
        
        var method_name = $('#id_method_name', this),
            method_error = $('#id_method_error', this),
            method_family = $('#id_select_method_family', this),
            method_colour = $('#id_select_colour', this),
            method = $('#id_select_method', this),
            mid = method.val(), // pk of method
            pos,
            dlg = $(this).closest('.modal_dialog'),
            preview_canvas = dlg.find('canvas');
        
        // locally-defined function has access to all variables in parent function scope
        var createOrReplaceMethodNode = function(result) {
            var inputs = result.inputs,
                outputs = result.outputs,
                method = new drydock_objects.MethodNode(
                        mid, 
                        method_family.val(), 
                        pos.left,
                        pos.top,
                        method_colour.val(), 
                        node_label,
                        inputs,
                        outputs);

            if (document.getElementById('id_method_button').value == 'Add Method') {
                // create new MethodNode
                canvasState.addShape(method);
            } else {
                // replace the selected MethodNode
                // if user clicks anywhere else, MethodNode is deselected
                // and Methods menu closes

                // draw new node over old node
                var old_node = canvasState.selection[0];
                canvasState.replaceMethod(old_node, method);

                canvasState.selection = [ method ];
            }
            
            dlg.removeClass('modal_dialog').hide();
        };

        if (dlg.length) {
            pos = preview_canvas.offset();
            pos.left += preview_canvas[0].width/2  - canvas.offsetLeft;
            pos.top  += preview_canvas[0].height/2 - canvas.offsetTop;
        } else {
            pos = { left: 100, top: 200 + Math.round(50 * Math.random()) };
        }
        
        if (mid === undefined || method_family.val() === '') {
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
                
                // use AJAX to retrieve Revision inputs and outputs
                $.getJSON("/api/methods/" + mid + "/").done(
                        createOrReplaceMethodNode);

                method_name.val_('');
            }
        }
    };
    var resetMethodDialog = function() {
        var method_family = $('#id_select_method_family', this);
        $('#id_method_name', this).val_('');
        method_family.val(method_family.children('option').eq(0)).change();
    };
    var documentKeyHandler = function(e) {
        // backspace or delete key also removes selected object
        if ([8,46].indexOf(e.which) > -1 && !$(e.target).is("input, textarea")) {
            // prevent backspace from triggering browser to navigate back one page
            e.preventDefault();
            if (canvasState.selection) {
                canvasState.deleteObject();
                var menus = $('.ctrl_menu, .context_menu, .modal_dialog').filter(':visible');
                menus.trigger('cancel');
                $('li', 'ul#id_ctrl_nav').add(menus).removeClass('clicked');
            }
            pipelineCheckReadiness();
        }
        
        // escape key closes menus
        else if (e.which == 27) {
            $('li', 'ul#id_ctrl_nav').removeClass('clicked');
            $('.ctrl_menu:visible').trigger('cancel');
            canvasState.selection = [];
            canvasState.valid = false;
        }
    };
    var documentClickHandler = function(e) {
        var menus = $('.ctrl_menu, .context_menu, .modal_dialog').filter(':visible');
        if ($(e.target).closest(menus).length === 0) {
            menus.trigger('cancel');
            $('li', 'ul#id_ctrl_nav').add(menus).removeClass('clicked');
        }
    };
    var documentResizeHandler = function() {
        var shape, i, scale_x, scale_y, out_z,
            canvasWidth, canvasHeight;
        canvasWidth  = canvasState.width = canvas.width  = window.innerWidth;
        canvasHeight = canvasState.height = canvas.height = window.innerHeight - $(canvas).offset().top - 5;
        
        scale_x = canvas.width  / canvasState.old_width;
        scale_y = canvas.height / canvasState.old_height;
            
        if (scale_x == 1 && scale_y == 1) {
            return;
        }
        
        for (i=0; i < canvasState.shapes.length; i++) {
            shape = canvasState.shapes[i];
            shape.x *= scale_x;
            shape.y *= scale_y;
            shape.dx = shape.dy = 0;
            canvasState.detectCollisions(shape);
        }
        
        out_z = canvasState.outputZone;
        out_z.x = canvasWidth * 0.8;
        out_z.h = out_z.w = canvasWidth * 0.15;
        while (out_z.h + out_z.y > canvasHeight) {
            out_z.h /= 1.5;
        }
        
        canvasState.old_width = canvas.width;
        canvasState.old_height = canvas.height;
        canvasState.valid = false;
    };
    var chooseContextMenuOption = function(e) {
        e.stopPropagation();

        var $this = $(this),
            sel = canvasState.selection;
        
        // if there's a current node selected on the canvas
        if (sel && sel.length > 0) {
            var action = $this.data('action');
            
            if (action == 'edit' && sel.length == 1) {
                sel = sel[0];
                
                if (sel instanceof drydock_objects.MethodNode) {
                    /*
                        Open the edit dialog (rename, method selection, colour picker...)
                    */
                    var menu = $('#id_method_ctrl').show().addClass('modal_dialog'),
                        preview_canvas = $('canvas', menu)[0],
                        inputs_width = sel.n_inputs * 4 + 7,
                        outputs_width = sel.n_outputs * 4 + 24;

                    preview_canvas.width = menu.innerWidth();
                    menu.css({
                        top:  sel.y + sel.dy - inputs_width + canvas.offsetTop - 29,
                        left: sel.x + sel.dx - preview_canvas.width/2 + 0.8660254 * Math.min(Math.max(outputs_width - inputs_width, 0), 45) + canvas.offsetLeft - 9
                    });
                    $('#id_select_colour').val(sel.fill);
                    $('#colour_picker_pick').css('background-color', sel.fill);
                    $('#id_select_method_family').val(sel.family).change();  // trigger ajax
                    
                    // #id_method_revision_field is always populated via ajax.
                    // $.one() will run this event exactly once before killing it.
                    // first we execute, then we kill.
                    $(document).one('ajaxComplete', function() {
                        // wait for AJAX to populate drop-down before selecting option
                        $('#id_method_revision_field select').val(sel.pk);
                        $('#id_method_name').val_(sel.label).select();
                    });
                }
                else if (sel instanceof drydock_objects.OutputNode) {
                    /*
                        Open the renaming dialog
                    */
                    var dialog = $("#dialog_form");
                    
                    dialog.data('node', sel).show().css({
                        left: sel.x + sel.dx + canvas.offsetLeft - dialog.width()/2,
                        top:  sel.y + sel.dy + canvas.offsetTop - dialog.height()/2 - sel.h/2 - sel.offset
                    }).addClass('modal_dialog');
                    
                    $('#output_name_error').hide();
                    $('#output_name', dialog).val(sel.label).select(); // default value;
                }
            }
            if (action == 'delete') {
                canvasState.deleteObject();
            }
            if (action == 'display') {
                sel = sel[0];
                if(sel instanceof drydock_objects.OutputNode)
                    window.location = '/dataset_view/' + sel.dataset_id + '?rtp_id=' + sel.rtp_id;
            }
            if (action == 'download') {
                sel = sel[0];
                if(sel instanceof drydock_objects.OutputNode)
                    window.location = '/dataset_download/' + sel.dataset_id;
            }
            if (action == 'viewlog') {
                sel = sel[0];
                if(sel instanceof drydock_objects.MethodNode)
                    window.location = '/stdout_view/' + sel.log_id + '?rtp_id=' + sel.rtp_id;
            }
            if (action == 'viewerrorlog') {
                sel = sel[0];
                if(sel instanceof drydock_objects.MethodNode)
                    window.location = '/stderr_view/' + sel.log_id + '?rtp_id=' + sel.rtp_id;
            }
        }
        $('.context_menu').hide();
    };
    var submitPipeline = function(e) {
        /*
        Trigger AJAX transaction on submitting form.
         */

        e.preventDefault(); // override form submit action

        // Since a field contains its label on pageload, a field's label as its value is treated as blank
        $('input, textarea', this).each(function() {
            if (this.value == $(this).data('label'))
                this.value = '';
        });
        
        var shapes = canvasState.shapes,
            this_shape,
            magnets,
            this_magnet,
            this_connector,
            i, j,
            pipeline_inputs = [],  // collect data nodes
            pipeline_outputs = [],
            method_nodes = [],
            num_connections;

        document.getElementById('id_submit_error').innerHTML = '';

        var is_revision = 0 < $('#id_pipeline_select').length;

        // arguments to initialize new Pipeline Family
        var family_name = $('#id_family_name').val(),  // hidden input if revision
            family_desc = $('#id_family_desc').val(),
            family_pk = $('#id_family_pk').val(),
            revision_name = $('#id_revision_name').val(),
            revision_desc = $('#id_revision_desc').val(),
            users_allowed = $("#id_permissions_0").val(),
            groups_allowed = $("#id_permissions_1").val();

        // Form validation
        if (!is_revision) {
            if (family_name === '') {
                // FIXME: is there a better way to do this trigger?
                $('li', 'ul#id_ctrl_nav')[0].click();
                $('#id_family_name').css({'background-color': '#ffc'}).focus();
                submitError('Pipeline family must be named');
                return;
            }
            $('#id_family_name, #id_family_desc').css('background-color', '#fff');
        }

        $('#id_revision_name, #id_revision_desc').css('background-color', '#fff');

        // Now we're ready to start
        // TODO: We should really push this into the Pipeline class
        var form_data = {
            users_allowed: users_allowed || [],
            groups_allowed: groups_allowed || [],

            // There is no PipelineFamily yet; we're going to create one.
            family: family_name,
            family_desc: family_desc,

            // arguments to add first pipeline revision
            revision_name: revision_name,
            revision_desc: revision_desc,
            revision_parent: is_revision ? $('#id_pipeline_select').val() : null,

            // Canvas information to store in the Pipeline object.
            canvas_width: canvas.width,
            canvas_height: canvas.height,
        };

        try {
            window.pipeline_revision = window.pipeline_revision || new Pipeline(canvasState);
            form_data = pipeline_revision.serialize(form_data);
        } catch(error) {
            submitError(error);
            return;
        }

        function submit_pipeline(){
            $.ajax({
                type: 'POST',
                url: '/api/pipelines/',
                data: {'_content': JSON.stringify(form_data), "_content_type": "application/json"},
                dataType: 'json',
                success: function(result) {
                    $('#id_submit_error').html('').hide();
                    window.location.href = '/pipelines';
                },
                error: function(xhr, status, error) {
                    var errors = [],
                        json = xhr.responseJSON,
                        serverErrors = json && json.non_field_errors || [];
                    
                    if (serverErrors.length === 0) {
                        submitError(xhr.status + " - " + error);
                    }
                    else {
                        submitError(serverErrors);
                    }
                }
            });
        }
        if(is_revision)
            submit_pipeline();

        else // Pushing a new family
            $.ajax({
                type: 'POST',
                url: '/api/pipelinefamilies/',
                data: {'_content': JSON.stringify({
                    users_allowed: users_allowed || [],
                    groups_allowed: groups_allowed || [],
                    name: family_name,
                    description: family_desc,
                }),
                "_content_type": "application/json"},
                dataType: 'json',
                success: function(result) {
                    submit_pipeline();
                },
                error: function(xhr, status, error) {
                    var errors = [],
                        json = xhr.responseJSON,
                        serverErrors = json && json.non_field_errors || [];
                    
                    if (serverErrors.length === 0) {
                        submitError(xhr.status + " - " + error);
                    }
                    else {
                        submitError(serverErrors);
                    }
                }
            });

    };
    var changeExecOrderDisplayOption = function() {
        var $this = $(this),
            val = $this.val(),
            val_map = { always: true, never: false, ambiguous: undefined };

        if ($this.is(':checked') && val_map.hasOwnProperty(val)) {
            canvasState.force_show_exec_order = val_map[val];
            canvasState.valid = false;
        }
    };
    var showColourPicker = function() {
        var pos = $(this).position();
        $('#colour_picker_menu').css({ top: pos.top + 20, left: pos.left }).show();
    };
    var pickColour = function() {
        var bg_col = $(this).css('background-color');
        $('#colour_picker_pick').css('background-color', bg_col);
        $('#id_select_colour').val(bg_col);
        $('#colour_picker_menu').hide();
        $('#id_select_method').trigger('change');
    };
    var tuckLabelsIntoInputFields = function() {
        var lbl = $('label[for="' + this.id +'"]', '#pipeline_ctrl'),
            lbl_txt = lbl.text();
        
        if (lbl.length > 0) {
            $(this).on('focus', function() {
                if (this.value === lbl_txt) {
                    $(this).removeClass('input-label').val('');
                }
            }).on('blur', function() {
                if (this.value === '') {
                    $(this).addClass('input-label').val(lbl_txt);
                }
            }).data('label', lbl_txt).addClass('input-label').val(lbl_txt);
            lbl.remove();
        }
    };
    
    pipelineCheckReadiness();
    
    // de-activate double-click selection of text on page
    canvas.addEventListener('selectstart', function(e) { e.preventDefault(); return false; }, false);
    canvas.addEventListener('mousedown',   function(e) { canvasState.doDown(e); }, true);
    canvas.addEventListener('mousemove',   function(e) { canvasState.doMove(e); }, true);
    canvas.addEventListener('mouseup',     function(e) { canvasState.doUp(e); pipelineCheckReadiness(e); }, true);
    canvas.addEventListener('contextmenu', function(e) { canvasState.contextMenu(e); }, true);
    
    canvasState.old_width = canvasWidth;
    canvasState.old_height = canvasHeight;


    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        $(this).wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
    });
    
    // Labels go within their input fields until they are filled in
    $('input, textarea', '#pipeline_ctrl').each(tuckLabelsIntoInputFields);
    
    /*
    EVENT BINDINGS TABLE
    --------------------------------------------------------------------------
     ELEMENT                         EVENT             FUNCTION CALLBACK
    --------------------------------------------------------------------------
    */
    $(window)                    .on('resize',         documentResizeHandler);
    $(document)                  .on('keydown',        documentKeyHandler)
                                 .on('mousedown',      documentClickHandler)
                                 .on('cancel', '.context_menu, .modal_dialog, .ctrl_menu', function() { $(this).hide(); });
    $('form', '#dialog_form')    .on('submit',         submitOutputNodeName)        // Handle jQuery-UI Dialog spawned for output cable
                                 .on('cancel',         cancelOutputNode);           // Cancel is not a native event and can only be triggered via javascript
    $('#id_select_cdt')          .on('change',         updateCDtPreviewCanvas);
    $('#id_select_method')       .on('change',         updateMethodPreviewCanvas);
    $("#id_select_method_family").on('change',         updateMethodRevisionsMenu)   // Update method drop-down
                                 .trigger('change');                             // Trigger on load
    $('form','#id_input_ctrl')   .on('submit',         createNewInputNode);         // Handle 'Inputs' menu
    $('form', '#id_method_ctrl') .on('submit',         submitMethodDialog)          // Handle 'Methods' menu
                                 .on('reset',          resetMethodDialog);
    $('#id_pipeline_form')       .on('submit',         submitPipeline);
    $('li', 'ul#id_ctrl_nav')    .on('click',          showMenu);
    $('.context_menu')           .on('click', 'li',    chooseContextMenuOption);    // when a context menu option is clicked
    $('#autolayout_btn')         .on('click',          function() { canvasState.autoLayout(); });
    $('.align-btn')              .on('click',          function() { canvasState.alignSelection($(this).data('axis')); });
    $('.form-inline-opts')       .on('click', 'input', changeExecOrderDisplayOption);
    $('#colour_picker_pick')     .on('click',          showColourPicker);
    $('#colour_picker_menu')     .on('click', 'div',   pickColour);
    /*
    --------------------------------------------------------------------------
    */
    
    $('.ctrl_menu').draggable();
    
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
            
            if (desc_length === 0 || $(this).hasClass('input-label')) {
                $('.happy_indicator, .happy_indicator_label', wrap).hide();
            } else if (desc_length > 20) {
                $('.happy_indicator', wrap).show();
                $('.happy_indicator_label', wrap).hide();
            } else {
                $('.happy_indicator, .happy_indicator_label', wrap).show();
            }
        }).on('blur', function() {
            $(this).siblings('.happy_indicator, .happy_indicator_label').hide();
        }).blur()
    ;
});// end of document.ready()


jQuery.fn.extend({
    val_: function(str) {
        // wrapper function for changing <input> values with added checks. replaces .val().
        
        if (typeof str == 'undefined') {
            if (this.val() == $('label[for="' + this[0].id +'"]', '#pipeline_ctrl').html()) {
                return "";
            } else {
                return this.val();
            }
        } else {
            this.val(str);
            if (this.is('input, textarea') && this.closest('#pipeline_ctrl').length > 0) {
                var data_lbl = this.data('label'),
                    lbl;
                if (typeof data_lbl !== 'undefined') {
                    lbl = data_lbl;
                } else {
                    lbl = $('label[for="' + this[0].id +'"]', '#pipeline_ctrl');
                    if (lbl.length === 0) {
                        return this;
                    } else {
                        lbl = lbl.html();
                    }
                    this.data('label', lbl);
                }
                if (str === lbl) {
                    this.addClass('input-label');
                } else {
                    this.removeClass('input-label');
                }
            }
            return this;
        }
    },
    draggable: function(opt) {
        opt = $.extend({ handle: '', cursor: 'normal' }, opt);
        var $el = opt.handle === '' ? this : this.find(opt.handle);
        
        $el.find('input, select, textarea').on('mousedown', function(e) {
            e.stopPropagation();
        });
        
        $el.css('cursor', opt.cursor).on("mousedown", function(e) {
            var $drag;
            if (opt.handle === '') {
                $drag = $(this).addClass('draggable');
            } else {
                $drag = $(this).addClass('active-handle').parent().addClass('draggable');
            }  
            
            $drag.data('z', $drag.data('z') || $drag.css('z-index'));
            
            var z = $drag.data('z'),
                pos = $drag.offset(),
                pos_y = pos.top - e.pageY,
                pos_x = pos.left - e.pageX;
            
            $drag.css('z-index', 1000).parents().off('mousemove mouseup').on("mousemove", function(e) {
                $('.draggable').offset({
                    top:  e.pageY + pos_y,
                    left: e.pageX + pos_x
                });
            }).on("mouseup", function() {
                $(this).removeClass('draggable').css('z-index', z);
            });
            
            e.preventDefault(); // disable selection
        }).on("mouseup", function() {
            if (opt.handle === "") {
                $(this).removeClass('draggable');
            } else {
                $(this).removeClass('active-handle').parent().removeClass('draggable');
            }
        });
        
        return $el;
    }
});
