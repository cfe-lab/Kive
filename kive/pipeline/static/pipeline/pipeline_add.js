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
    
    var pipelineCheckReadiness = function() {
        var $btn = $('#id_submit_button');
        if (!!canvasState.exec_order) {// exec_order is a 2D array if pipeline is executable, otherwise false
            $btn.addClass('pipeline-ready').removeClass('pipeline-not-ready');
        } else {
            $btn.removeClass('pipeline-ready').addClass('pipeline-not-ready');
        }
    };

    var showMenu = function(e) {
        e && e.stopPropagation();
        var $this = $(this),
            menu = $($this.data('rel')),
            inputs, input, preview_canvas, i;

        console.log($this);
        $('#id_ctrl_nav li').not(this).removeClass('clicked');
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
                    n_inputs  = Object.keys(result.inputs).length * 8 + 14;
                
                preview_canvas.height = (n_outputs + n_inputs) / 2 + 55;
                (new drydock_objects.MethodNode(
                    val,
                    null,// family
                    // Ensures node is centred perfectly on the preview canvas
                    // For this calculation to be accurate, method node draw params cannot change.
                    preview_canvas.width / 2 - 
                        (
                            Math.max(0, n_outputs - n_inputs + 48) - 
                            Math.max(0, n_outputs - n_inputs - 42)
                        ) * 0.4330127,// x
                    n_inputs / 2 + 20,// y
                    colour,
                    null,// label
                    result.inputs,
                    result.outputs
                )).draw(ctx);

                /*
                 * Update outputs fieldset list
                 * (not a part of this function's original mandate)
                 */
                var fieldset = $('#id_method_delete_outputs_details').empty();
                for (var i = 0, output; (output = result.outputs[i]); i++) {
                    fieldset.append(
                        $('<input>', {
                            type: 'checkbox',
                            name: 'dont_delete_outputs',
                            class: 'method_delete_outputs',
                            id: 'dont_delete_outputs_'+ output.dataset_idx,
                            value: output.dataset_name,
                            checked: 'checked'
                        }),
                        $('<label>', {
                            'for': 'dont_delete_outputs_'+ output.dataset_idx
                        }).text(output.dataset_name),
                        $('<br>')
                    );
                }
                $('#id_method_delete_outputs').prop('checked', true);
            });
        }
        e.stopPropagation();
    };

    var linkParentCheckbox = function() {
        var siblings = $(this).siblings('input').add(this),
            checked_inputs = siblings.filter(':checked').length,
            prop_obj = { indeterminate: false };

        if (checked_inputs < siblings.length && checked_inputs > 0) {
            prop_obj.indeterminate = true;
        } else {
            prop_obj.checked = (checked_inputs !== 0);
        }
        $('#id_method_delete_outputs').prop(prop_obj);
    };

    var linkChildCheckboxes = function() {
        $('#id_method_delete_outputs_details input')
            .prop('checked', $(this).is(':checked'));
    };

    var childCheckboxVisibilityCtrl = function() {
        var is_shown = $('#id_method_delete_outputs_details').is(':visible');

        $('#id_method_delete_outputs_details')
            [is_shown ? 'hide':'show']();
        $(this).text(
            is_shown ? '▸ List outputs':'▾ Hide list'
        );
    };

    var updateMethodRevisionsMenu = function() {
        var mf_id = this.value;
        if (mf_id !== '') {
            $('#id_method_revision_field').show().focus();
            return $.ajax({
                type: "GET",
                url: "/api/methodfamilies/" + mf_id + "/methods/",
                data: {}, // specify data as an object
                datatype: "json", // type of data expected back from server
                success: function(result) {
                    var options = [];
                    $.each(result, function(index, value) {
                        var option = $("<option>");
                        option.attr({
                            value: value.id,
                            title: value.revision_desc
                        }).text(
                            value.revision_number + ': ' + value.revision_name
                        );
                        options.push(option);
                    });
                    $("#id_select_method").show().empty().append(options).change();
                }
            });
        }
        $("#id_method_revision_field").hide();
        return $.Deferred().reject(); // No method family chosen, never loads.
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
                shape = new drydock_objects.RawNode(pos.left, pos.top, node_label);
            } else {
                shape = new drydock_objects.CdtNode(
                        parseInt(this_pk),
                        pos.left,
                        pos.top,
                        node_label);
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
            method_dont_delete_outputs = $('#id_method_delete_outputs_details input', this),
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
                        outputs)
            ;

            method_dont_delete_outputs.each(function() {
                if (!$(this).prop('checked')) {
                    method.outputs_to_delete.push(this.value);

                    for (var i = 0, magnet; (magnet = method.out_magnets[i]); i++) {
                        if (this.value === magnet.label)
                            magnet.toDelete = true;
                    }
                }
            });

            if ($('#id_method_button').val() == 'Add Method') {
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
            } else {
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
                $('#id_ctrl_nav li').add(menus).removeClass('clicked');
            }
            pipelineCheckReadiness();
        }
        
        // escape key closes menus
        else if (e.which == 27) {
            $('#id_ctrl_nav li').removeClass('clicked');
            $('.ctrl_menu:visible').trigger('cancel');
            canvasState.selection = [];
            canvasState.valid = false;
        }
    };

    var documentClickHandler = function(e) {
        var menus = $('.ctrl_menu, .context_menu, .modal_dialog').filter(':visible');
        if ($(e.target).closest(menus).length === 0) {
            menus.trigger('cancel');
            $('#id_ctrl_nav li').add(menus).removeClass('clicked');
        }
    };

    var resize_timeout = false;
    var documentResizeHandler = function() {
        var shape, i, scale_x, scale_y;

        canvasState.width  = canvas.width  = window.innerWidth;
        canvasState.height = canvas.height = window.innerHeight - $(canvas).offset().top - 5;
        
        scale_x = canvas.width  / canvasState.old_width;
        scale_y = canvas.height / canvasState.old_height;
            
        if (scale_x == 1 && scale_y == 1) {
            return;
        }
        
        for (i = 0; (shape = canvasState.shapes[i]); i++) {
            shape.x *= scale_x;
            shape.y *= scale_y;
            shape.dx *= scale_x;
            shape.dy *= scale_y;
        }
        
        canvasState.old_width = canvas.width;
        canvasState.old_height = canvas.height;
        canvasState.valid = false;

        clearTimeout(resize_timeout);
        resize_timeout = setTimeout(endDocumentResize, 500);
    };
    var endDocumentResize = function() {
        canvasState.valid = false;
        for (var i = 0, shape; (shape = canvasState.shapes[i]); i++) {
            shape.dx = shape.dy = 0;
            canvasState.detectCollisions(shape);
        }
        canvasState.outputZone.alignWithCanvas(canvas.width, canvas.height);
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
                    $('#id_select_colour', menu).val(sel.fill);
                    $('#colour_picker_pick', menu).css('background-color', sel.fill);

                    if ($('#id_method_delete_outputs_details', menu).is(':visible')) {
                        $('#id_method_delete_outputs_field .expand_outputs_ctrl', menu).trigger('click');
                    }

                    var $method_family = $('#id_select_method_family', menu);
                    $method_family.val(sel.family);
                    var request = updateMethodRevisionsMenu.call($method_family[0]); // trigger ajax
                    if (sel.new_code_resource_revision) {
                        request.done(function() {
                            var name = sel.new_code_resource_revision.revision_name;
                            $('<option>', { value: sel.pk })
                                .text('new: ' + name)
                                .prependTo($("#id_select_method", menu));
                        });
                    }
                    if (sel.new_dependencies) {
                        request.done(function() {
                            var name = "new: ";
                            for (var i = 0; i < sel.new_dependencies.length; i++) {
                                if (i > 0) name += ", ";
                                name += sel.new_dependencies[i].revision_name;
                            }
                            $('<option>', { value: sel.pk })
                                .text(name)
                                .prependTo($("#id_select_method", menu));
                        });
                    }
                    
                    // disable forms while ajax is loading
                    $('input', menu).prop('disabled', true);

                    // #id_method_revision_field is always populated via ajax.
                    // it will run this event exactly twice before killing it.
                    // first we execute, then we kill.
                    $(document).on('ajaxComplete', (function(method) {
                        var counter = 0;
                        return function() {
                            // only act on the second time this is called.
                            if (2 !== ++counter) return;
                            // don't run it a third time.
                            $(document).off('ajaxComplete');
                            $('input', menu).prop('disabled', false);

                            var checkboxes = $('#id_method_delete_outputs_details input', menu);
                            // wait for AJAX to populate drop-down before selecting option
                            $('#id_method_revision_field select', menu).val(method.pk);
                            $('#id_method_name', menu).val_(method.label).select();
                            checkboxes.each(function() {
                                $(this).prop('checked', -1 === method.outputs_to_delete.indexOf(this.value) );
                            });
                            linkParentCheckbox.call(checkboxes[0]);
                        };
                    })(sel) );
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
                if(sel instanceof drydock_objects.OutputNode || sel instanceof drydock_objects.CdtNode ||
                   sel instanceof drydock_objects.RawNode) {
                    window.location = '/dataset_view/' + sel.dataset_id + '?run_id=' + sel.run_id + "&view_run";
                }
            }
            if (action == 'download') {
                sel = sel[0];
                if(sel instanceof drydock_objects.OutputNode || sel instanceof drydock_objects.CdtNode ||
                   sel instanceof drydock_objects.RawNode) {
                    window.location = '/dataset_download/' + sel.dataset_id+ "&view_run";
                }
            }
            if (action == 'viewlog') {
                sel = sel[0];
                if(sel instanceof drydock_objects.MethodNode) {
                    window.location = '/stdout_view/' + sel.log_id + '?run_id=' + sel.run_id + "&view_run";
                }
            }
            if (action == 'viewerrorlog') {
                sel = sel[0];
                if(sel instanceof drydock_objects.MethodNode) {
                    window.location = '/stderr_view/' + sel.log_id + '?run_id=' + sel.run_id + "&view_run";
                }
            }
        }
        $('.context_menu').hide();
    };

    var submitPipeline = (function() {
        var $submit_error = $('#id_submit_error');
        return function(e) {
            /*
            Trigger AJAX transaction on submitting form.
             */

            e.preventDefault(); // override form submit action

            // Since a field contains its label on pageload, a field's label as its value is treated as blank
            $('input, textarea', this).each(function() {
                if (this.value == $(this).data('label')) {
                    this.value = '';
                }
            });

            $submit_error.empty();

            var is_new = e.data.action == "new",
                is_revision = e.data.action == "revise",
                // arguments to initialize new Pipeline Family
                family = {
                    name: $('#id_family_name'),  // hidden input if revision
                    desc: $('#id_family_desc')
                },
                revision = {
                    name: $('#id_revision_name'),
                    desc: $('#id_revision_desc')
                },
                users_allowed = [],
                groups_allowed = [];

            $("#id_permissions_0 option:selected").each(function() {
                users_allowed.push($(this).text());
            });
            $("#id_permissions_1 option:selected").each(function() {
                groups_allowed.push($(this).text());
            });
            
            // Form validation
            if (is_new) {
                if (family.name.val() === '') {
                    showMenu.apply( $('[data-rel="#id_family_ctrl"]', '#id_ctrl_nav')[0] );
                    family.name.addClass('submit-error-missing').focus();
                    submitError('Pipeline family must be named');
                    return;
                }
                family.name.add(family.desc).removeClass('submit-error-missing');
            }

            revision.desc.add(revision.name).removeClass('submit-error-missing');

            // Now we're ready to start
            // TODO: We should really push this into the Pipeline class
            var form_data = {
                users_allowed: users_allowed,
                groups_allowed: groups_allowed,

                // There is no PipelineFamily yet; we're going to create one.
                family: family.name.val(),
                family_desc: family.desc.val(),

                // arguments to add first pipeline revision
                revision_name: revision.name.val(),
                revision_desc: revision.desc.val(),
                revision_parent: is_revision ? JSON.parse($("#initial_data").text()).id : null,
                published: $('#published').prop('checked'),

                // Canvas information to store in the Pipeline object.
                canvas_width: canvas.width,
                canvas_height: canvas.height
            };

            try {
                window.pipeline_revision = window.pipeline_revision || new Pipeline(canvasState);

                // TODO: data cleaning should either be within pipeline_revision.serialize 
                // or within this context but probably not both.
                form_data = pipeline_revision.serialize(form_data);
            } catch(error) {
                submitError(error);
                return;
            }

            if(!is_new) {
                submitPipelineAjax($('#id_family_pk').val(), form_data);
            } else { // Pushing a new family
                submitPipelineFamilyAjax(JSON.stringify({
                    users_allowed: users_allowed,
                    groups_allowed: groups_allowed,
                    name: family.name.val(),
                    description: family.desc.val()
                }));
            }
        };// end exposed function - everything that follows is closed over
            
        function buildErrors(context, json, errors) {
            for (var field in json) {
                var value = json[field],
                    new_context = context;
                if (new_context.length) {
                    new_context += ".";
                }
                new_context += field;
                
                for (var i = 0; i < value.length; i++) {
                    var item = value[i];
                    if (typeof(item) === "string") {
                        errors.push(new_context + ": " + item);
                    } else {
                        buildErrors(new_context, item, errors);
                    }
                }
            }
        }
        function submitError(errors) {
            if (errors instanceof Array) {
                $submit_error.empty();
                for (var i = 0; i < errors.length; i++) {
                    $submit_error.append($('<p>').text(errors[i]));
                }
            } else {
                $submit_error.text(errors);
            }
            $submit_error.show();
        }
        function submitPipelineAjax(family_pk, form_data) {
            $.post({
                url: '/api/pipelines/',
                data: JSON.stringify(form_data),
                contentType: "application/json"// is this needed? jquery default is: application/x-www-form-urlencoded; charset=UTF-8
            }).success(function() {
                $('#id_submit_error').empty().hide();
                $(window).off('beforeunload');
                window.location.href = '/pipelines/' + family_pk;
            }).error(function(xhr, status, error) {
                var json = xhr.responseJSON,
                    errors = [];
                
                if (json) {
                    if (json.non_field_errors) {
                        submitError(json.non_field_errors);
                    } else {
                        buildErrors("", json, errors);
                        submitError(errors);
                    }
                } else {
                    submitError(xhr.status + " - " + error);
                }
            });
        }
        function submitPipelineFamilyAjax(form_data) {
            $.post({
                url: '/api/pipelinefamilies/',
                data: form_data,
                contentType: "application/json"// is this needed? jquery default is: application/x-www-form-urlencoded; charset=UTF-8
            }).success(function(result) {
                submitPipelineAjax(result.id, form_data);
            }).error(function(xhr, status, error) {
                var json = xhr.responseJSON,
                    serverErrors = json && json.non_field_errors || [];
                
                if (serverErrors.length === 0) {
                    serverErrors = xhr.status + " - " + error;
                }

                submitError(serverErrors);
            });
        }
    })();

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

    var checkForUnsavedChanges = function() {
        if (canvasState.can_edit && canvasState.has_unsaved_changes) {
            return 'You have unsaved changes.';
        }
    }
    
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
    $('input, textarea', '#pipeline_ctrl').not('[type="checkbox"],[type="radio"]').each(tuckLabelsIntoInputFields);
    
    /*
    EVENT BINDINGS TABLE
    ------------------------------------------------------------------------------------
     ELEMENT                         EVENT       (SELECTOR)  FUNCTION CALLBACK
    ------------------------------------------------------------------------------------
    */
    $(window)                    .on('resize',               documentResizeHandler)
                                 .on('beforeunload',         checkForUnsavedChanges);
    $(document)                  .on('keydown',              documentKeyHandler)
                                 .on('mousedown',            documentClickHandler)
                                 // this one is set separately so that it can be disabled separately
                                 .on('cancel', '.ctrl_menu', function() { $(this).hide(); })
                                 // do not combine this line with the previous
                                 .on('cancel', '.context_menu, .modal_dialog', function() { $(this).hide(); });
                                 // Handle jQuery-UI Dialog spawned for output cable
    $('form', '#dialog_form')    .on('submit',               submitOutputNodeName)        
                                 // Cancel is not a native event and can only be triggered via javascript
                                 .on('cancel',               cancelOutputNode);           
    $('#id_select_cdt')          .on('change',               updateCDtPreviewCanvas);
    $('#id_select_method')       .on('change',               updateMethodPreviewCanvas);
    $('form','#id_input_ctrl')   .on('submit',               createNewInputNode);         // Handle 'Inputs' menu
    $('form', '#id_method_ctrl') .on('submit',               submitMethodDialog)          // Handle 'Methods' menu
                                 .on('reset',                resetMethodDialog);
    $('#id_pipeline_new_form')   .submit({action: "new"},    submitPipeline);
    $('#id_pipeline_add_form')   .submit({action: "add"},    submitPipeline);
    $('#id_pipeline_revise_form').submit({action: "revise"}, submitPipeline);
    $('li', 'ul#id_ctrl_nav')    .on('click',                showMenu);
    $('.context_menu')           .on('click', 'li',          chooseContextMenuOption);    // when a context menu option is clicked
    $('#autolayout_btn')         .on('click',                canvasState.autoLayout.bind(canvasState) );
    $('.align-btn')              .on('click',                function() { canvasState.alignSelection($(this).data('axis')); });
    $('.form-inline-opts')       .on('click', 'input',       changeExecOrderDisplayOption);
    $('#colour_picker_pick')     .on('click',                showColourPicker);
    $('#colour_picker_menu')     .on('click', 'div',         pickColour);
    $("#id_select_method_family")           .on('change',    updateMethodRevisionsMenu)                             .trigger('change'); // Trigger on load
    $('#id_method_delete_outputs')          .on('change',    linkChildCheckboxes)                                   .trigger('change');
    $('#id_method_delete_outputs_field')    .on('click',   '.expand_outputs_ctrl',   childCheckboxVisibilityCtrl)   .trigger('click');
    $('#id_method_delete_outputs_details')  .on('change',  '.method_delete_outputs', linkParentCheckbox);
    /*
    ------------------------------------------------------------------------------------
    */
    
    $('.ctrl_menu').draggable();
    
    $('#id_revision_desc')
        .on({
            keydown: function() {
                var chars_per_smile = 12,
                    happy_mapped = -Math.min(15, Math.floor(this.value.length / chars_per_smile)) * 32;
                
                $('.happy_indicator').css(
                    'background-position-x',
                    happy_mapped + 'px'
                );
            },
            'focus keyup': function() {
                var desc_length = this.value.length,
                    $this = $(this),
                    indicator = $this.siblings('.happy_indicator'),
                    label = $this.siblings('.happy_indicator_label')
                ;
                
                if (desc_length === 0 || $this.hasClass('input-label')) {
                    indicator.add(label).hide();
                } else {
                    indicator.show();
                    label[ desc_length > 20 ? 'hide':'show' ]();
                }
            },
            blur: function() {
                $(this).siblings('.happy_indicator, .happy_indicator_label').hide();
            }
        })
        .trigger('keydown')
        .trigger('blur')
        .wrap('<div id="description_wrap">')
        .after('<div class="happy_indicator">')
        .after(
            $('<div class="happy_indicator_label">')
                .text('Keep typing!')
        )
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
