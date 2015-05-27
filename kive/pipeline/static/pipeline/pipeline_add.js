// place in global namespace to access from other files
var canvas,
    canvasState,
    submit_to_url = '/pipeline_add',
    submitError = function(error) {
        $('#id_submit_error').show().text(error);
    };
/*
legacy code

var rawNodeWidth = 20,
    rawNodeHeight = 25,
    rawNodeColour = "#8D8",
    rawNodeInset = 10,
    rawNodeOffset = 25;

var cdtNodeWidth = 45,
    cdtNodeHeight = 28,
    cdtNodeColour = '#88D',
    cdtNodeInset = 13,
    cdtNodeOffset = 15;

var mNodeWidth = 80,
    mNodeInset = 10,
    mNodeSpacing = 20,
    mNodeColour = '#999',
    mNodeOffset = 10;
*/

function Menu(selector, button) {
    this.dialog = $(selector);
    this.button = button ? $(button) : $('#id_ctrl_nav li').filter(function() { return $(this).data('rel') === selector; });
    this.form = $('form', this.dialog);
    this.elements = $('input, select, textarea', this.form);
    this.is_modal = false;

    var menu = this;
    this.form.on('submit', function(e) { e.preventDefault(); menu.submit(e); })
}
Menu.prototype = {
    open: function() {
        var i, inputs, input, default_input_value, preview_canvas;
        this.button.addClass('clicked');
        this.dialog.show();
        this.form.trigger('reset');

        if (this.is_modal) {
            this.dialog.css({ left: 100, top: 350 });
            preview_canvas = $('canvas', this.dialog)[0];
            preview_canvas.width = this.dialog.innerWidth();
            preview_canvas.height = 60;
            $('#id_select_cdt').change();
        }
        inputs = $('input', this.dialog);
        for (i = 0; i < inputs.length; i++) {
            input = inputs[i];
            default_input_value = $('label[for="' + input.id +'"]', '#pipeline_ctrl').html();
            if (input.value === '' || input.value === default_input_value) {
                $(input).val_('').focus();
                break;
            }
        }
    }
};

jQuery.fn.extend({
    val_: function(str) {
        // wrapper function for changing <input> values with added checks. replaces .val().
        this.val(str);
        
        if (this.is('input, textarea') && this.closest('#pipeline_ctrl').length > 0) {
            var data_lbl = this.data('label'),
                lbl;
            
            if (typeof data_lbl !== 'undefined') {
                lbl = data_lbl;
            } else {
                lbl = $('label[for="' + this[0].id +'"]', '#pipeline_ctrl');
                if (lbl.length == 0) {
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
                $(this).removeClass('draggable')
            } else {
                $(this).removeClass('active-handle').parent().removeClass('draggable');
            }
        });
        
        return $el;
    }
});

$(function() { // wait for page to finish loading before executing jQuery code
    // initialize animated canvas
    canvas = $('#pipeline_canvas')[0];
    var canvasWidth  = canvas.width  = window.innerWidth,
        canvasHeight = canvas.height = window.innerHeight - $(canvas).offset().top - 5;
    
    canvasState = new CanvasState(canvas);
    
    // de-activate double-click selection of text on page
    canvas.addEventListener('selectstart', function(e) { e.preventDefault(); return false; }, false);
    canvas.addEventListener('mousedown',   function(e) { canvasState.doDown(e); }, true);
    canvas.addEventListener('mousemove',   function(e) { canvasState.doMove(e); }, true);
    canvas.addEventListener('mouseup',     function(e) { canvasState.doUp(e); }, true);
    canvas.addEventListener('contextmenu', function(e) { canvasState.contextMenu(e); }, true);
    
    canvasState.old_width = canvasWidth;
    canvasState.old_height = canvasHeight;

    // trigger ajax on CR drop-down to populate revision select
    $(document).ajaxSend(function(event, xhr, settings) {
        /*
            from https://docs.djangoproject.com/en/1.3/ref/contrib/csrf/#csrf-ajax
            On each XMLHttpRequest, set a custom X-CSRFToken header to the value of the CSRF token.
            ajaxSend is a function to be executed before an Ajax request is sent.
        */

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
            var host = document.location.host, // host + port
                protocol = document.location.protocol,
                sr_origin = '//' + host,
                origin = protocol + sr_origin;
            
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
    $("#id_select_method_family").on('change', function() {
        mf_id = this.value;
        if (mf_id !== '') {
            $.ajax({
                type: "POST",
                url: "/get_method_revisions/",
                data: { mf_id: mf_id }, // specify data as an object
                datatype: "json", // type of data expected back from server
                success: function(result) {
                    var options = [];
                    var arr = JSON.parse(result)
                    $.each(arr, function(index,value) {
                        options.push('<option value="', value.pk, '" title="', value.fields.filename, '">', value.fields.method_number, ': ', value.fields.method_name, '</option>');
                    });
                    $("#id_select_method").show().html(options.join('')).change();
                }
            });
            
            $('#id_method_revision_field').show().focus();
        }
        else {
            $("#id_method_revision_field").hide();
        }
    }).change(); // trigger on load

    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        $(this).wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
    });
    
    // Labels go within their input fields until they are filled in
    $('input, textarea', '#pipeline_ctrl').each(function() {
        var lbl = $('label[for="' + this.id +'"]', '#pipeline_ctrl');
        
        if (lbl.length > 0) {
            $(this).on('focus', function() {
                if (this.value === lbl.html()) {
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
    
    $('.ctrl_menu').draggable();
    
    $('li', 'ul#id_ctrl_nav').on('click', function(e) {
        var $this = $(this),
            menu = $($this.data('rel')),
            inputs, input, preview_canvas, i, default_input_value
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
            input = inputs[i];
            default_input_value = $('label[for="' + input.id +'"]', '#pipeline_ctrl').html();
            if (input.value === '' || input.value === default_input_value) {
                $(input).val_('').focus();
                break;
            }
        }
        e.stopPropagation();
    });

    // Handle jQuery-UI Dialog spawned for output cable
    $('form', '#dialog_form').on('submit', function(e) {
        // override ENTER key, click Create output button on form
        e.preventDefault();
        var dialog = $(this).closest('#dialog_form'),
            out_node = dialog.data('node'),
            label = $('#output_name').val(),
            shape;
        for (var i = 0; i < canvasState.shapes.length; i++) {
            shape = canvasState.shapes[i];
            if (shape == out_node) continue;
            if (shape instanceof OutputNode && shape.label == label) {
                $('#output_name_error').show();
                return false;
            }
        }
        $('#output_name_error').hide();
        out_node.label = label;
        canvasState.selection = [ out_node ];
        canvasState.valid = false;
        dialog.hide();
    }).on('cancel', function() {// cancel is not a native event and can only be triggered via javascript
        $(this).closest('#dialog_form').hide();
        $('#output_name_error').hide();
        canvasState.connectors.pop();
        canvasState.valid = false;
    });
    
    $('#id_select_cdt, #id_select_method').on('change', function(e) {
        // Update preview picture of node to show a CDtNode or RawNode appropriately
        var preview_canvas = $(this).closest('.modal_dialog').find('canvas'),
            val = this.value,
            ctx, filename, colour;
        
        if (preview_canvas.length) {
            preview_canvas = preview_canvas[0];
            ctx = preview_canvas.getContext('2d');
            if (this.id == 'id_select_cdt') {
                ctx.clearRect(0, 0, preview_canvas.width, preview_canvas.height);
                if (val === '') {
                    (new RawNode(preview_canvas.width/2, preview_canvas.height/2)).draw(ctx);
                } else {
                    (new CDtNode(val, preview_canvas.width/2, preview_canvas.height/2)).draw(ctx);
                }
            } else if (this.id == 'id_select_method') {
                filename = $(this).find('option:selected')[0].title;
                colour = $(this).closest('.modal_dialog').find('#id_select_colour').val();
                $('#id_method_name').val_(filename);
                
                // use AJAX to retrieve Revision inputs and outputs
                $.ajax({
                    type: "POST",
                    url: "/get_method_io/",
                    data: { mid: val }, // specify data as an object
                    datatype: "json",
                    success: function(result) {
                        ctx.clearRect(0, 0, preview_canvas.width, preview_canvas.height);
                        var n_outputs = Object.keys(result.outputs).length,
                            n_inputs  = Object.keys(result.inputs).length;
                        
                        preview_canvas.height = (n_outputs + n_inputs) * 4 + 62;
                        (new MethodNode(
                            val,
                            null,//family
                            // Ensures node is centred perfectly on the preview canvas
                            // Makes assumptions that parameters like magnet radius and scoop length are default.
                            preview_canvas.width/2 - Math.sqrt(3) * ( Math.min(- ( n_inputs * 8 + 14 ), 42 - n_outputs * 8) + Math.max( n_inputs  * 8 + 14, n_outputs * 8 + 48 ) ) /4,// x
                            n_inputs * 4 + 27,// y
                            colour, 
                            null,//label
                            result.inputs,
                            result.outputs
                        )).draw(ctx);
                    }
                });
            }
        }
        e.stopPropagation();
    });
    
    // Handle 'Inputs' menu
    $('form','#id_input_ctrl').on('submit', function(e) {
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
            if ((shape instanceof RawNode || shape instanceof CDtNode) && shape.label === node_label) {
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
                shape = new RawNode(         pos.left, pos.top, node_label);
            } else {
                shape = new CDtNode(this_pk, pos.left, pos.top, node_label);
            }
            canvasState.addShape(shape);
            canvasState.detectCollisions(shape, 0);// Second arg: Upon collision, move new shape 0% and move existing objects 100%
            $('#id_datatype_name').val('');  // reset text field
            dlg.removeClass('modal_dialog').hide();
        }
    });

    // Handle 'Methods' menu
    $('form', '#id_method_ctrl').on('submit', function(e) {
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
        
        if (dlg.length) {
            pos = preview_canvas.offset();
            pos.left += preview_canvas[0].width/2  - canvas.offsetLeft;
            pos.top  += preview_canvas[0].height/2 - canvas.offsetTop;
        } else {
            pos = { left: 100, top: 200 + Math.round(50 * Math.random()) };
        }
        
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
                
                // use AJAX to retrieve Revision inputs and outputs
                $.ajax({
                    type: "POST",
                    url: "/get_method_io/",
                    data: { mid: mid }, // specify data as an object
                    datatype: "json", // type of data expected back from server
                    success: function(result) {
                        var inputs = result['inputs'],
                            outputs = result['outputs'];

                        if (document.getElementById('id_method_button').value == 'Add Method') {
                            // create new MethodNode
                            canvasState.addShape(new MethodNode(
                                mid, 
                                method_family.val(), 
                                pos.left, pos.top,
                                method_colour.val(), 
                                node_label,
                                inputs, outputs
                            ));
                        } else {
                            // replace the selected MethodNode
                            // if user clicks anywhere else, MethodNode is deselected
                            // and Methods menu closes

                            // draw new node over old node
                            var old_node = canvasState.selection[0],
                                new_node = new MethodNode(
                                    mid, 
                                    method_family.val(), 
                                    old_node.x, old_node.y,
                                    method_colour.val(), 
                                    node_label, 
                                    inputs, 
                                    outputs
                                ),
                                // check if we can re-use any Connectors
                                idx, new_xput, old_xput, connector;

                            for (idx in old_node.inputs) {
                                old_xput = old_node.inputs[idx];
                                if (inputs.hasOwnProperty(idx)) {
                                    new_xput = inputs[idx];
                                    if (new_xput.cdt_pk === old_xput.cdt_pk
                                            && old_node.in_magnets[idx-1].connected.length) {
                                        // re-attach Connector
                                        connector = old_node.in_magnets[idx-1].connected.pop();
                                        connector.dest = new_node.in_magnets[idx-1];
                                        new_node.in_magnets[idx-1].connected.push(connector);
                                    }
                                }
                            }

                            for (idx in old_node.outputs) if (old_node.outputs.propertyIsEnumerable(idx)) {
                                old_xput = old_node.outputs[idx];
                                if (outputs.hasOwnProperty(idx)) {
                                    new_xput = outputs[idx];
                                    if (new_xput.cdt_pk === old_xput.cdt_pk) {
                                        // re-attach all Connectors - note this does not reverse order any longer
                                        for (var i = 0; i < old_node.out_magnets[idx-1].connected.length; i++) {
                                            while (old_node.out_magnets[idx-1].connected.length) {
                                                connector = old_node.out_magnets[idx-1].connected.pop();
                                                connector.source = new_node.out_magnets[idx-1];
                                                new_node.out_magnets[idx-1].connected.unshift(connector);
                                            }
                                        }
                                    }
                                }
                            }

                            canvasState.deleteObject();  // delete selected (old Method)
                            canvasState.addShape(new_node);
                            canvasState.selection = [ new_node ];
                        }
                        
                        dlg.removeClass('modal_dialog').hide();
                    }
                });

                method_name.val_('');
            }
        }
    }).on('reset', function() {
        var method_family = $('#id_select_method_family', this);
        $('#id_method_name', this).val_('');
        method_family.val(method_family.children('option').eq(0)).change();
    });
    
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
    
    $('.form-inline-opts').on('click', 'input', function() {
        var $this = $(this),
            val = $this.val(),
            val_map = { always: true, never: false, ambiguous: undefined };

        if ($this.is(':checked') && val_map.hasOwnProperty(val)) {
            canvasState.force_show_exec_order = val_map[val];
            canvasState.valid = false;
        }
    });
    
    $(document).on('keydown', function(e) {
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
        }
        
        // escape key closes menus
        else if (e.which == 27) {
            $('li', 'ul#id_ctrl_nav').removeClass('clicked');
            $('.ctrl_menu:visible').trigger('cancel');
            
            canvasState.selection = [];
            canvasState.valid = false;
        }
    }).on('mousedown', function(e) {
        var menus = $('.ctrl_menu, .context_menu, .modal_dialog').filter(':visible');
        if ($(e.target).closest(menus).length === 0) {
            menus.trigger('cancel');
            $('li', 'ul#id_ctrl_nav').add(menus).removeClass('clicked');
        }
    }).on('cancel', '.context_menu, .modal_dialog, .ctrl_menu', function() {
        $(this).hide();
    });
    
    // when a context menu option is clicked
    $('.context_menu').on('click', 'li', function(e) {
        var $this = $(this),
            sel = canvasState.selection;
        
        // if there's a current node selected on the canvas
        if (sel) {
            var action = $this.data('action');
            
            if (action == 'edit' && sel.length == 1) {
                sel = sel[0];
                
                if (sel.constructor == MethodNode) {
                    /*
                        Open the edit dialog (rename, method selection, colour picker...)
                    */
                    var menu = $('#id_method_ctrl').show().addClass('modal_dialog'),
                        preview_canvas = $('canvas', menu)[0];
                    preview_canvas.width = menu.innerWidth();
                    menu.css({
                        top:  sel.y + sel.dy - sel.n_inputs * 4 + canvas.offsetTop - 36,
                        left: sel.x + sel.dx - (
                            preview_canvas.width/2 - Math.sqrt(3) * ( Math.min(- ( sel.n_inputs * 8 + 14 ), 42 - sel.n_outputs * 8) + Math.max( sel.n_inputs  * 8 + 14, sel.n_outputs * 8 + 48 ) ) /4
                        ) + canvas.offsetLeft - 9
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
                else if (sel.constructor == OutputNode) {
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
                if(sel instanceof OutputNode)
                    window.location = '/dataset_view/' + sel.dataset_id + '?rtp_id=' + sel.rtp_id;
            }
            if (action == 'download') {
                sel = sel[0];
                if(sel instanceof OutputNode)
                    window.location = '/dataset_download/' + sel.dataset_id;
            }
            if (action == 'viewlog') {
                sel = sel[0];
                if(sel instanceof MethodNode)
                    window.location = '/stdout_view/' + sel.log_id + '?rtp_id=' + sel.rtp_id;
            }
            if (action == 'viewerrorlog') {
                sel = sel[0];
                if(sel instanceof MethodNode)
                    window.location = '/stderr_view/' + sel.log_id + '?rtp_id=' + sel.rtp_id;
            }
        }
        $('.context_menu').hide();
        e.stopPropagation();
    });
    
    $('#colour_picker_pick').on('click', function() {
        var pos = $(this).position();
        $('#colour_picker_menu').css({ top: pos.top + 20, left: pos.left }).show();
    });
    
    $('#colour_picker_menu').on('click', 'div', function() {
        var bg_col = $(this).css('background-color');
        $('#colour_picker_pick').css('background-color', bg_col);
        $('#id_select_colour').val(bg_col);
        $('#colour_picker_menu').hide();
        $('#id_select_method').trigger('change');
    });
    
    /*
        Submit form
    */
    $('#id_pipeline_form').submit(function(e) {
        /*
        Trigger AJAX transaction on submitting form.
         */

        e.preventDefault(); // override form submit action

        // Since a field contains its label on pageload, a field's label as its value is treated as blank
        $('input, textarea', this).each(function() {
            if (this.value == $(this).data('label'))
                this.value = '';
        });
        
        var shapes = canvasState.shapes;

        // check graph integrity
        var this_shape,
            magnets,
            this_magnet,
            i, j,
            pipeline_inputs = [],  // collect data nodes
            pipeline_outputs = [],
            method_nodes = [],
            num_connections;

        document.getElementById('id_submit_error').innerHTML = '';

        for (i = 0; i < shapes.length; i++) {
            this_shape = shapes[i];
            if (this_shape instanceof MethodNode) {
                method_nodes.push(this_shape);

                // at least one out-magnet must be occupied
                magnets = this_shape.out_magnets;
                num_connections = 0;
                for (j = 0; j < magnets.length; j++) {
                    this_magnet = magnets[j];
                    num_connections += this_magnet.connected.length;
                }
                if (num_connections === 0) {
                    submitError('MethodNode with unused outputs');
                    return;
                }
            }
            else if (this_shape instanceof OutputNode) {
                pipeline_outputs.push(this_shape);

                // no need to check for connected magnets - all output nodes have
                // exactly 1 magnet with exactly 1 cable.
            }
            else {
                // this is a data node
                pipeline_inputs.push(this_shape);

                // all CDtNodes or RawNodes (inputs) should feed into a MethodNode
                magnets = this_shape.out_magnets;
                this_magnet = magnets[0];  // data nodes only ever have one magnet

                // is this magnet connected?
                if (this_magnet.connected.length === 0) {
                    // unconnected input in graph, exit
                    submitError('Unconnected input node');
                    return;
                }
            }
        }

        // sort pipelines by their isometric position, left-to-right, top-to-bottom
        // (sort of like reading order if you tilt your screen 30° clockwise)
        pipeline_inputs.sort(Geometry.isometricSort);
        pipeline_outputs.sort(Geometry.isometricSort);

        // at least one Connector must terminate as pipeline output
        if (pipeline_outputs.length === 0) {
            submitError('Pipeline has no output');
            return;
        }

        var is_revision = 0 < $('#id_pipeline_select').length;

        // arguments to initialize new Pipeline Family
        var family_name = $('#id_family_name').val(),  // hidden input if revision
            family_desc = $('#id_family_desc').val(),
            revision_name = $('#id_revision_name').val(),
            revision_desc = $('#id_revision_desc').val(),
            users_allowed = $("#id_users_allowed").val(),
            groups_allowed = $("#id_groups_allowed").val();


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

        // FIXME: This is fragile if we add a menu to either page
        var meta_menu_index = !is_revision;
        $('#id_revision_name, #id_revision_desc').css('background-color', '#fff');
        
        // Now we're ready to start
        // @note: shorthand object notation { val } is shorthand for { "val": val }
        var form_data = {
            users_allowed: users_allowed,
            groups_allowed: groups_allowed,
            // There is no PipelineFamily yet; we're going to create one.
            family_pk: null,
            family_name: family_name,
            family_desc: family_desc,
            // arguments to add first pipeline revision
            revision_name: revision_name,
            revision_desc: revision_desc,
            // Canvas information to store in the Pipeline object.
            canvas_width: canvas.width,
            canvas_height: canvas.height,
            revision_parent_pk: is_revision ? $('#id_pipeline_select').val() : null,
            // Arrays will be populated in the following code.
            pipeline_steps: [],
            pipeline_inputs: [],
            pipeline_outputs: []
        };

        // update form data with inputs
        var this_input;
        form_data.pipeline_inputs = [];
        for (i = 0; i < pipeline_inputs.length; i++) {
            this_input = pipeline_inputs[i];
            form_data.pipeline_inputs[i] = {
                CDT_pk: (this_input instanceof CDtNode) ? this_input.pk : null,
                dataset_name: this_input.label,
                dataset_idx: i + 1,
                x: this_input.x / canvas.width,
                y: this_input.y / canvas.height,
                min_row: null, // in the future these can be more detailed
                max_row: null
            }
        }

        // MethodNodes are now sorted live, prior to pipeline submission —JN
        var sorted_elements = [];
        for (i = 0; i < canvasState.exec_order.length; i++) {
            sorted_elements = sorted_elements.concat(canvasState.exec_order[i]);
        }

        // add arguments for input cabling
        var this_step, this_source;

        for (i = 0; i < sorted_elements.length; i++) {
            this_step = sorted_elements[i];

            form_data.pipeline_steps[i] = {
                transf_pk: this_step.pk,  // to retrieve Method
                transf_type: "Method", // in the future we can make this take Pipelines as well
                step_num: i+1,  // 1-index (pipeline inputs are index 0)
                x: this_step.x / canvas.width,
                y: this_step.y / canvas.height,
                name: this_step.label,
                cables_in: [],
                outputs_to_delete: [] // not yet implemented
            };

            // retrieve Connectors
            magnets = this_step.in_magnets;

            for (j = 0; j < magnets.length; j++) {
                this_magnet = magnets[j];
                if (this_magnet.connected.length === 0) {
                    continue;
                }
                this_connector = this_magnet.connected[0];
                this_source = this_connector.source.parent;

                form_data.pipeline_steps[i].cables_in[j] = {
                    //'source_type': this_source.constructor === RawNode ? 'raw' : 'CDT',
                    //'source_pk': this_source.constructor === RawNode ? '' : this_source.pk,
                    source_dataset_name: this_connector.source.label,
                    dest_dataset_name: this_connector.dest.label,
                    source_step: this_source instanceof MethodNode ? sorted_elements.indexOf(this_source)+1 : 0,
                    keep_output: false, // in the future this can be more flexible
                    wires: [] // no wires for a raw cable
                };
            }
        }

        var this_output;
        for (i = 0; i < pipeline_outputs.length; i++) {
            this_output = pipeline_outputs[i];
            this_connector = this_output.in_magnets[0].connected[0];
            var this_source_step = this_connector.source.parent;
            
            form_data.pipeline_outputs[i] = {
                output_name: this_output.label,
                output_idx: i+1,
                output_CDT_pk: this_connector.source.cdt,
                source: this_source_step.pk,
                source_step: sorted_elements.indexOf(this_source_step) + 1, // 1-index
                source_dataset_name: this_connector.source.label,  // magnet label
                x: this_output.x / canvas.width,
                y: this_output.y / canvas.height,
                wires: [] // in the future we might have this
            };
        }

        // this code written on Signal Hill, St. John's, Newfoundland
        // May 2, 2014 - afyp

        // this code modified at my desk
        // June 18, 2014 -- RL
        
        // I code at my desk too.
        // July 30, 2014 - JN

        // How did I even computer?
        // April 28, 2015 - Cat

        // do AJAX transaction
        $.ajax({
            type: 'POST',
            url: submit_to_url,
            data: JSON.stringify(form_data),
            datatype: 'json',
            success: function(result) {
                if (result['status'] == 'failure') {
                    submitError(result['error_msg']);
                }
                else if (result['status'] == 'success') {
                    $('#id_submit_error').html('').hide();
                    window.location.href = '/pipelines';
                }
            }
        })
    })
    
    $(window).resize(function(e) {
        var shape, i, scale_x, scale_y, out_z;
        canvasWidth  = canvasState.width = canvas.width  = window.innerWidth,
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
        out_z.x = canvasWidth * .8;
        out_z.h = out_z.w = canvasWidth * .15;
        while (out_z.h + out_z.y > canvasHeight) {
            out_z.h /= 1.5;
        }
        
        canvasState.old_width = canvas.width;
        canvasState.old_height = canvas.height;
        canvasState.valid = false;
    });
});// end of document.ready()
