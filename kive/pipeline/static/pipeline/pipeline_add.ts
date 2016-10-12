"use strict";

import { CanvasState } from "./drydock";
import { Pipeline } from "./pipeline_load";
import { PipelineReviser } from "./pipeline_revise";
import { Dialog, InputDialog, MethodDialog, OutputDialog, ViewDialog } from "./pipeline_dialogs";
import 'jquery';
declare var noXSS:Function;

// console.log($);

$.fn.extend({
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

            if (typeof opt.start == 'function') {
                opt.start(this);
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
            if (typeof opt.stop == 'function') {
                opt.stop(this);
            }
        });

        return $el;
    }
});

export var canvasState;
$(function() {
    var canvas;
    noXSS();

    /*
    @todo
    
    - move as much html into html template as possible
    - greater and greater modularization
    
    */
    
    // initialize animated canvas
    let REDRAW_INTERVAL = 50; // ms
    canvas = document.getElementById('pipeline_canvas');
    canvas.width  = window.innerWidth;
    canvas.height = window.innerHeight - $(canvas).offset().top - 5;
    canvasState = new CanvasState(canvas, REDRAW_INTERVAL);
    
    let $submit_btn = $('#id_submit_button');
    let parent_revision_id;
    
    var pipelineCheckReadiness = function() {
        let is = 'pipeline-not-ready';
        let isnt = 'pipeline-ready';
        if (canvasState.isComplete()) {// exec_order is a 2D array if pipeline is executable, otherwise false
            [is, isnt] = [isnt, is]; // swap variables
        }
        $submit_btn.addClass(is).removeClass(isnt);
    };

    function documentKeyHandler(e: JQueryKeyEventObject) {
        let backspace = e.which === 8;
        let del = e.which === 46;
        let esc = e.which === 27;
        // backspace or delete key also removes selected object
        if ((backspace || del) && !targetIsDialog(e)) {
            // prevent backspace from triggering browser to navigate back one page
            e.preventDefault();
            if (canvasState.selection) {
                canvasState.deleteObject();
            }
            pipelineCheckReadiness();
        }
        
        // escape key closes menus
        else if (e.which == 27) {
            for (let dialog of dialogs) {
                dialog.cancel();
            }
            canvasState.selection = [];
            canvasState.valid = false;
        }
    }
    
    function targetIsDialog(e: Event): boolean {
        for (let dialog of dialogs) {
            if ($(e.target).closest(dialog.jqueryRef).length !== 0) {
                return true;
            }
        }
        return false;
    }

    function documentClickHandler(e: JQueryMouseEventObject) {
        for (let dialog of dialogs) {
            if (!targetIsDialog(e)) {
                dialog.cancel();
            }
        }
    }

    var documentResizeHandler = (function() {
        var resize_timeout = 0;
        
        function endDocumentResize() {
            canvasState.valid = false;
            canvasState.outputZone.alignWithCanvas(canvas.width, canvas.height);
            canvasState.detectAllCollisions();
        }

        return function() {
            canvasState.width  = canvas.width  = window.innerWidth;
            canvasState.height = canvas.height = window.innerHeight - $(canvas).offset().top - 5;
            
            let scale_x = canvas.width  / canvasState.old_width;
            let scale_y = canvas.height / canvasState.old_height;
                
            if (scale_x === 1 && scale_y === 1) {
                return;
            }
            if (scale_x !== 1) {
                for (let shape of canvasState.shapes) {
                    shape.x  *= scale_x;
                    shape.dx *= scale_x;
                }
            }
            if (scale_y !== 1) {
                for (let shape of canvasState.shapes) {
                    shape.y  *= scale_y;
                    shape.dy *= scale_y;
                }
            }
            
            canvasState.old_width = canvas.width;
            canvasState.old_height = canvas.height;
            canvasState.valid = false;
            
            // Collision detection is computationally expensive, so
            // deferred until 0.5s have passed without further resizing.
            clearTimeout(resize_timeout);
            resize_timeout = setTimeout(endDocumentResize, 500);
        };
    })();

    var chooseContextMenuOption: (e: Event) => void = function (e) {
        e.stopPropagation();
        $('.context_menu').hide();
    
        var sel = canvasState.selection;
        var action = $(this).data('action');
    
        // if there's not a current node selected on the canvas
        if (!sel || sel.length === 0) {
            return;
        }
    
        if (action == 'delete') {
            canvasState.deleteObject();
            return;
        }
    
        // actions on one node only
        let sel0 = sel[0];
    
        switch (action) {
        
            case 'edit':
                if (CanvasState.isMethodNode(sel0) || CanvasState.isOutputNode(sel0)) {
                    // For methods, open the edit dialog (rename, method selection, colour picker...)
                    // For outputs, open the renaming dialog
                    var coords = canvasState.getAbsoluteCoordsOfNode(sel0);
                    if (CanvasState.isMethodNode(sel0)) {
                        let dialog = method_dialog;
                        dialog.show();
                        dialog.align(coords.x, coords.y);
                        dialog.load(sel0);
                    } else {
                        let dialog = output_dialog;
                        dialog.show();
                        dialog.align(coords.x, coords.y);
                        dialog.load(sel0);
                    }
                }
                break;
            case 'display':
                if (CanvasState.isNode(sel0) && !CanvasState.isMethodNode(sel0)) {
                    window.location.href = '/dataset_view/' + sel0.dataset_id + '?run_id=' + sel0.run_id + "&view_run";
                }
                break;
            case 'download':
                if (CanvasState.isNode(sel0) && !CanvasState.isMethodNode(sel0)) {
                    window.location.href = '/dataset_download/' + sel0.dataset_id + "&view_run";
                }
                break;
            case 'viewlog':
                if (CanvasState.isMethodNode(sel0)) {
                    window.location.href = '/stdout_view/' + sel0.log_id + '?run_id=' + sel0.run_id + "&view_run";
                }
                break;
            case 'viewerrorlog':
                if (CanvasState.isMethodNode(sel0)) {
                    window.location.href = '/stderr_view/' + sel0.log_id + '?run_id=' + sel0.run_id + "&view_run";
                }
                break;
        
        }
    };

    var submitPipeline = (function() {
        var $submit_error = $('#id_submit_error');
        return function(e) {
            /*
            Trigger AJAX transaction on submitting form.
             */

            e.preventDefault(); // override form submit action

            $submit_error.empty();

            var action = $('#id_pipeline_action').val();

            var is_new = action == "new",
                is_revision = action == "revise",
                // arguments to initialize new Pipeline Family
                family = {
                    name: $('#id_family_name'),  // hidden input if revision
                    desc: $('#id_family_desc')
                },
                revision = {
                    name: $('#id_revision_name'),
                    desc: $('#id_revision_desc')
                },
                users_allowed  = $("#id_permissions_0").find("option:selected").get()
                    .map(el => this.textContent),
                groups_allowed = $("#id_permissions_1").find("option:selected").get()
                    .map(el => this.textContent);
            
            // Form validation
            if (is_new) {
                if (family.name.val() === '') {
                    pipeline_family_dialog.show();
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
                users_allowed,
                groups_allowed,

                // There is no PipelineFamily yet; we're going to create one.
                family: family.name.val(),
                family_desc: family.desc.val(),

                // arguments to add first pipeline revision
                revision_name: revision.name.val(),
                revision_desc: revision.desc.val(),
                revision_parent: is_revision ? parent_revision_id : null,
                published: $('#published').prop('checked'),

                // Canvas information to store in the Pipeline object.
                canvas_width: canvas.width,
                canvas_height: canvas.height
            };

            try {
                // window.pipeline_revision = window.pipeline_revision || new Pipeline(canvasState);
                let pipeline_revision = new Pipeline(canvasState);

                // TODO: data cleaning should either be within pipeline_revision.serialize 
                // or within this context but probably not both.
                form_data = pipeline_revision.serialize(form_data);
            } catch(error) {
                submitError(error);
                return;
            }

            // console.log(form_data);
            // return;

            if(!is_new) {
                submitPipelineAjax($('#id_family_pk').val(), form_data);
            } else { // Pushing a new family
                submitPipelineFamilyAjax(
                    {
                        users_allowed,
                        groups_allowed,
                        name: family.name.val(),
                        description: family.desc.val()
                    },
                    form_data
                );
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
                
                for (let i = 0; i < value.length; i++) {
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
            $.ajax({
                type: "POST",
                url: '/api/pipelines/',
                data: JSON.stringify(form_data),
                contentType: "application/json"// data will not be parsed correctly without this
            }).done(function() {
                // $('#id_submit_error').empty().hide();
                $(window).off('beforeunload');
                window.location.href = '/pipelines/' + family_pk;
            }).fail(function(xhr, status, error) {
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
        function submitPipelineFamilyAjax(family_form_data, pipeline_form_data) {
            $.ajax({
                type: "POST",
                url: '/api/pipelinefamilies/',
                data: JSON.stringify(family_form_data),
                contentType: "application/json"// data will not be parsed correctly without this
            }).done(function(result) {
                submitPipelineAjax(result.id, pipeline_form_data);
            }).fail(function(xhr, status, error) {
                var json = xhr.responseJSON,
                    serverErrors = json && json.non_field_errors || [];
                
                if (serverErrors.length === 0) {
                    serverErrors = xhr.status + " - " + error;
                }
                submitError(serverErrors);
            });
        }
    })();

    function checkForUnsavedChanges() {
        if (canvasState.can_edit && canvasState.has_unsaved_changes) {
            return 'You have unsaved changes.';
        }
    }
    
    pipelineCheckReadiness();

    let initialData = $("#initial_data").text();
    if (initialData.length) {
        let loader = new PipelineReviser(initialData);
        loader.load(canvasState);
        loader.setUpdateCtrl($('#id_update'));
        loader.setRevertCtrl($('#id_revert'));
        parent_revision_id = JSON.parse(initialData).id;
    }
    
    // de-activate double-click selection of text on page
    canvas.addEventListener('selectstart', function(e) { e.preventDefault(); return false; }, false);
    canvas.addEventListener('mousedown',   function(e) { canvasState.doDown(e); }, true);
    canvas.addEventListener('mousemove',   function(e) { canvasState.doMove(e); }, true);
    canvas.addEventListener('mouseup',     function(e) { canvasState.doUp(e); pipelineCheckReadiness(); }, true);
    canvas.addEventListener('contextmenu', function(e) { canvasState.contextMenu(e); }, true);

    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        $(this).wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
    });
    
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

    var $ctrl_nav = $("#id_ctrl_nav");
    var pipeline_family_dialog = new Dialog( $('#id_family_ctrl'), $ctrl_nav.find("li[data-rel='#id_family_ctrl']") );
    var pipeline_dialog =        new Dialog( $('#id_meta_ctrl'),   $ctrl_nav.find("li[data-rel='#id_meta_ctrl']")   );
    var input_dialog    =   new InputDialog( $('#id_input_ctrl'),  $ctrl_nav.find("li[data-rel='#id_input_ctrl']")  );
    var method_dialog   =  new MethodDialog( $('#id_method_ctrl'), $ctrl_nav.find("li[data-rel='#id_method_ctrl']") );
    var output_dialog   =  new OutputDialog( $('#id_output_ctrl'), $ctrl_nav.find("li[data-rel='#id_output_ctrl']") );
    var view_dialog     =    new ViewDialog( $('#id_view_ctrl'),   $ctrl_nav.find("li[data-rel='#id_view_ctrl']")   );
    var dialogs = [ pipeline_family_dialog, pipeline_dialog, input_dialog, method_dialog, output_dialog, view_dialog ];

    // Handle jQuery-UI Dialog spawned for output cable
    $('form', '#id_output_ctrl') .submit( function(e) { e.preventDefault(); output_dialog.submit(canvasState); } );
    $('form', '#id_input_ctrl')  .submit( function(e) { e.preventDefault();  input_dialog.submit(canvasState); } );
    $('form', '#id_method_ctrl') .submit( function(e) { e.preventDefault(); method_dialog.submit(canvasState); } );
    
    /*
    EVENT BINDINGS TABLE
    ------------------------------------------------------------------------------------
     ELEMENT                         EVENT       (SELECTOR)  FUNCTION CALLBACK
    ------------------------------------------------------------------------------------
    */
    $(window)  .resize(documentResizeHandler)
               .on('beforeunload',         checkForUnsavedChanges);
    $(document).keydown(documentKeyHandler)
               .mousedown(documentClickHandler)
               // this one is set separately so that it can be disabled separately
               .on('cancel', '.ctrl_menu', function() { $(this).hide(); })
               // do not combine this line with the previous
               .on('cancel', '.context_menu, .modal_dialog', function() { $(this).hide(); });
    $('#id_pipeline_form').submit(submitPipeline);
    $('.context_menu').on('click', 'li', chooseContextMenuOption);    // when a context menu option is clicked
    $('#autolayout_btn').click(() => canvasState.autoLayout() );
    $('.align-btn').click( function() { canvasState.alignSelection($(this).data('axis')); });
    $('.form-inline-opts').on('click', 'input', () => view_dialog.changeExecOrderDisplayOption(canvasState) );
    /*
    ------------------------------------------------------------------------------------
    */

});// end of document.ready()
