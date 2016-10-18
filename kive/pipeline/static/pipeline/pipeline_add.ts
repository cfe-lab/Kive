"use strict";
import { CanvasState, CanvasContextMenu, CanvasListeners, Pipeline, PipelineReviser, REDRAW_INTERVAL } from "./pipeline_all";
import { ViewDialog, OutputDialog, MethodDialog, InputDialog, Dialog } from "./pipeline_dialogs";
import 'jquery';
import '/static/portal/noxss.js';

declare var noXSS: any;
noXSS();

// initialize animated canvas
let canvas = document.getElementById('pipeline_canvas') as HTMLCanvasElement;
canvas.width  = window.innerWidth;
canvas.height = window.innerHeight - $(canvas).offset().top - 5;

export var canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);
export var contextMenu = new CanvasContextMenu('.context_menu', canvasState);

// de-activate double-click selection of text on page
CanvasListeners.initMouseListeners(canvasState);
CanvasListeners.initContextMenuListener(canvasState, contextMenu);
CanvasListeners.initKeyListeners(canvasState);
CanvasListeners.initResizeListeners(canvasState);

let parent_revision_id;
let initialData = $("#initial_data");
if (initialData.length) {
    let text = initialData.text();
    if (text.length) {
        let loader = new PipelineReviser(text);
        loader.load(canvasState);
        loader.setUpdateCtrl($('#id_update'));
        loader.setRevertCtrl($('#id_revert'));
        parent_revision_id = loader.pipelineRaw.id;
    }
}

// Pack help text into an unobtrusive icon
$('.helptext', 'form').each(function() {
    $(this).wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
});



var submitPipeline = (function() {
    var $submit_error = $('#id_submit_error');
    return function(e) {
        /*
         Trigger AJAX transaction on submitting form.
         */
        
        e.preventDefault(); // override form submit action
        clearErrors();
    
        let action = $('#id_pipeline_action').val();
        let is_new = action == "new";
        
        let form_data;
        let pipeline;
        
        try {
            pipeline = new Pipeline(canvasState);
            pipeline.setMetadata(
                action,
                $('#id_family_name').val(),
                $('#id_family_desc').val(),
                $('#id_revision_name').val(),
                $('#id_revision_desc').val(),
                parent_revision_id,
                $('#published').prop('checked'),
                $("#id_permissions_0").find("option:selected").get().map(el => el.textContent),
                $("#id_permissions_1").find("option:selected").get().map(el => el.textContent)
            );
        } catch(e) {
            pipeline_family_dialog.show();
            $('#id_family_name').addClass('submit-error-missing').focus();
            submitError(e);
            return;
        }
        
        try {
            // TODO: data cleaning should either be within pipeline.serialize
            // or within this context but probably not both.
            form_data = pipeline.serialize();
        } catch(error) {
            submitError(error);
            return;
        }
        
        if (!is_new) {
            submitPipelineAjax($('#id_family_pk').val(), form_data);
        } else { // Pushing a new family
            submitPipelineFamilyAjax({
                users_allowed: form_data.users_allowed,
                groups_allowed: form_data.groups_allowed,
                name: form_data.family,
                description: form_data.family_desc
            }).done(function(result) {
                submitPipelineAjax(result.id, form_data);
            });
        }
        
    };// end exposed function - everything that follows is closed over
    
    function clearErrors() {
        $submit_error.empty();
        $('#id_family_name, #id_family_desc, #id_revision_name, #id_revision_desc').removeClass('submit-error-missing');
    }
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
        return $.ajax({
            type: "POST",
            url: '/api/pipelines/',
            data: JSON.stringify(form_data),
            contentType: "application/json" // data will not be parsed correctly without this
        }).done(function() {
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
    function submitPipelineFamilyAjax(family_form_data) {
        return $.ajax({
            type: "POST",
            url: '/api/pipelinefamilies/',
            data: JSON.stringify(family_form_data),
            contentType: "application/json" // data will not be parsed correctly without this
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
$('#id_pipeline_form').submit(submitPipeline);



/**
 * dialogs
 *
 */
var $ctrl_nav = $("#id_ctrl_nav");
var $add_menu = $('#id_add_ctrl');
var $view_menu = $('#id_view_ctrl');

var pipeline_family_dialog = new Dialog( $('#id_family_ctrl'), $ctrl_nav.find("li[data-rel='#id_family_ctrl']") );
                             new Dialog( $('#id_meta_ctrl'),   $ctrl_nav.find("li[data-rel='#id_meta_ctrl']")   );
                         new ViewDialog( $view_menu,           $ctrl_nav.find("li[data-rel='#id_view_ctrl']")   );

var add_menu        =        new Dialog( $add_menu,            $ctrl_nav.find("li[data-rel='#id_add_ctrl']")    );
var input_dialog    =   new InputDialog( $('#id_input_ctrl'),  $add_menu.find("li[data-rel='#id_input_ctrl']")  );
var method_dialog   =  new MethodDialog( $('#id_method_ctrl'), $add_menu.find("li[data-rel='#id_method_ctrl']") );
var output_dialog   =  new OutputDialog( $('#id_output_ctrl'), $add_menu.find("li[data-rel='#id_output_ctrl']") );

$add_menu.click('li', function() {
    add_menu.hide();
});

// Handle jQuery-UI Dialog spawned for output cable
$('form', '#id_output_ctrl') .submit( function(e) { e.preventDefault(); output_dialog.submit(canvasState); } );
$('form', '#id_input_ctrl')  .submit( function(e) { e.preventDefault();  input_dialog.submit(canvasState); } );
$('form', '#id_method_ctrl') .submit( function(e) { e.preventDefault(); method_dialog.submit(canvasState); } );

canvas.addEventListener("new_output", function(e: CustomEvent) {
    // spawn dialog for output label
    let node = e.detail.out_node;
    output_dialog.makeImmune(); // hack to get around the document.click event closing this right away
    output_dialog.show();
    output_dialog.align(node.x + node.dx, node.y + node.dy);
    output_dialog.load(node);
}, false);

$view_menu.find('.show-order-grp').on('click', '.show-order', function() {
    ViewDialog.changeExecOrderDisplayOption(canvasState, this.value);
});
$view_menu.find('.align-btn-grp').on('click', '.align-btn', function() {
    ViewDialog.alignCanvasSelection(canvasState, $(this).data('axis'));
});
$view_menu.find('#autolayout_btn').click(
    () => canvasState.autoLayout()
);

/******/



/**
 * context menu
 */
contextMenu.registerAction('delete', function(sel) { canvasState.deleteObject(); });
contextMenu.registerAction('edit', function(sel) {
    var coords = canvasState.getAbsoluteCoordsOfNode(sel);
    
    // For methods, open the edit dialog (rename, method selection, colour picker...)
    if (CanvasState.isMethodNode(sel)) {
        let dialog = method_dialog;
        dialog.show();
        dialog.align(coords.x, coords.y);
        dialog.load(sel);
    }

    // For outputs, open the renaming dialog
    else if (CanvasState.isOutputNode(sel)) {
        let dialog = output_dialog;
        dialog.show();
        dialog.align(coords.x, coords.y);
        dialog.load(sel);
    }
});


$(window).on('beforeunload', function checkForUnsavedChanges() {
    if (canvasState.can_edit && canvasState.has_unsaved_changes) {
        return 'You have unsaved changes.';
    }
});
let $submit_btn = $('#id_submit_button');
var pipelineCheckReadiness = function() {
    let is = 'pipeline-not-ready';
    let isnt = 'pipeline-ready';
    if (canvasState.isComplete()) {// exec_order is a 2D array if pipeline is executable, otherwise false
        [is, isnt] = [isnt, is]; // swap variables
    }
    $submit_btn.addClass(is).removeClass(isnt);
};
pipelineCheckReadiness();
document.addEventListener('keydown', pipelineCheckReadiness, false);
canvas.addEventListener('mouseup', pipelineCheckReadiness, false);


/* Silly happy face widget. */
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
