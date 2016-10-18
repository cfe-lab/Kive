"use strict";

import { ViewDialog, OutputDialog, MethodDialog, InputDialog, Dialog } from "./pipeline_dialogs";
import { canvasState, dialogs, parent_revision_id, contextMenuActions } from "./pipeline_dashboard";
import { Pipeline } from "./pipeline_load";
import { CanvasState } from "./drydock";

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
            canvas_width: canvasState.width,
            canvas_height: canvasState.height
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

var $ctrl_nav = $("#id_ctrl_nav");
var pipeline_family_dialog = new Dialog( $('#id_family_ctrl'), $ctrl_nav.find("li[data-rel='#id_family_ctrl']") );
var pipeline_dialog =        new Dialog( $('#id_meta_ctrl'),   $ctrl_nav.find("li[data-rel='#id_meta_ctrl']")   );
var add_menu        =        new Dialog( $('#id_add_ctrl'),    $ctrl_nav.find("li[data-rel='#id_add_ctrl']")    );

add_menu.jqueryRef.click('li', function() {
    add_menu.hide();
});

var input_dialog    =   new InputDialog( $('#id_input_ctrl'),  $("li[data-rel='#id_input_ctrl']")  );
var method_dialog   =  new MethodDialog( $('#id_method_ctrl'), $("li[data-rel='#id_method_ctrl']") );
var output_dialog   =  new OutputDialog( $('#id_output_ctrl'), $("li[data-rel='#id_output_ctrl']") );
var view_dialog     =    new ViewDialog( $('#id_view_ctrl'),   $ctrl_nav.find("li[data-rel='#id_view_ctrl']")   );
dialogs.push(pipeline_family_dialog, pipeline_dialog, input_dialog, method_dialog, output_dialog, view_dialog, add_menu);

// Handle jQuery-UI Dialog spawned for output cable
$('form', '#id_output_ctrl') .submit( function(e) { e.preventDefault(); output_dialog.submit(canvasState); } );
$('form', '#id_input_ctrl')  .submit( function(e) { e.preventDefault();  input_dialog.submit(canvasState); } );
$('form', '#id_method_ctrl') .submit( function(e) { e.preventDefault(); method_dialog.submit(canvasState); } );


contextMenuActions['delete'] = function(sel) {
    canvasState.deleteObject();
};
contextMenuActions['edit'] = function(sel) {
    if (CanvasState.isMethodNode(sel) || CanvasState.isOutputNode(sel)) {
        // For methods, open the edit dialog (rename, method selection, colour picker...)
        // For outputs, open the renaming dialog
        var coords = canvasState.getAbsoluteCoordsOfNode(sel);
        if (CanvasState.isMethodNode(sel)) {
            let dialog = method_dialog;
            dialog.show();
            dialog.align(coords.x, coords.y);
            dialog.load(sel);
        } else {
            let dialog = output_dialog;
            dialog.show();
            dialog.align(coords.x, coords.y);
            dialog.load(sel);
        }
    }
};

function checkForUnsavedChanges() {
    if (canvasState.can_edit && canvasState.has_unsaved_changes) {
        return 'You have unsaved changes.';
    }
}

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
$(window).on('beforeunload', checkForUnsavedChanges);
$(document).keydown(pipelineCheckReadiness);
canvasState.canvas.addEventListener('mouseup', pipelineCheckReadiness, true);
$('#id_pipeline_form').submit(submitPipeline);
$('.form-inline-opts').on('click', 'input', () => view_dialog.changeExecOrderDisplayOption(canvasState) );

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
