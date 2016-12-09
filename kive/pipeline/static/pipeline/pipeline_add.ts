"use strict";
import {
    CanvasState, CanvasContextMenu, CanvasListeners,
    Pipeline, buildPipelineSubmit, REDRAW_INTERVAL
} from "./pipeline_all";
import { ViewDialog, OutputDialog, MethodDialog, InputDialog, Dialog } from "./pipeline_dialogs";
import 'jquery';
import '/static/portal/noxss.js';

declare var noXSS: any;
noXSS();

/**
 * DIRECTORY:
 *
 * Part 1/8: Initialize CanvasState with event listeners.
 * Part 2/8: Load initial pipeline data if present.
 * Part 3/8: Prepare Pipeline completeness check.
 * Part 4/8: Initialize Dialogs (Family, revision, add, add input, add method, add output, and view)
 * Part 5/8: Initialize the submission of this page
 * Part 6/8: Initialize CanvasContextMenu and register actions
 * Part 7/8: Prompt the user when navigating away unsaved changes
 * Part 8/8: Silly happy face widget
 */

/**
 * @todo
 * New features:
 * - History tracking: Ctrl+Z & Ctrl+Shift+Z - medium
 * - Upload Methods from the Dialog - hard
 * - Add Methods using GUI - hard
 * - Create CDTs from the Dialog - hard
 * - Drag area to select nodes - medium
 */

/**
 * Part 1/8: Initialize CanvasState with event listeners.
 */
let canvas = document.getElementById('pipeline_canvas') as HTMLCanvasElement;
canvas.width  = window.innerWidth;
canvas.height = window.innerHeight - $(canvas).offset().top - 5;
let canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);
CanvasListeners.initMouseListeners(canvasState);
CanvasListeners.initKeyListeners(canvasState);
CanvasListeners.initResizeListeners(canvasState);

/**
 * Part 2/8: Load initial pipeline data if present.
 */
let parent_revision_id;
let text = $("#initial_data").text();
let loader = new Pipeline(canvasState);
if (text) {
    loader.setUpdateCtrl('#id_update');
    loader.setRevertCtrl('#id_revert');
    loader.loadFromString(text);
    parent_revision_id = loader.pipeline.id;
    loader.draw();
    $(canvas).hide().fadeIn();
}

/**
 * Part 3/8: Prepare Pipeline completeness check.
 */
interface BtnFunction extends Function { $btn?: JQuery; }
var pipelineCheckCompleteness: BtnFunction = function() {
    let $submit_btn = pipelineCheckCompleteness.$btn || $('#id_submit_button');
    pipelineCheckCompleteness.$btn = $submit_btn;
    let is = 'pipeline-not-ready';
    let isnt = 'pipeline-ready';
    // exec_order is a 2D array if pipeline is executable, otherwise false
    if (canvasState.isComplete()) {
        [is, isnt] = [isnt, is]; // swap variables
    }
    $submit_btn.addClass(is).removeClass(isnt);
};
canvas.addEventListener('CanvasStateChange', () => pipelineCheckCompleteness(), false);
pipelineCheckCompleteness();

/**
 * Part 4/8: Initialize Dialogs (Family, revision, add, add input, add method, add output, and view)
 */
var $ctrl_nav = $("#id_ctrl_nav");
var $add_menu = $('#id_add_ctrl');
var $view_menu = $('#id_view_ctrl');

var family_dialog =       new Dialog( $('#id_family_ctrl'), $ctrl_nav.find("li[data-rel='#id_family_ctrl']") );
/* anonymous */           new Dialog( $('#id_meta_ctrl'),   $ctrl_nav.find("li[data-rel='#id_meta_ctrl']")   );
/* anonymous */       new ViewDialog( $view_menu,           $ctrl_nav.find("li[data-rel='#id_view_ctrl']")   );
var add_menu      =       new Dialog( $add_menu,            $ctrl_nav.find("li[data-rel='#id_add_ctrl']")    );
var input_dialog  =  new InputDialog( $('#id_input_ctrl'),  $add_menu.find("li[data-rel='#id_input_ctrl']")  );
var method_dialog = new MethodDialog( $('#id_method_ctrl'), $add_menu.find("li[data-rel='#id_method_ctrl']") );
var output_dialog = new OutputDialog( $('#id_output_ctrl'), $add_menu.find("li[data-rel='#id_output_ctrl']") );

$add_menu.click('li', function() { add_menu.hide(); });

// Handle jQuery-UI Dialog spawned for output cable
$('form', '#id_output_ctrl') .submit( function(e) { e.preventDefault(); output_dialog.submit(canvasState); } );
$('form', '#id_input_ctrl')  .submit( function(e) { e.preventDefault();  input_dialog.submit(canvasState); } );
$('form', '#id_method_ctrl') .submit( function(e) { e.preventDefault(); method_dialog.submit(canvasState); } );

canvas.addEventListener("CanvasStateNewOutput", function(e: CustomEvent) {
    let d = e.detail;
    if (d.open_dialog && Array.isArray(d.added) && CanvasState.isOutputNode(d.added[0])) {
        // spawn dialog for output label
        let node = d.added[0];
        // hack to get around the document.click event closing this right away
        output_dialog.makeImmune();
        output_dialog.show();
        output_dialog.align(node.x + node.dx, node.y + node.dy);
        output_dialog.load(node);
    }
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

/**
 * Part 5/8: Initialize the submission of this page
 */
$('#id_pipeline_form').submit(buildPipelineSubmit(
    canvasState,
    $('#id_pipeline_action').val(),
    $('#id_family_name'),   $('#id_family_desc'),
    parseInt($('#id_family_pk').val(), 10),
    $('#id_revision_name'), $('#id_revision_desc'),
    parent_revision_id,
    $('#published'),
    $("#id_permissions_0"), $("#id_permissions_1"),
    $('#id_submit_error'),
    function() {
        family_dialog.show();
        $('#id_family_name').addClass('submit-error-missing').focus();
    }
));

/**
 * Part 6/8: Initialize context menu and register actions
 */
var contextMenu = new CanvasContextMenu('#context_menu', canvasState);
var nothingSelected = (multi, sel) => sel === undefined || sel === null || Array.isArray(sel) && sel.length === 0;
CanvasListeners.initContextMenuListener(canvasState, contextMenu);
contextMenu.registerAction('Delete', function(multi, sel) {
    return CanvasState.isNode(sel) ||
        Array.isArray(sel) &&
        sel.filter(CanvasState.isNode).length === sel.length;
}, function() {
    canvasState.deleteObject();
});
contextMenu.registerAction('Edit', function(multi, sel) {
    return !multi &&
        (
            CanvasState.isMethodNode(sel) ||
            CanvasState.isOutputNode(sel)
        );
}, function(sel) {
    let coords = canvasState.getAbsoluteCoordsOfNode(sel);

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
contextMenu.registerAction('Add Input', nothingSelected, function(_, pos) {
    input_dialog.show();
    input_dialog.align(pos.clientX, pos.clientY);
});
contextMenu.registerAction('Add Method', nothingSelected, function(_, pos) {
    method_dialog.show();
    method_dialog.align(pos.clientX, pos.clientY);
});
contextMenu.registerAction('Add Output', nothingSelected, function(_, pos) {
    output_dialog.show();
    output_dialog.align(pos.clientX, pos.clientY);
});
contextMenu.registerAction('Complete Inputs', function(multi, sel) {
    return !multi &&
        CanvasState.isMethodNode(sel) &&
        sel.in_magnets.filter(el => el.connected.length === 0).length > 0;
}, function(sel) {
    if (CanvasState.isMethodNode(sel)) {
        canvasState.completeMethodInputs(sel);
    }
});
contextMenu.registerAction('Complete Outputs', function(multi, sel) {
    return !multi &&
        CanvasState.isMethodNode(sel) &&
        sel.out_magnets.filter(el => el.connected.length === 0).length > 0;
}, function(sel) {
    if (CanvasState.isMethodNode(sel)) {
        canvasState.completeMethodOutputs(sel);
    }
});

/**
 * Part 7/8: Prompt the user when navigating away unsaved changes
 */
if (canvasState.can_edit) {
    $(window).on('beforeunload', function checkForUnsavedChanges() {
        if (canvasState.has_unsaved_changes) {
            return 'You have unsaved changes.';
        }
    });
}

/**
 * Part 8/8: Silly happy face widget
 */
let smileWidgetEvents = (function() {
    let indicator = $('#happy_indicator');
    let label = $('#happy_indicator_label');
    let indicatorAndLabel = indicator.add(label);
    const CHARS_PER_SMILE = 12;
    const NUM_EMOJIS = 10;
    const EMOJI_WIDTH = 16;

    let keydown = function() {
        let happy_mapped = -Math.min(
                NUM_EMOJIS - 1,
                Math.floor(this.value.length / CHARS_PER_SMILE)
            ) * EMOJI_WIDTH;
        indicator.css('background-position-x', happy_mapped + 'px');
    };
    let focus = function() {
        let chars_typed = this.value.length;
        if (chars_typed === 0 || $(this).hasClass('input-label')) {
            indicatorAndLabel.hide();
        } else {
            indicator.show();
            label[ chars_typed > 20 ? 'hide' : 'show' ]();
        }
    };
    let blur = function() {
        indicatorAndLabel.hide();
    };

    return { keydown, keyup: focus, focus, blur };
})();
$('#id_revision_desc').on(smileWidgetEvents)
    .keydown().blur();