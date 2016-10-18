"use strict";

import { CanvasState } from "./drydock";
import { CNode } from "./drydock_objects";
import { PipelineReviser } from "./pipeline_revise";
import { Dialog } from "./pipeline_dialogs";
import 'jquery';
declare var noXSS:Function;

export var canvasState;
export var dialogs: Dialog[] = [];
export var parent_revision_id;

noXSS();

/**
 * @todo
 * greater and greater modularization
 */

// initialize animated canvas
let REDRAW_INTERVAL = 50; // ms
let canvas = document.getElementById('pipeline_canvas');
canvas.width  = window.innerWidth;
canvas.height = window.innerHeight - $(canvas).offset().top - 5;
canvasState = new CanvasState(canvas, REDRAW_INTERVAL);

let $contextMenu = $('.context_menu');

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
    }
    
    // escape key closes menus
    else if (esc) {
        for (let dialog of dialogs) {
            dialog.cancel();
        }
        $contextMenu.trigger('cancel');
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
    return $(e.target).closest($contextMenu).length !== 0;
}

function documentClickHandler(e: JQueryMouseEventObject) {
    for (let dialog of dialogs) {
        if (!targetIsDialog(e)) {
            dialog.cancel();
        }
    }
    $contextMenu.trigger('cancel');
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

$(window).resize(documentResizeHandler);

interface ContextMenuInterface {
    [action: string]: (sel: CNode|CNode[]) => void;
}
export var contextMenuActions: ContextMenuInterface = {
    display: function(sel) {
        if (CanvasState.isNode(sel) && !CanvasState.isMethodNode(sel)) {
            window.location.href = '/dataset_view/' + sel.dataset_id + '?run_id=' + sel.run_id + "&view_run";
        }
    },
    download: function(sel) {
        if (CanvasState.isNode(sel) && !CanvasState.isMethodNode(sel)) {
            window.location.href = '/dataset_download/' + sel.dataset_id + "&view_run";
        }
    },
    viewlog: function(sel) {
        if (CanvasState.isMethodNode(sel)) {
            window.location.href = '/stdout_view/' + sel.log_id + '?run_id=' + sel.run_id + "&view_run";
        }
    },
    viewerrorlog: function(sel) {
        if (CanvasState.isMethodNode(sel)) {
            window.location.href = '/stderr_view/' + sel.log_id + '?run_id=' + sel.run_id + "&view_run";
        }
    }
};
$contextMenu.on({
    // necessary to stop document click handler from kicking in
    mousedown: (e: JQueryMouseEventObject) => e.stopPropagation(),
    click: function (e: JQueryMouseEventObject) {
        // when a context menu option is clicked
        e.stopPropagation();
        $contextMenu.hide();

        var sel = canvasState.selection;
        var action = $(this).data('action');

        // if there's not a current node selected on the canvas
        if (!sel || sel.length === 0) {
            return;
        }
        // 'delete' is the only action that allows >1 node
        if (action !== 'delete') {
            sel = sel[0];
        }
        if (contextMenuActions.hasOwnProperty(action)) {
            contextMenuActions[action](sel);
        }
    }
}, 'li');

let initialData = $("#initial_data").text();
if (initialData.length) {
    let loader = new PipelineReviser(initialData);
    loader.load(canvasState);
    loader.setUpdateCtrl($('#id_update'));
    loader.setRevertCtrl($('#id_revert'));
    parent_revision_id = JSON.parse(initialData).id;
}

// de-activate double-click selection of text on page
canvasState.initMouseListeners();
canvas.addEventListener('contextmenu', function(e) { canvasState.contextMenu(e); }, true);

$(document).keydown(documentKeyHandler)
    .mousedown(documentClickHandler)
    // this one is set separately so that it can be disabled separately
    .on('cancel', '.ctrl_menu', function() { $(this).hide(); })
    // do not combine this line with the previous
    .on('cancel', '.context_menu, .modal_dialog', function() { $(this).hide(); });

// Pack help text into an unobtrusive icon
$('.helptext', 'form').each(function() {
    $(this).wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
});

$('#autolayout_btn').click(
    () => canvasState.autoLayout()
);
$('.align-btn').click(function() {
    canvasState.alignSelection($(this).data('axis'));
});