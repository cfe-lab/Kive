"use strict";

import {
    CanvasState, CanvasContextMenu, CanvasListeners,
    Pipeline, REDRAW_INTERVAL
} from "static/pipeline/pipeline_all";
import 'jquery';

const RUN_ID         = parseInt($('#run_id').val(), 10);
const PIPELINE_ID    = parseInt($('#run_pipeline_id').val(), 10);
const MD5            = $('#run_md5').val();
const TIMER_INTERVAL = 1000;

// initialize animated canvas
let canvas = <HTMLCanvasElement> document.getElementById('pipeline_canvas');
canvas.width  = window.innerWidth;
canvas.height = window.innerHeight - $(canvas).offset().top - 5;

var canvasState = new CanvasState(canvas, false, REDRAW_INTERVAL);
var pipeline = new Pipeline(canvasState);
CanvasListeners.initMouseListeners(canvasState);
CanvasListeners.initKeyListeners(canvasState);
CanvasListeners.initResizeListeners(canvasState);

$.get("/api/pipelines/" + PIPELINE_ID + "/")
    .done(function(pipeline_raw) {
            pipeline.load(pipeline_raw);
            pipeline.draw();
            grabStatus();
        }
    );

var status_message = {
    $msg: $('#run_status'),
    set: function(msg: string, use_link: boolean = false, use_anim: boolean = false) {
        status_message.$msg.empty();
        if (use_link) {
            $('<a>')
                .attr('href', '/view_results/' + RUN_ID + '?back_to_view=true')
                .text(msg)
                .appendTo(status_message.$msg)
            ;
        } else {
            status_message.$msg.text(msg);
        }
        if (use_anim) {
            status_message.$msg.prepend('<img src="/static/sandbox/preload.gif"> &nbsp;');
        }
    }
};

interface TimerFunction extends Function { timer?: number; }
var grabStatus: TimerFunction = function() {
    if (grabStatus.timer === undefined) {
        grabStatus.timer = setInterval(grabStatus, TIMER_INTERVAL);
    }

    // Poll the server
    $.getJSON("/api/runs/" + RUN_ID + "/run_status/", {}, function(run) {
        var status: string = run.status;
        status_message.set('Complete', true);

        // TODO: Use a better system of reporting overall run status
        if (status.indexOf('?') >= 0) {
            status_message.set('Waiting for run to start', false, true);
        } else if (status.indexOf('Too') >= 0) {
            status_message.set(status);
            clearInterval(grabStatus.timer);
        } else {
            if (status.indexOf('!') >= 0) {
                status_message.set('Failed', true);
                clearInterval(grabStatus.timer);
            } else if (status.match(/[\+\.:]/)) {
                status_message.set('In progress', true, true);
            } else {
                clearInterval(grabStatus.timer);
            }
            pipeline.update(run, MD5, RUN_ID);
        }
    });
};

var contextMenu = new CanvasContextMenu('#context_menu', canvasState);
var hasDataset = (multi, sel) => !multi && CanvasState.isDataNode(sel)   && sel.dataset_id;
var hasLogs    = (multi, sel) => !multi && CanvasState.isMethodNode(sel) && sel.log_id;
CanvasListeners.initContextMenuListener(canvasState, contextMenu);
contextMenu.registerAction('View Dataset', hasDataset, function(sel) {
    if (CanvasState.isNode(sel) && !CanvasState.isMethodNode(sel)) {
        window.location.href = '/dataset_view/' + sel.dataset_id + '?run_id=' + sel.run_id + "&view_run";
    }
});
contextMenu.registerAction('Download Dataset', hasDataset, function(sel) {
    if (CanvasState.isNode(sel) && !CanvasState.isMethodNode(sel)) {
        window.location.href = '/dataset_download/' + sel.dataset_id + "?view_run";
    }
});
contextMenu.registerAction('View Log', hasLogs, function(sel) {
    if (CanvasState.isMethodNode(sel)) {
        window.location.href = '/stdout_view/' + sel.log_id + '?run_id=' + sel.run_id + "&view_run";
    }
});
contextMenu.registerAction('View Error Log', hasLogs, function(sel) {
    if (CanvasState.isMethodNode(sel)) {
        window.location.href = '/stderr_view/' + sel.log_id + '?run_id=' + sel.run_id + "&view_run";
    }
});