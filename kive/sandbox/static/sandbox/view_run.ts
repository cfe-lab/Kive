"user strict";

import { canvasState } from '/static/pipeline/pipeline_dashboard';
import { Pipeline } from "/static/pipeline/pipeline_load";
import 'jquery';

var run_id = parseInt($('#run_id').val(), 10),
    pipeline_id = parseInt($('#run_pipeline_id').val(), 10),
    md5 = $('#run_md5').val(),
    timer,
    timerInterval = 1000,
    pipeline = new Pipeline(canvasState);

// Methods
var $msg = $('#run_status');
var status_message = {
    set: function(msg: string, use_link: boolean = false, use_anim: boolean = false) {
        $msg.empty();
        if (use_link) {
            $('<a>')
                .attr('href', '/view_results/' + run_id + '?back_to_view=true')
                .text(msg)
                .appendTo($msg)
            ;
        } else {
            $msg.text(msg);
        }
        if (use_anim) {
            $msg.prepend('<img src="/static/sandbox/preload.gif"> &nbsp;');
        }
    }
};

function grabStatus() {
    if (timer === undefined) {
        timer = setInterval(grabStatus, timerInterval);
    }

    // Poll the server
    $.getJSON("/api/runs/" + run_id + "/run_status/", {}, function(run) {
        var status: string = run.status;
        status_message.set('Complete', true);

        // TODO: Use a better system of reporting overall run status
        if (status.indexOf('?') >= 0) {
            status_message.set('Waiting for run to start', false, true);
        } else if (status.indexOf('Too') >= 0) {
            status_message.set(status);
            clearInterval(timer);
        } else {
            if (status.indexOf('!') >= 0) {
                status_message.set('Failed', true);
                clearInterval(timer);
            } else if (status.match(/[\+\.:]/)) {
                status_message.set('In progress', true, true);
            } else {
                clearInterval(timer);
            }
            pipeline.update(run, md5, run_id);
        }
    });
}

$.get("/api/pipelines/" + pipeline_id + "/")
    .done(function(pipeline_raw) {
        pipeline.load(pipeline_raw);
        pipeline.draw();
        grabStatus();
    }
);

$(document).off('cancel', '.ctrl_menu');