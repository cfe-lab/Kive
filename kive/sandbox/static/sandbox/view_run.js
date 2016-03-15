/*
 *
 * Includes:
 * /static/pipeline/drydock_objects.js
 * /static/pipeline/drydock.js
 * /static/pipeline/pipeline_add.js
 * /static/pipeline/pipeline_load.js
 */


$(function() {
    var self = this,
        run_id = parseInt($('#run_id').val(), 10),
        pipeline_id = parseInt($('#run_pipeline_id').val(), 10),
        md5 = $('#run_md5').val(),
        timer,
        timerInterval = 1000,
        pipeline = new Pipeline(canvasState),
        $msg = $('#run_status');
    ;

    // Methods
    $msg.set = function(msg, use_link, use_anim) {
        this.empty();
        if (use_link) {
            $('<a>')
                .attr('href', '/view_results/' + run_id + '?back_to_view=true')
                .text(msg)
                .appendTo(this)
            ;
        } else {
            this.text(msg);
        }
        if (use_anim) {
            this.prepend('<img src="/static/sandbox/preload.gif"> &nbsp;');
        }
    }

    function grabStatus() {
        if (timer === undefined) {
            timer = setInterval(grabStatus, timerInterval);
        }

        // Poll the server
        $.getJSON("/api/runs/" + run_id + "/run_status/", {}, function(run) {
            var stat = run.status;
            $msg.set('Complete', true);

            // TODO: Use a better system of reporting overall run status
            if (stat.indexOf('?') >= 0) {
                $msg.set('Waiting for run to start', false, true);
            } else if (stat.indexOf('Too') >= 0) {
                $msg.set(stat);
                clearInterval(timer);
            } else {
                if (stat.indexOf('!') >= 0) {
                    $msg.set('Failed', true);
                    clearInterval(timer);
                } else if (stat.match(/[\+\.:]/)) {
                    $msg.set('In progress', true, true);
                } else {
                    clearInterval(timer);
                }
                pipeline.update(run, md5, run_id);
            }
        });
    }

    $.get("/api/pipelines/" + pipeline_id + "/")
        .success(function(pipeline_raw) {
            pipeline.load(pipeline_raw);
            pipeline.draw();
            grabStatus();
        }
    );

    $(document).off('cancel', '.ctrl_menu');
});