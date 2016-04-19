/*
 *
 * Includes:
 * /static/pipeline/drydock_objects.js
 * /static/pipeline/drydock.js
 * /static/pipeline/pipeline_add.js
 * /static/pipeline/pipeline_load.js
 */


function setupRunView(run_id, pipeline_id, md5) {
    var self = this;
    run_id = parseInt(run_id);

    // Instance variables
    self.timer = null;
    self.timerInterval = 1000;
    self.pipeline = new Pipeline(canvasState);

    // Methods
    function setMsg ($el, msg_text, use_link, use_loading_anim) {
        if (use_link) {
            $el.html('<a href="/view_results/' + run_id +'?back_to_view=true">'+ msg_text +'</a>');
        } else {
            $el.text(msg_text);
        }
        if (use_loading_anim) {
            $el.prepend('<img src="/static/sandbox/preload.gif"/> &nbsp;');
        }
    }
    function grabStatus() {
        if(self.timer === null)
            self.timer = setInterval(grabStatus, self.timerInterval);

        // Poll the server
        $.getJSON("/api/runs/" + run_id + "/run_status/", {}, function(run){
            var stat = run.status,
                $msg = $('<span class="status-message">');

            setMsg($msg, 'Complete', true);

            // TODO: Use a better system of reporting overall run status
            if(stat.indexOf('?') >= 0) {
                setMsg($msg, 'Waiting for run to start', false, true);
            } else if (stat.indexOf('Too') >= 0) {
                setMsg($msg, stat);
                clearInterval(self.timer);
            } else {
                if (stat.indexOf('!') >= 0) {
                    setMsg($msg, 'Failed!', true);
                    clearInterval(self.timer);
                } else if (stat.indexOf('.') >= 0 || stat.indexOf('+') >= 0 || stat.indexOf(':') >= 0) {
                    setMsg($msg, 'In progress', true, true);
                } else {
                    clearInterval(self.timer);
                }
                pipeline.update(run, md5, run_id);
            }
            $('#run_status').empty().append($msg);
        });
    }

    $.ajax({
        type: "GET",
        url: "/api/pipelines/" + pipeline_id + "/",
        datatype: "json",
        success: function(pipeline_raw) {
            pipeline.load(pipeline_raw);
            pipeline.draw();
            grabStatus();
        }
    });
};