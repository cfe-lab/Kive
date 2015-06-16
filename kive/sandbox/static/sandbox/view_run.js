/*
 *
 * Includes:
 * /static/pipeline/drydock_objects.js
 * /static/pipeline/drydock.js
 * /static/pipeline/pipeline_add.js
 * /static/pipeline/pipeline_load.js
 */


function setupRunView(rtp_id, pipeline_id, md5) {
    var self = this;

    // Instance variables
    self.timer = null;
    self.timerInterval = 1000;
    self.pipeline = new Pipeline(canvasState);

    // Methods
    function grabStatus() {
        if(self.timer === null)
            self.timer = setInterval(grabStatus, self.timerInterval);

        // Poll the server
        $.getJSON("/api/runs/" + rtp_id + "/run_status/", {}, function(run){
            var stat = run.status,
                $msg = $('<span class="status-message">');
            $msg.append('<a>Complete</a>').attr(
                    'href',
                    '/view_results/' + parseInt(run.id));

            // TODO: Use a better system of reporting overall run status
            if(stat.indexOf('?') >= 0) {
                $msg.empty().text('Waiting for run to start').append(
                        $('<img src="/static/sandbox/preload.gif"/>'));
            }
            else if (stat.indexOf('Too') >= 0) {
                $msg.empty().text(stat)
                clearInterval(self.timer);
            }
            else {
                if(stat.indexOf('!') >= 0) {
                    $msg.empty().append($('<a>Failed!</a>').attr(
                            'href',
                            '/view_results/' + rtp_id));
                    clearInterval(self.timer);
                } else if(stat.indexOf('.') >= 0 || stat.indexOf('+') >= 0 || stat.indexOf(':') >= 0) {
                    $msg.empty().text('In progress').append(
                            $('<img src="/static/sandbox/preload.gif"/>'));
                } else {
                    clearInterval(self.timer);
                }
                pipeline.update(run, md5);
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