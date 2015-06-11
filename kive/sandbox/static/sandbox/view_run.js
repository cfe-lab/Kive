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
                msg = '<a href="/view_results/'+run.id+'">Complete</a>';

            // TODO: Use a better system of reporting overall run status
            if(stat.indexOf('?') > -1 || stat.indexOf('Too') > -1)
                msg = 'Waiting for run to start <img src="/static/sandbox/preload.gif"/>';
            else {
                if(stat.indexOf('!') > -1) {
                    msg = '<a href="/view_results/'+rtp_id+'">Failed!</a>';
                    clearInterval(self.timer);
                } else if(stat.indexOf('.') > -1 || stat.indexOf('+') > -1 || stat.indexOf(':') > -1)
                    msg = 'In progress <img src="/static/sandbox/preload.gif"/>';
                else
                    clearInterval(self.timer);
                    pipeline.update(run, md5);
            }
            $('#run_status').html('<span class="status-message">'+msg+'</span>');
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