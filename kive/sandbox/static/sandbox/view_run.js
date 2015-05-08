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
                    msg = '<a href="/view_results/'+run.id+'">Failed!</a>';
                    clearInterval(self.timer);
                } else if(stat.indexOf('.') > -1 || stat.indexOf('+') > -1 || stat.indexOf(':') > -1)
                    msg = 'In progress <img src="/static/sandbox/preload.gif"/>';
                else
                    clearInterval(self.timer);
                update_status(canvasState, run, md5)
            }
            $('#run_status').html('<span class="status-message">'+msg+'</span>');
        });
    }

    // Grab the pipeline
    $.ajax({
        type: "POST",
        url: "/get_pipeline/",
        data: { pipeline_id: pipeline_id },
        datatype: "json",
        success: function(result) {
            // prepare to redraw canvas
            $('#id_reset_button').click();
            submit_to_url = result.family_pk;

            draw_pipeline(canvasState, result);
            grabStatus();

            canvasState.testExecutionOrder();

            for (var i = 0; i < canvasState.shapes.length; i++)
                canvasState.detectCollisions(canvasState.shapes[i], 0.5);
        }
    });



}


