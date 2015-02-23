/*
 * run_data is a JSON object of the form 
 *
 *      {"run": integer, 
 *       "status": string,
 *       "finished": bool, 
 *       "success": bool,
 *       "queue_placeholder": integer,
 *       "crashed": bool}
 *
 * - "run" is the primary key of the Run 
 * - "status" is a string describing the Run's status
 * - "finished" is true if the Run is done or false otherwise
 * - "success" is true if the Run was successful or false if it failed
 * - "queue_placeholder" is the primary key of the RunToProcess object holding its place in the queue
 * - "crashed" is true if Shipyard crashed while running the Pipeline
 */

/* How long to wait for a server response. */
var timeout = 1000;

/* Ask the server for a progress report of the run. */
function poll_run_progress(run_data) {
    setTimeout(function() {
        $.getJSON("poll_run_progress", run_data,
            function (new_data) { 
                show_run_progress(new_data["status"]);
                if (new_data["finished"]) {
                    $("#loading").hide("slow");
                    if (new_data["success"]) {
                        show_results_link(new_data["run"]);
                    } else if (!new_data["crashed"]) {
                        handle_run_failure(new_data);
                    }
                } else {
                    poll_run_progress(new_data); 
                }
            }
        );
    }, timeout);
}

/* Display the progress of a run on the page. */
function show_run_progress(message) {
    $("#progress").html($("<pre/>").text(message));
}

/* Make appropriate adjustments to the page for a failed Run. */
function handle_run_failure(run_data) {
    $.getJSON("get_failed_output", run_data,
        function (response) {
            display_stdout_stderr(response["stdout"], response["stderr"]);
        }
    );
}

/* Display the link to the next page. */
function show_results_link(run_pk) {
    $("#progress").append('<div><a href="view_results/' + run_pk + '/">View results</a></div>');
}

/* Display stdout and stderr from a failure. */
function display_stdout_stderr(stdout, stderr) {
    $("#details").append('<h3>Output log</h3><pre>' + stdout + '</pre>');
    $("#details").append('<h3>Error log</h3><pre>' + stderr + '</pre>');
}

$(function(){ // wait for page to finish loading before executing jQuery code
    // Security stuff to prevent cross-site scripting.
    noXSS();
    
    run_data = {
            "run": 1, 
            "status": "",
            "finished": false, 
            "success": false,
            "queue_placeholder": 1,
            "crashed": false
    };
    poll_run_progress(run_data);
});