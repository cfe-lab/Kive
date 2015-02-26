/*
 * run_data is a JSON object of the form 
 *
 *     [{"id": integer (optional),
 *       "name": string,
 *       "status": string}]
 *
 * - "id" is the primary key of the Run
 * - "name" is the pipeline and first input name 
 * - "status" is a string describing the Run's status
 */

/* How long to wait for a server response. */
var timeout = 1000;

/* Ask the server for a progress report of the run. */
function poll_run_progress(run_data) {
    setTimeout(function() {
        $.getJSON(
                "poll_run_progress",
                {previous: run_data},
                function (new_data) {
                    var errors = new_data['errors'];
                    if (errors.length != 0) {
                        show_errors(errors);
                    }
                    else {
                        show_run_progress(new_data);
                        poll_run_progress(new_data);
                    }
                });
    }, timeout);
}

function show_errors(errors) {
    var $progress = $("#progress");
    $progress.empty();
    $progress.append($('<h2>Errors:</h2>'));
    $.each(errors, function() {
        $progress.append($('<p/>').text(this));
    });
}

/* Display the progress of a run on the page. */
function show_run_progress(run_data) {
    var $progress = $("#progress"),
        $pre = $("<pre/>"),
        $name,
        run_id;
    $.each(run_data['runs'], function() {
        $pre.append($("<span/>").text("\n" + this["status"] + "-"));
        run_id = this["id"];
        if (run_id == null) {
            $name = $('<span/>');
        }
        else {
            $name = $('<a/>').attr("href", "view_results/" + run_id);
        }
        $pre.append($name.text(this["name"]));
    });
    $progress.empty();
    $progress.append($pre);
}

$(function(){ // wait for page to finish loading before executing jQuery code
    // Security stuff to prevent cross-site scripting.
    noXSS();
    
    run_data = [];
    poll_run_progress(run_data);
});