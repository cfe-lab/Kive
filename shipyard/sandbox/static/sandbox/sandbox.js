/*
 * run_data is a JSON object of the form {"run": integer, "status": string, "finished": bool},
 * where "run" is the primary key of the Run, "status" is a string describing the Run's status,
 * and "finished" is true if the Run is done or False otherwise.
 */

var timeout = 1000;

function poll_run_progress(run_data) {
    setTimeout(function() {
        $.ajax({
            type: "POST",
            url: "poll_run_progress",
            data: run_data,
            datatype: "json",
            success: function (new_data) { 
                new_data = $.parseJSON(new_data);
                show_run_progress(new_data);
                if (!new_data["finished"]) {
                    poll_run_progress(new_data); 
                }
            }
        });
    }, timeout);
}

function show_run_progress(run_data) {
    $("#submit").val(run_data["status"]);
}

$(document).ready(function(){ // wait for page to finish loading before executing jQuery code
    // Security stuff to prevent cross-site scripting.
    noXSS();
    
    $("#submit").on("click", function () {
        $(this).disabled = true;
        $.ajax({
            type: "POST",
            url: "run_pipeline",
            data: $("#inputs_form").serialize(),
            datatype: "json",
            success: function (result) { 
                run_data = $.parseJSON(result);
                show_run_progress(run_data);
                poll_run_progress(run_data); 
            }
        });
        return false;
    });
});
