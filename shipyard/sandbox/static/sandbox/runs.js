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

/* polling interval. */
var pollingInterval = 1000,
    timeoutId,
    ajaxRequest;

/* Ask the server for a progress report of the run. */
function poll_run_progress(run_data) {
    ajaxRequest = $.getJSON(
            "poll_run_progress",
            {
                filters: get_run_filters(),
                previous: run_data
            },
            function (new_data) {
                var errors = new_data['errors'];
                if (errors.length != 0) {
                    show_errors(errors);
                }
                else {
                    if (new_data['changed']) {
                        show_run_progress(new_data);
                    }
                    else {
                        new_data = run_data;
                    }
                    timeoutId = setTimeout(
                            poll_run_progress,
                            pollingInterval,
                            new_data);
                }
            });
}

function reset_polling() {
    ajaxRequest.abort();
    window.clearTimeout(timeoutId);
    $('.results tbody').empty();
}

function get_run_filters() {
    var filters = [];
    $('#active_filters .filter').each(function() {
        filters.push($(this).data());
    });
    
    return filters;
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
    var $name,
        run_id,
        $tbody = $(".results tbody"),
        $row;
    $tbody.empty();
    $.each(run_data['runs'], function() {
        $row = $('<tr/>');
        $row.append($('<td class="code"/>').text(this["status"]));
        run_id = this["id"];
        if (run_id == null) {
            $name = $('<span/>');
        }
        else {
            $name = $('<a/>').attr("href", "view_results/" + run_id);
        }
        $row.append($('<td/>').append($name.text(this["name"])));
        $tbody.append($row);
    });
}

$(function(){ // wait for page to finish loading before executing jQuery code
    // Security stuff to prevent cross-site scripting.
    noXSS();
    
    run_data = [];
    poll_run_progress(run_data);
    
    $('a.remove').click(function() {
        var $filter = $(this).closest('.filter'),
            $active_filters = $filter.closest('.active_filters');
        $filter.detach();
        reset_polling();
        poll_run_progress([]);
    });
    
    $('form.short-filter').submit(function(e) {
        var $filters = $('#active_filters'),
            $filter = $('<div class="filter" data-key="name"/>'),
            $search = $('input[type="text"]', this),
            v = $search.val();
        e.preventDefault();
        $filter.attr('data-val', $search.val());
        $filter.append($('<span class="field">Name:</span>'));
        $filter.append($('<span class="value"/>').text($search.val()));
        $filter.append($('<a class="remove">&times;</a>'));
        $filters.append($filter);
        reset_polling();
        poll_run_progress([]);
    });
});