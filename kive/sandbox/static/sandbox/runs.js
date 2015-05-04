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
    ajaxRequest,
    adminLock;

/* Ask the server for a progress report of the run. */
function poll_run_progress(run_data) {
    $.getJSON(
            "poll_run_progress",
            {
                filters: get_run_filters(),
                is_admin: adminLock.is_admin,
                previous: run_data
            },
            function (new_data) {
                var errors = new_data['errors'];
                if (errors.length != 0) {
                    show_errors(errors);
                }
                else {
                    if (new_data['changed'] || run_data == null) {
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
    $('.no_results').empty();
}

function get_run_filters() {
    var filters = [];
    $('#active_filters .filter').each(function() {
        filters.push($(this).data());
    });
    
    return filters;
}

function show_errors(errors) {
    var $no_results = $('.no_results');
    $no_results.empty();
    $no_results.append($('<h2>Errors:</h2>'));
    $.each(errors, function() {
        $no_results.append($('<p/>').text(this));
    });
    $('.results').hide();
    $no_results.show();
}

/* Display the progress of a run on the page. */
function show_run_progress(run_data) {
    var $name,
        run_id,
        $tbody = $(".results tbody"),
        $row;
    if (run_data == null || run_data['runs'].length == 0) {
        $('.no_results').html('<p>No results match your query.</p>').show();
        $('.results').hide();
        return;
    }
    $('.no_results').hide();
    $('.results').show();
    $tbody.empty();
    $.each(run_data['runs'], function() {
        $row = $('<tr/>');
        $run_status = $('<a/>').attr("href", "/view_run/" + this.rtp_id).text(this.status);
        $row.append($('<td class="code"/>').append($run_status));
        console.log(run_data)
        run_id = this["id"];
        if (run_id == null) {
            $name = $('<span/>');
        }
        else {
            $name = $('<a/>').attr("href", "view_results/" + run_id);
        }
        $row.append($('<td/>').append($name.text(this["name"])));
        $row.append($('<td/>').text(this["start"] || '-'));
        $row.append($('<td/>').text(this["end"] || '-'));
        $tbody.append($row);
    });
    $('.results table caption').text(
            run_data['has_more']
            ? 'Showing ' + run_data['runs'].length + ' most recent matching runs.'
            : 'Showing all matching runs.')
}

function remove_handler() {
    var $filter = $(this).closest('.filter');
    $filter.detach();
    reset_polling();
    poll_run_progress();
}

function lock_handler() {
    reset_polling();
    poll_run_progress();
}

function add_filter(key, value) {
    var $filters = $('#active_filters'),
        $filter,
        $duplicates;
    $filter = $('<div class="filter"/>').data('key', key);
    $filter.append($('<span class="field"/>').text(key + ':'));
    if (value != null) {
        $filter.data('val', value);
        $filter.append($('<span class="value"/>').text(value));
    }
    $duplicates = $('div.filter', $filters).filter(function() {
        var $f = $(this);
        return $f.data('key') == key && $f.data('val') == value;
    });
    if ( !$duplicates.length) {
        $filter.append($('<a class="remove">&times;</a>').click(remove_handler));
        $filters.prepend($filter);
    }
}

function normalize_date(text) {
    var monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        date = new Date(text),
        min = date.getMinutes();
    if (Number.isNaN(min)) {
        return null;
    }
    if (min < 10) {
        min = "0" + min;
    }
    return (date.getDate() + " " + monthNames[date.getMonth()] + " " + 
            date.getFullYear() + " " + date.getHours() + ":" + min);
}

$(function(){ // wait for page to finish loading before executing jQuery code
    // Security stuff to prevent cross-site scripting.
    noXSS();
    
    $('.advanced-filter').prepend('<input type="button" class="close ctrl" value="Close">');

    $('input[value="Advanced"]').on('click', function() {
        $(this).closest('.short-filter').fadeOut({ complete: function() {
            $(this).siblings('.advanced-filter').fadeIn()
                .closest('li').addClass('advanced');
        } });
    });

    $('.advanced-filter input.close.ctrl').on('click', function() {
        $(this).closest('.advanced-filter').fadeOut({ complete: function() {
            $(this).siblings('.short-filter').fadeIn()
                .closest('li').removeClass('advanced');
        } });
    });
    
    $('form.short-filter, form.advanced-filter').submit(function(e) {
        var $fields = $('input[type="text"], input:checked', this);
        e.preventDefault();
        $fields.each(function() {
            var $field = $(this),
                value = $field.val();
            if (value.length == 0) {
                return;
            }
            if ($field.is('.datetime')) {
                value = normalize_date(value);
            }
            add_filter(
                    $field.attr('name'),
                    $field.is(':checked') ? null : value);
            if ($field.is(':checked')) {
                $field.attr('checked', false);
            }
            else {
                $field.val('');
            }
        });
        reset_polling();
        poll_run_progress();
    });
    
    add_filter('active');
    adminLock = new admin_lock.AdminLock(
            $('#active_filters .lock'),
            is_user_admin,
            lock_handler);
    
    poll_run_progress();
});