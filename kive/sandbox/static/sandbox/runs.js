"use strict";

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

var pollingInterval = 1000, // milliseconds
    runsTable;

function get_run_filters() {
    var filters = [];
    $('#active_filters .filter').each(function() {
        filters.push($(this).data());
    });
    
    return filters;
}

function remove_handler() {
    var $filter = $(this).closest('.filter');
    $filter.detach();
    runsTable.reloadTable();
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

var RunsTable = function($table, is_user_admin, $no_results) {
    permissions.PermissionsTable.call(this, $table, is_user_admin);
    this.$no_results = $no_results;
    this.list_url = "/api/runs/status/";
    this.reload_interval = pollingInterval;
    this.registerColumn("Status", function($td, run) {
        $td.addClass("code").append($('<a/>')
                .attr('href', '/view_run/'+run.rtp_id)
                .text(run.status));
    });
    this.registerColumn("Name", function($td, run) {
        var $name;
        if (run.id === undefined) {
            $name = $('<span/>');
        }
        else {
            $name = $('<a/>').attr("href", "view_results/" + run.id);
        }
        $td.append($name.text(run.name));
    });
    this.registerColumn("Start", function($td, run) {
        $td.text(run.start || '-');
    });
    this.registerColumn("End", function($td, run) {
        $td.text(run.end || '-');
    });
}
RunsTable.prototype = Object.create(permissions.PermissionsTable.prototype);
RunsTable.prototype.getQueryParams = function() {
    var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
    params.filters = get_run_filters();
    return params;
}
RunsTable.prototype.extractRows = function(response) {
    var $no_results = this.$no_results,
        runs;
    $no_results.empty();
    if ('errors' in response) {
        $no_results.append($('<h2>Errors:</h2>'));
        $.each(response.errors, function() {
            $no_results.append($('<p/>').text(this));
        });
    } else {
        runs = response.runs;
        if (runs !== undefined && runs.length > 0) {
            $no_results.hide();
            this.$table.children('caption').text(
                    response.has_more
                    ? 'Showing ' + runs.length + ' most recent matching runs.'
                    : 'Showing all matching runs.')
            this.$table.show();
            return runs;
        }
        $no_results.html('<p>No runs match your query.</p>');
    }
    
    this.$table.hide();
    $no_results.show();
    return []; // no runs
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
                value = permissions.formatDate(value);
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
        runsTable.reloadTable();
    });
    
    add_filter('active');
    runsTable = new RunsTable($('#runs'), is_user_admin, $('.no_results'));
    runsTable.reloadTable();
});
