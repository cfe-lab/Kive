"use strict";

/*
 */

var pollingInterval = 1000, // milliseconds
    runsTable;

var RunsTable = function($table, is_user_admin, $no_results, $active_filters, $navigation_links) {
    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
    this.$no_results = $no_results;
    var runsTable = this;
    this.filterSet = new permissions.FilterSet(
            $active_filters,
            function() {
                runsTable.page = 1;
                runsTable.reloadTable();
            });
    this.list_url = "/api/runs/status/";
    this.reload_interval = pollingInterval;

    this.registerColumn("Status", function($td, run) {
        $td.addClass("code").append($('<a/>')
                .attr('href', '/view_run/' + run.id)
                .text(run.run_progress.status));
    });

    this.registerColumn("Name", function($td, run) {
        var $name;
        if (run.id === undefined) {
            $name = $('<span/>');
        }
        else {
            $name = $('<a/>').attr("href", "view_results/" + run.id);
        }
        $td.append($name.text(run.display_name));
    });

    this.registerColumn("Start", function($td, run) {
        $td.text(run.run_progress.start || '-');
    });
    this.registerColumn("End", function($td, run) {
        $td.text(run.run_progress.end || '-');
    });

    this.registerStandardColumn("user");
};
RunsTable.prototype = Object.create(permissions.PermissionsTable.prototype);

RunsTable.prototype.getQueryParams = function() {
    var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
    params.filters = this.filterSet.getFilters();
    return params;
};

RunsTable.prototype.extractRows = function(response) {
    var $no_results = this.$no_results,
        runs;
    $no_results.empty();
    if ('detail' in response) {
        $no_results.append($('<h2>Errors:</h2>'));
        $no_results.append($('<p/>').text(response.detail));
    } else {
        runs = response.results;
        if (runs !== undefined && runs.length > 0) {
            $no_results.hide();
            this.$table.children('caption').text(
                response.count + " matching runs"
            );
            return runs;
        }
        $no_results.html('<p>No runs match your query.</p>');
    }

    $no_results.show();
    return []; // no runs
};

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
        e.preventDefault();
        runsTable.filterSet.addFromForm(this);
    });
    
    runsTable = new RunsTable(
        $('#runs'),
        is_user_admin,
        $('.no_results'),
        $('#active_filters'),
        $(".navigation_links")
    );
    // runsTable.filterSet.add('active');
    runsTable.reloadTable();
});
