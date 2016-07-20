"use strict";

/*
 */

var RunsTable = function($table, user, is_user_admin, $no_results, runbatch_pk, $active_filters, $navigation_links) {
    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
    this.$no_results = $no_results;
    var runsTable = this;

    this.user = user;

    this.session_filters_key = runbatch_pk === null ? "runFilters" : "batchFilters_" + runbatch_pk;
    this.session_page_key = runbatch_pk === null? "runPage" : "batchPage_" + runbatch_pk;

    this.filterSet = new permissions.FilterSet(
            $active_filters,
            function() {
                runsTable.page = 1;
                runsTable.reloadTable();
                sessionStorage.setItem(
                    this.session_filters_key,
                    runsTable.filterSet.getPairs()
                );
            });

    this.runbatch_pk = runbatch_pk;

    this.list_url = "/api/runs/status/";
    this.reload_interval = pollingInterval;

    this.create_url = "/api/runs/";

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

    if (runbatch_pk === null) {
        this.registerColumn("Batch", function($td, run) {
            var $name;
            if (run.runbatch !== null) {
                $name = $("<a/>").attr("href", "../runbatch/" + run.runbatch);
                if (! run.runbatch_name) {
                    $name.text("[batch " + run.runbatch + " (unnamed)]");
                }
                else {
                    $name.text(run.runbatch_name);
                }
                $td.append($name);
            }
        });
    }

    this.registerColumn("Start", function($td, run) {
        $td.text(run.run_progress.start || '-');
    });
    this.registerColumn("End", function($td, run) {
        $td.text(run.run_progress.end || '-');
    });

    this.registerStandardColumn("user");

    // This is a stop/rerun column.
    this.registerColumn(" ", function($td, run) {

        // A "rerun" link, accessible to anyone who can see this Run.
        var $rerun_link = $("<a/>");
        $rerun_link.attr("href", runsTable.create_url)
            .text("Rerun")
            .click({
                run: run,
                run_table: runsTable
            }, clickRerun);
        $td.append($rerun_link);

        if (run.stopped_by !== null) {
            $td.append(" (Stopped by user " + run.stopped_by + ")");
        }
        else if (run.run_progress.end === null &&
                (runsTable.user === run.user || ! runsTable.is_locked)) {
            var $stop_link = $("<a/>");
            $stop_link.attr("href", run.url)
                .attr("run_id", run.id)
                .text("Stop")
                .click(this, clickStop);
            $td.append(" (");
            $td.append($stop_link);
            $td.append(")");
        }
    });

    function clickStop(event) {
        var $a = $(this),
            run_id = $a.attr("run_id"),
            run_url = $a.attr("href"),
            run_table = event.data;
        event.preventDefault();
        var stop_message = "Are you sure you want to stop this run?";
        if (window.confirm(stop_message)) {
            $.ajax(
                {
                    url: run_url,
                    method: "PATCH",
                    data: {
                        is_stop_requested: true
                    },
                    success: function () {
                        run_table.reloadTable();
                    }
                }
            ).fail(
                function (request) {
                    var response = request.responseJSON,
                        detail = (
                            response ?
                            response.detail :
                            "Failed to redact"
                        );
                    window.alert(detail);
                }
            );
        }
    }

    function clickRerun(event) {
        var $a = $(this),
            run = event.data.run,
            run_table = event.data.run_table;

        event.preventDefault();

        $.ajax(
            {
                url: run_table.create_url,
                method: "POST",
                data: JSON.stringify({
                    pipeline: run.pipeline,
                    name: run.name,
                    description: run.description,
                    users_allowed: run.users_allowed,
                    groups_allowed: run.groups_allowed,
                    inputs: run.inputs
                }),
                contentType: "application/json",
                processData: false,
                success: function () {
                    run_table.reloadTable();
                }
            }
        ).fail(
            function (request) {
                var response = request.responseJSON,
                    detail = (
                        "Failed to rerun"
                    );
                window.alert(detail);
            }
        );
    }
};
RunsTable.prototype = Object.create(permissions.PermissionsTable.prototype);

RunsTable.prototype.getQueryParams = function() {
    var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
    params.filters = this.filterSet.getFilters();
    sessionStorage.setItem(this.session_page_key, this.page);

    if (this.runbatch_pk !== null) {
        // Filter for the current RunBatch.
        params.filters.push(
            {
                key: "batch_pk",
                val: this.runbatch_pk
            }
        );
    }

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

RunsTable.prototype.buildHeaders = function($tr) {
    this.buildPermissionHeaders($tr);
    $tr.eq(0).attr(
            'title',
            'steps-outputs\n? new\n. waiting\n: ready\n+ running\n* finished\n! failed\nx cancelled\n# quarantined');
}