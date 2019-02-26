(function(permissions) {//dependent on PermissionsTable class
	"use strict";
	permissions.ContainerRunTable = function($table, is_user_admin, $navigation_links) {
	    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        var runsTable = this;
	    this.list_url = "/api/containerruns/";
        this.session_filters_key = "containerrunFilters";
        this.session_page_key = "containerrunPage";
	    this.registerLinkColumn("Name", "", "name", "absolute_url");
	    this.registerLinkColumn("Batch", "", "batch_name", "batch_absolute_url");
	    this.registerColumn("State", "state");
	    this.registerDateTimeColumn("Start", "start_time");
	    this.registerDateTimeColumn("End", "end_time");
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");

        // This is a stop/rerun column.
        this.registerColumn(" ", function($td, run) {

            // A "rerun" link, accessible to anyone who can see this Run.
            $("<a>").attr("href", runsTable.list_url)
                .text("Rerun")
                .click({
                    run: run,
                    run_table: runsTable
                }, clickRerun)
                .addClass('button')
                .appendTo($td);

            if (run.stopped_by !== null) {
                $td.append(" (Stopped by user ", $('<span>').text(run.stopped_by), ")");
            } else if ('NLRS'.indexOf(run.state) < 0) {
                // Not active, don't allow a stop request.
            } else if (runsTable.user === run.user || ! runsTable.is_locked) {
                var $stop_link = $("<a>")
                    .attr({
                        "href": run.url,
                        "run_id": run.id
                    })
                    .text("Stop")
                    .addClass('button')
                    .click(runsTable, clickStop);
                $td.append(" (", $stop_link, ")");
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
                $.ajax({
                    url: run_url,
                    method: "PATCH",
                    data: {
                        is_stop_requested: true
                    },
                    success: function() {
                        run_table.reloadTable();
                    }
                }).fail(function (request) {
                    var response = request.responseJSON,
                        detail = (
                            response ?
                            response.detail :
                            "Failed to stop the run."
                        );
                    window.alert(detail);
                });
            }
        }

        function clickRerun(event) {
            var $a = $(this),
                run = event.data.run,
                run_table = event.data.run_table;

            event.preventDefault();

            $.ajax({
                url: run_table.list_url,
                method: "POST",
                data: JSON.stringify({
                    name: run.name,
                    original_run:run.url,
                    description: run.description,
                    users_allowed: run.users_allowed,
                    groups_allowed: run.groups_allowed
                }),
                contentType: "application/json",
                processData: false
            }).fail(function () {
                window.alert("Failed to create run.");
            }).done(function () {
                run_table.reloadTable();
            });
        }
	};
	permissions.ContainerRunTable.prototype = Object.create(permissions.PermissionsTable.prototype);
})(permissions);
