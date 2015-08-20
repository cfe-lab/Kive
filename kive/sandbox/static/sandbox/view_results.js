"use strict";

var OutputsTable = function($table, is_user_admin, rtp_id) {
    permissions.PermissionsTable.call(this, $table, is_user_admin);
    this.list_url = "/api/runs/" + rtp_id + "/run_outputs/";
    this.registerColumn("Source", "step_name");
    this.registerColumn("Input/Output", "output_name");
    this.registerColumn("Size", "size");
    this.registerColumn("Date created", "date");
    this.registerColumn("", function($td, output) {
        var href;
        if (output.id !== null) {
            href = '../../' + output.type + '_view/' + output.id;
            $td.append($('<a>View</a>').attr('href', href));
        }
    });
    this.registerColumn("", function($td, output) {
        var href;
        if (output.id !== null) {
            href = '../../' + output.type + '_download/' + output.id;
            $td.append($('<a>Download</a>').attr('href', href));
        }
    });
}
OutputsTable.prototype = Object.create(
        permissions.PermissionsTable.prototype);

OutputsTable.prototype.extractRows = function(response) {
    var run = response.run;
    if (this.$remove_link !== undefined) {
        this.$remove_link.toggle( ! this.is_locked);
    }
    else if ( ! this.is_locked) {
        this.$remove_link = $('<a href="#">Remove Run</a>').click(
                this,
                clickRemove);
        var $tr = this.$table.find('thead tr');
        $tr.append($('<th/>').append(this.$remove_link));
    }
    if (run === null) {
        return [];
    }
    return run.input_summary.concat(run.output_summary);
}

OutputsTable.prototype.getRedactionField = function(plan_url) {
    var output_ending = "output_redaction_plan/",
        error_ending  = "/error_redaction_plan/",
        ending = plan_url.substr(plan_url.length - output_ending.length);
    return (ending === output_ending
            ? "output_redacted"
            : ending === error_ending
            ? "error_redacted"
            : "is_redacted");
}

function clickRemove(event) {
    var $a = $(this),
        permissions_table = event.data,
        run_url = '/api/runs/' + rtp_id,
        plan_url = run_url + '/removal_plan/';
    event.preventDefault();
    $.getJSON(
            plan_url,
            {},
            function (plan) {
                var message = permissions_table.buildConfirmationMessage(
                        plan,
                        "remove");
                if (window.confirm(message)) {
                    $.ajax({
                        url: run_url,
                        method: 'DELETE',
                        success: function() {
                            location = '../../runs';
                        }
                    })
                }
            });
}

$(function(){ // wait for page to finish loading before executing jQuery code
    // Security stuff to prevent cross-site scripting.
    noXSS();
    
    var table = new OutputsTable($('#outputs'), is_user_admin, rtp_id);
    table.buildTable(table.extractRows($.parseJSON($('#outputs_json').text())));
});
