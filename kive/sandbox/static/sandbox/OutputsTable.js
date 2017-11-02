(function(permissions) {//dependent on PermissionsTable class
    "use strict";
    permissions.OutputsTable = function($table, is_user_admin, run_id) {
        permissions.PermissionsTable.call(this, $table, is_user_admin);
        var outputsTable = this;
        this.list_url = "/api/runs/" + run_id + "/run_outputs/";
        this.registerColumn("Source", "step_name");
        this.registerColumn("Input/Output", function($td, output) {
            $td.text(output.display);
            outputsTable.setErrors($td, output.errors);
        });
        this.registerColumn("Size", "size");
        this.registerColumn("Date created", "date");
        this.registerColumn("", function($td, output) {
            var href;
            if (output.id !== null) {
                href = '../../' + output.type + '_view/' + output.id + "?run_id=" + run_id + "&view_results";
                $td.append($('<a>View</a>').attr('href', href));
            }
        });
        this.registerColumn("", function($td, output) {
            var href;
            if (output.id !== null && output.filename !== null) {
                href = '../../' + output.type + '_download/' + output.id + "?run_id=" + run_id + "&view_results";
                $td.append($('<a>Download</a>').attr('href', href));
            }
        });
    };
    permissions.OutputsTable.prototype = Object.create(permissions.PermissionsTable.prototype);
    permissions.OutputsTable.prototype.extractRows = function(response) {
        if (this.$remove_link !== undefined) {
            this.$remove_link.toggle(!this.is_locked);
        } else if (!this.is_locked) {
            this.$remove_link = $('<a href="#">Remove Run</a>').click(
                    this,
                    clickRemove);
            var $tr = this.$table.find('thead tr');
            $('<th>').append(this.$remove_link).appendTo($tr);
        }
        if (response === null) {
            return [];
        }
        return response.input_summary.concat(response.output_summary);
    };
    permissions.OutputsTable.prototype.getRedactionField = function(plan_url) {
        var output_ending = "output_redaction_plan/",
            error_ending  = "/error_redaction_plan/",
            ending = plan_url.substr(plan_url.length - output_ending.length);
        return (ending === output_ending ?
                "output_redacted" :
                ending === error_ending ?
                "error_redacted" :
                "is_redacted");
    };
    function clickRemove(e) {
        var permissions_table = e.data,
            run_url = '/api/runs/' + run_id;
        e.preventDefault();
        $.getJSON(
            run_url + '/removal_plan/',
            {},
            function(plan) {
                var message = permissions_table.buildConfirmationMessage(
                        plan,
                        "remove");
                if (window.confirm(message)) {
                    $.ajax({
                        url: run_url,
                        method: 'DELETE',
                        success: function() { window.location = '../../runs'; }
                    });
                }
            }
        );
    }
})(permissions);