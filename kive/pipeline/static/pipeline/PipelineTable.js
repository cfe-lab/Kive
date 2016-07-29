(function(permissions) {//dependent on PermissionsTable class
	"use strict";

    permissions.PipelineTable = function($table, is_user_admin, family_pk, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "../../api/pipelines/";
        this.family_pk = family_pk;

        this.registerColumn("Name", function ($td, pipeline) {
            var $a = $("<a/>").attr("href", pipeline.view_url);
            if (pipeline.revision_name === '') {
                $a.text(pipeline.revision_number + ': ').append('<span class="placeholder">anonymous</span>');
            } else {
                $a.text(pipeline.display_name);
            }
            $td.append($a);
        });
        this.registerColumn("", function pipeline_revise_link($td, pipeline) {
            $("<a>").attr("href", pipeline.absolute_url).text("Revise").appendTo($td);
        });
        this.registerColumn("Description", function($td, pipeline) {
            if (pipeline.revision_desc && pipeline.revision_desc !== '') {
                $td.append(pipeline.revision_desc);
            } else {
                $td.append('<span class="placeholder">(none)</span>');
            }
        });
        this.registerColumn(
            "Published version",
            buildPublishedVersion,
            {
                family_pk: family_pk,
                table: this
            });
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };
    permissions.PipelineTable.prototype = Object.create(permissions.PermissionsTable.prototype);

    function buildPublishedVersion($td, pipeline, data) {
        var $button_group = $('<div class="button-group">'),
            $input = $('<input type="submit" value="Publish">'),
            $pipeline_pk_input = $('<input>', {
                type: 'hidden',
                name: "pipeline_pk",
                value: pipeline.id
            }),
            $action_input = $('<input type="hidden" name="action" value="publish">');
        
        if (pipeline.published) {
            $button_group.html('<input type="button" class="left-button" disabled value="Published">');
            $action_input.val("unpublish");
            $input.val("×").addClass('right-button close-button');
        }
        $button_group.append($pipeline_pk_input, $action_input, $input);

        $('<form class="publish">').submit(function(e) {
            e.preventDefault();
            $.ajax({
                type: "PATCH",
                url: "/api/pipelines/"  + pipeline.id + "/",
                data: { published: ($action_input.val() !== "unpublish") },
                datatype: "json",
                success: function() { data.table.reloadTable(); }
            }).fail(function(data) {
                $('.errortext').text('API error ' + data.status + ': ' + data.responseJSON.detail);
            });
        }).append($button_group).appendTo($td);
    }
})(permissions);