"use strict";

import { CanvasState, Pipeline } from "static/pipeline/pipeline_all";
import 'jquery';
declare var permissions: any;

permissions.PipelineRunTable = function($table, is_user_admin, $navigation_links) {
    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
    this.list_url = "/api/pipelinefamilies/";
    this.registerColumn("Pipeline Family", "name");
    this.registerColumn("Thumbnail", buildThumbnail);
    this.registerColumn("Revision", buildMembers);
    this.registerStandardColumn("user");
    this.registerStandardColumn("users_allowed");
    this.registerStandardColumn("groups_allowed");
};
permissions.PipelineRunTable.prototype = Object.create(
        permissions.PermissionsTable.prototype);
permissions.PipelineRunTable.prototype.buildTable = function(rows) {
    permissions.PermissionsTable.prototype.buildTable.call(this, rows);
    this.drawThumbnails();
};
permissions.PipelineRunTable.prototype.drawThumbnails = function() {
    this.$table.find('select').each(function() { drawThumbnail(this); });
};

function drawThumbnail(select) {
    var $select = $(select),
        $canvas = $select.closest('tr').find('canvas'),
        canvas = $canvas[0];
    $.getJSON("/api/pipelines/" + $select.val()).done(function(result) {
        var cs = new CanvasState(<HTMLCanvasElement> canvas, false);
        var pipeline = new Pipeline(cs);
        cs.setScale(0.12);
        cs.enable_labels = false;
        pipeline.load(result);
        pipeline.draw();
    });
}
function buildThumbnail($td, row) {
    $td.addClass('preview-canvas');
    $('<canvas class="preview" width="120" height="90">' +
            'Warning: Kive does not support your web browser.</canvas>')
            .appendTo($td);
}
function buildMembers($td, row) {
    var $form = $('<form method="GET" action="choose_inputs">'),
        $select = $('<select name="pipeline">'),
        already_have_one_selected = false,
        i, member, $option;
	
    if (((row.members.length == 1) && (! row.members[0])) || (row.members.length === 0)) {
        $option = $("<option>")
            .attr("disabled", 1)
            .text("No published versions");
        $select.append($option).appendTo($form);
    } else {
        for (i = 0; i < row.members.length; i++) {
            member = row.members[i];
            $option = $('<option>').attr('value', member.id);
            if (member.published) {
                $option.text(member.display_name);
                if (!already_have_one_selected) {
                    $option.attr('selected', true);
                    already_have_one_selected = true;
                }
            } else {
                // De-emphasize this.
                $option.text(member.display_name + "*");
            }
            $select.append($option);
        }
        $select.change(function() { drawThumbnail(this); });
        $form.append($select, '&nbsp;<input type="submit" value="Choose">');
    }
    $td.append($form);
}

// already exported - manipulating in the global scope
// export permissions;