var choose_pipeline = (function() {
    "use strict";
    var my = {};
    
    my.PipelineFamiliesTable = function($table, is_user_admin, rows) {
        permissions.PermissionsTable.call(this, $table, is_user_admin);
        this.list_url = "/api/pipelinefamilies/";
        this.registerColumn("Pipeline Family", "name");
        this.registerColumn("Thumbnail", buildThumbnail);
        this.registerColumn("Revision", buildMembers);
        this.buildTable(rows);
    };
    my.PipelineFamiliesTable.prototype = Object.create(
            permissions.PermissionsTable.prototype);
    my.PipelineFamiliesTable.prototype.getQueryParams = function() {
        var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
        return params;
    };
    
    my.PipelineFamiliesTable.prototype.drawThumbnails = function() {
        this.$table.find('select').each(drawThumbnail);
    }
    
    function drawThumbnail() {
        var $select = $(this),
            $canvas = $select.closest('tr').find('canvas'),
            canvas = $canvas[0];
        $.ajax({
            type: "GET",
            url: "/api/pipelines/" + $select.val(),
            datatype: "json",
            success: function(result) {
                var cs = new drydock.CanvasState(canvas);
                var pipeline = new Pipeline(cs);
    
                cs.setScale(0.12);
                cs.enable_labels = false;
                pipeline.load(result);
                pipeline.draw();
            }
        });
    }

    function buildThumbnail($td, row) {
        $td.addClass('preview-canvas').append($(
                '<canvas class="preview" width="120" height="90">' +
                'Warning: Kive does not support your web browser.</canvas>'));
    }
    
    function buildMembers($td, row) {
        var $select = $('<select>');
        for (var i = 0; i < row.members.length; i++) {
            var member = row.members[i],
                $option = $('<option>').attr(
                        'value',
                        member.id).text(member.display);
            if (member.id === row.published_version) {
                $option.attr('selected', true);
            }
            $select.append($option);
        }
        $select.change(drawThumbnail);
        $td.append($select, '&nbsp;', $('<a href="#">Choose</a>'));
    }
    
    return my;
}());
