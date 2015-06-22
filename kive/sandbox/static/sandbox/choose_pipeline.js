var choose_pipeline = (function() {
    "use strict";
    var my = {};
    
    my.PipelineFamiliesTable = function($table, is_user_admin, $active_filters) {
        permissions.PermissionsTable.call(this, $table, is_user_admin);
        this.list_url = "/api/pipelinefamilies/";
        var pipelineFamiliesTable = this;
        this.filterSet = new permissions.FilterSet(
                $active_filters,
                function() { pipelineFamiliesTable.reloadTable(); });
        this.registerColumn("Pipeline Family", "name");
        this.registerColumn("Thumbnail", buildThumbnail);
        this.registerColumn("Revision", buildMembers);
    };
    my.PipelineFamiliesTable.prototype = Object.create(
            permissions.PermissionsTable.prototype);
    my.PipelineFamiliesTable.prototype.getQueryParams = function() {
        var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
        params.filters = this.filterSet.getFilters();
        return params;
    };
    
    my.PipelineFamiliesTable.prototype.buildTable = function(rows) {
        permissions.PermissionsTable.prototype.buildTable.call(this, rows);
        this.drawThumbnails();
    };
    
    my.PipelineFamiliesTable.prototype.drawThumbnails = function() {
        this.$table.find('select').each(function() { drawThumbnail(this); });
    };
    
    function drawThumbnail(select) {
        var $select = $(select),
            $canvas = $select.closest('tr').find('canvas'),
            canvas = $canvas[0],
            x = arguments;
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
        var $form = $('<form method="GET" action="choose_inputs">'),
            $select = $('<select name="pipeline">');
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
        $select.change(function() { drawThumbnail(this); });
        $form.append($select);
        $form.append('&nbsp;<input type="submit" value="Choose">');
        $td.append($form);
    }
    
    return my;
}());
