var pipelines = (function() {
    "use strict";
    var my = {};

    function pipeline_link($td, pipeline) {
        var $a = $("<a/>").attr("href", pipeline.absolute_url).text(pipeline.display_name);
        $td.append($a);
    }

    function published_version($td, pipeline, data) {
        var $form = $('<form class="publish">'),
            $input = $('<input type="submit">'),
            $pipeline_pk_input = $('<input>', {
                type: 'hidden',
                name: "pipeline_pk",
                value: pipeline.id
            }),
            $action_input = $('<input type="hidden" name="action">'),
            family_pk = data.family_pk;
        
        $form.append($pipeline_pk_input, $action_input, $input);

        if (pipeline.is_published_version) {
            $form.append("Published");
            $action_input.val("unpublish");
            $input.val("Unpublish");
        } else {
            $action_input.val("publish");
            $input.val("Publish");
        }
        $form.submit(
            function(e) {
                e.preventDefault();

                var action = $action_input.val(),
                    new_published_version = $pipeline_pk_input.val();

                if (action === "unpublish") {
                    new_published_version = null;
                }

                $.ajax({
                    type: "PATCH",
                    url: "/api/pipelinefamilies/"  + family_pk,
                    data: { published_version: new_published_version },
                    datatype: "json",
                    success: function(){
                        data.table.reloadTable();
                    }
                });
            }
        );
        $td.append($form);
    }

    var PipelinesTable = function($table, family_pk, is_user_admin) {
        permissions.PermissionsTable.call(this, $table, is_user_admin);
        this.list_url = "../../api/pipelinefamilies/" + family_pk + "/pipelines/";
        this.registerColumn("Name", pipeline_link);
        this.registerColumn("Description", "revision_desc");
        this.registerColumn(
            "Published version",
            published_version,
            {
                family_pk: family_pk,
                table: this
            });
    };
    PipelinesTable.prototype = Object.create(permissions.PermissionsTable.prototype);

    // Code that will be called on loading in the HTML document.
    my.main = function(is_user_admin, family_pk, $table, bootstrap) {
        noXSS();
        var table = new PipelinesTable($table, family_pk, is_user_admin);
        table.buildTable(bootstrap);
    };

    return my;
}());
