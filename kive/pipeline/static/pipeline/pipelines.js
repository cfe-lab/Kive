function pipeline_link($td, pipeline) {
    var $a = $("<a/>").attr("href", pipeline["absolute_url"]).text(pipeline["display_name"]);
    $td.append($a);
}

var PipelinesTable = function($table, family_pk, is_user_admin) {
    permissions.PermissionsTable.call(this, $table, is_user_admin);
    this.list_url = "../../api/pipelinefamilies/" + family_pk + "/pipelines/";
    this.registerColumn("Name", pipeline_link);
    this.registerColumn("Description", "revision_desc");
};
PipelinesTable.prototype = Object.create(permissions.PermissionsTable.prototype);

// Code to be called after loading.
function pipelines_main(is_user_admin, family_pk, $table, bootstrap) {
    noXSS();
    var table = new PipelinesTable($table, family_pk, is_user_admin);
    table.buildTable(bootstrap);
}