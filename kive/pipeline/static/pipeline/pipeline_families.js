function family_link($td, pipeline_family) {
    var $a = $("<a/>").attr("href", pipeline_family["absolute_url"]).text(pipeline_family["name"]);
    $td.append($a);
}

function published_version($td, pipeline_family) {
    var published_version = pipeline_family["published_version"],
        published_version_text;

    if (published_version !== null) {
        published_version_text = published_version["revision_number"];
        if (published_version["revision_name"] !== "") {
            published_version_text += ": " + published_version["revision_name"];
        }

        $td.text(published_version_text);
    }
    else {
        $td.text("None");
    }
}

var PipelineFamiliesTable = function($table, is_user_admin) {
    permissions.PermissionsTable.call(this, $table, is_user_admin);
    this.list_url = "api/pipelinefamilies/";
    this.registerColumn("Family", family_link);
    this.registerColumn("Description", "description");
    this.registerColumn("# revisions", "num_revisions");
    this.registerColumn("Published version", published_version);
};
PipelineFamiliesTable.prototype = Object.create(permissions.PermissionsTable.prototype);

// Code that will be called on loading in the HTML document.
function pipelinefamilies_main(is_user_admin, $table, bootstrap){
    noXSS();
    var table = new PipelineFamiliesTable($table, is_user_admin);
    table.buildTable(bootstrap);
}
