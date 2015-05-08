function family_link($td, method_family) {
    var $a = $("<a/>").attr("href", method_family["absolute_url"]).text(method_family["name"]);
    $td.append($a);
}

var MethodFamiliesTable = function($table, is_user_admin) {
    permissions.PermissionsTable.call(this, $table, is_user_admin);
    this.list_url = "api/methodfamilies/";
    this.registerColumn("Family", family_link);
    this.registerColumn("Description", "description")
    this.registerColumn("# revisions", "num_revisions")
};
MethodFamiliesTable.prototype = Object.create(permissions.PermissionsTable.prototype);

// Code that will be called on loading in the HTML document.
function methodfamilies_main(is_user_admin, $table, bootstrap){
    noXSS();
    var table = new MethodFamiliesTable($table, is_user_admin);
    table.buildTable(bootstrap);
}
