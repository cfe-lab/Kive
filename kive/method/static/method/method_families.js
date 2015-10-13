function family_link($td, method_family) {
    var $a = $("<a/>").attr("href", method_family["absolute_url"]).text(method_family["name"]);
    $td.append($a);
}

var MethodFamiliesTable = function($table, is_user_admin, $navigation_links) {
    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
    this.list_url = "api/methodfamilies/";
    this.registerColumn("Family", family_link);
    this.registerColumn("Description", "description");
    this.registerColumn("# revisions", "num_revisions");

    this.registerStandardColumn("user");
    this.registerStandardColumn("users_allowed");
    this.registerStandardColumn("groups_allowed");
};
MethodFamiliesTable.prototype = Object.create(permissions.PermissionsTable.prototype);

// Code that will be called on loading in the HTML document.
function methodfamilies_main(is_user_admin, $table, $navigation_links){
    noXSS();
    var table = new MethodFamiliesTable($table, is_user_admin, $navigation_links);
    table.reloadTable();
}
