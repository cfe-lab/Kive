function method_link($td, method) {
    var $a = $("<a/>").attr("href", method["absolute_url"]).text(method["display_name"]);
    $td.append($a);
}

var MethodsTable = function($table, family_pk, is_user_admin) {
    permissions.PermissionsTable.call(this, $table, is_user_admin);
    this.list_url = "../../api/methodfamilies/" + family_pk + "/methods/";
    this.registerColumn("#", "revision_number");
    this.registerColumn("Name", method_link);
    this.registerColumn("Description", "revision_desc");
};
MethodsTable.prototype = Object.create(permissions.PermissionsTable.prototype);

// Code to be called after loading.
function methods_main(is_user_admin, family_pk, $table, bootstrap) {
    noXSS();
    var table = new MethodsTable($table, family_pk, is_user_admin);
    table.buildTable(bootstrap);
}