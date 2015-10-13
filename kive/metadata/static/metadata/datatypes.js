function datatype_link($td, datatype) {
    var $a = $("<a/>").attr("href", datatype["absolute_url"]).text(datatype["name"]);
    $td.append($a);
}

var DatatypesTable = function($table, is_user_admin, $navigation_links) {
    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
    this.list_url = "api/datatypes/";
    this.registerColumn("Name", datatype_link);
    this.registerColumn("Description", "description");
    this.registerColumn("Restricts", "restricts");
    this.registerColumn("Date created", "date_created");

    this.registerStandardColumn("user");
    this.registerStandardColumn("users_allowed");
    this.registerStandardColumn("groups_allowed");
};
DatatypesTable.prototype = Object.create(permissions.PermissionsTable.prototype);

// Code to be run after the page loads.
function datatypes_main(is_user_admin, $table, $navigation_links) {
    noXSS();
    var table = new DatatypesTable($table, is_user_admin, $navigation_links);
    table.reloadTable();
}