
var DatatypesTable = function($table, is_user_admin) {
    permissions.PermissionsTable.call(this, $table, is_user_admin);
    this.list_url = "api/datatypes/";
    this.basic_headers = ["Name", "URL", "Description", "Restricts", "Date created"]; // FIXME
    this.basic_fields = ["name", "absolute_url", "description", "restricts", "date_created"]; // FIXME
};
DatatypesTable.prototype = Object.create(permissions.PermissionsTable.prototype);


// Code that is run after the page is finished loading.
$(function(){
    // Security stuff to prevent cross-site scripting.
    noXSS();

    var table = new DatatypesTable($('#datatypes'), is_user_admin);
    table.buildTable($.parseJSON($('#initial_data').text()));
});
