var MethodFamiliesTable = function($table, is_user_admin) {
    permissions.PermissionsTable.call(this, $table, is_user_admin);
    this.list_url = "api/methodfamilies/";
    this.basic_headers = ["Family", "Description", "Creator", "# revisions"]; // FIXME
    this.basic_fields = ["family_link", "description", "user", "num_revisions"]; // FIXME
};
MethodFamiliesTable.prototype = Object.create(permissions.PermissionsTable.prototype);


// Code that is run after the page is finished loading.
$(function(){
    // Security stuff to prevent cross-site scripting.
    noXSS();

    var table = new MethodFamiliesTable($('#methodfamilies'), is_user_admin);
    table.buildTable($.parseJSON($('#initial_data').text()));
});
