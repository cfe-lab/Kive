/**
 * Created by rliang on 15-05-05.
 */


var MethodsTable = function($table, family_pk, is_user_admin) {
    permissions.PermissionsTable.call(this, $table, is_user_admin);
    this.list_url = "../../api/methodfamilies/" + family_pk + "/methods/";
    this.basic_headers = ["Name", "Description", ""]; // FIXME update this once permissions is updated to allow entries with links
    this.basic_fields = ["revision_name", "revision_desc", "absolute_url"];
};
MethodsTable.prototype = Object.create(permissions.PermissionsTable.prototype);

// Code that is run after the page is finished loading.
$(function(){
    // Security stuff to prevent cross-site scripting.
    noXSS();

    // family_pk and is_user_admin must be defined prior to importing this file.
    var table = new MethodsTable($('#methods'), family_pk, is_user_admin);
    table.buildTable($.parseJSON($('#initial_data').text()));
});