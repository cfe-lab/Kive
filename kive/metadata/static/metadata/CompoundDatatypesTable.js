(function(permissions) {//dependent on PermissionsTable class
	"use strict";
    permissions.CompoundDatatypesTable = function($table, is_user_admin, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "api/compounddatatypes/";
        this.registerLinkColumn("Name", "", "name", "absolute_url");
        this.registerColumn("Scheme", "representation");
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };
    permissions.CompoundDatatypesTable.prototype = Object.create(
        permissions.PermissionsTable.prototype);
    permissions.CompoundDatatypesTable.prototype.buildTable = function(rows) {
        permissions.PermissionsTable.prototype.buildTable.apply(this, [rows]);
    };
})(permissions);