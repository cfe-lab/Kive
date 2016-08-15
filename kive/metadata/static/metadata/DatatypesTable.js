(function(permissions) {//dependent on PermissionsTable class
	"use strict";
    permissions.DatatypesTable = function($table, is_user_admin, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "api/datatypes/";
        this.registerLinkColumn("Name", "", "name", "absolute_url");
        this.registerColumn("Description", "description");
        this.registerColumn("Restricts", "restricts");
        this.registerDateTimeColumn("Date created", "date_created");
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };
    permissions.DatatypesTable.prototype = Object.create(permissions.PermissionsTable.prototype);
})(permissions);