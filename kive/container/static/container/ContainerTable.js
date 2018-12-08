(function(permissions) {//dependent on PermissionsTable class
	"use strict";
	permissions.ContainerTable = function($table, is_user_admin, $navigation_links) {
	    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
	    this.list_url = "/api/containers/";
	    this.registerLinkColumn("Tag", "", "tag", "absolute_url");
	    this.registerColumn("Description", "description");
        this.registerColumn("Apps", "num_apps");
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
	};
	permissions.ContainerTable.prototype = Object.create(permissions.PermissionsTable.prototype);
})(permissions);
