(function(permissions) {//dependent on PermissionsTable class
	"use strict";
	permissions.ContainerFamilyTable = function($table, is_user_admin, $navigation_links) {
	    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
	    this.list_url = "api/containerfamilies/";
	    this.registerLinkColumn("Name", "", "name", "absolute_url");
	    this.registerColumn("Git", "git");
	    this.registerColumn("Description", "description");
	    this.registerColumn("Containers", "num_containers");
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
	};
	permissions.ContainerFamilyTable.prototype = Object.create(permissions.PermissionsTable.prototype);
})(permissions);
