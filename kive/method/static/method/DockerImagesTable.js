(function(permissions) {//dependent on PermissionsTable class
	"use strict";
	permissions.DockerImagesTable = function($table, is_user_admin, $navigation_links) {
	    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
	    this.list_url = "api/dockerimages/";
	    this.registerLinkColumn("Name", "", "name", "absolute_url");
	    this.registerColumn("Tag", "tag");
	    this.registerColumn("Git", "git");
	    this.registerColumn("Description", "description");
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
	};
	permissions.DockerImagesTable.prototype = Object.create(permissions.PermissionsTable.prototype);
})(permissions);