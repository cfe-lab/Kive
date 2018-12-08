(function(permissions) {//dependent on PermissionsTable class
	"use strict";
	permissions.ContainerRunTable = function($table, is_user_admin, $navigation_links) {
	    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
	    this.list_url = "api/containerruns/";
	    this.registerLinkColumn("Name", "", "name", "absolute_url");
	    this.registerColumn("Description", "description");
	    this.registerColumn("App", "app_name");
	    this.registerColumn("State", "state");
	    this.registerDateTimeColumn("Start", "start_time");
	    this.registerDateTimeColumn("End", "end_time");
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
	};
	permissions.ContainerRunTable.prototype = Object.create(permissions.PermissionsTable.prototype);
})(permissions);
