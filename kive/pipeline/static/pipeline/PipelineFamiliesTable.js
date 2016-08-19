(function(permissions) {//dependent on PermissionsTable class
	"use strict";
    permissions.PipelineFamiliesTable = function($table, is_user_admin, $active_filters, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "api/pipelinefamilies/";
        this.registerLinkColumn("Family", "", "name", "absolute_url");
        this.registerColumn("Description", "description");
        this.registerColumn("# revisions", "num_revisions");
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };
    permissions.PipelineFamiliesTable.prototype = Object.create(permissions.PermissionsTable.prototype);
})(permissions);