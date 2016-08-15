(function(permissions) {//dependent on PermissionsTable class
    "use strict";
    permissions.CodeResourceTable = function($table, is_user_admin, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "api/coderesources/";
        this.registerLinkColumn("Name", "", "name", "absolute_url");
        this.registerColumn("Description", "description");
        this.registerColumn("Filename", "filename");
        this.registerColumn("# of revisions", "num_revisions");
        this.registerDateTimeColumn("Last revision date", "last_revision_date");
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };
    permissions.CodeResourceTable.prototype = Object.create(permissions.PermissionsTable.prototype);
})(permissions);