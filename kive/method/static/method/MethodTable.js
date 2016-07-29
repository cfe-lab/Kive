(function(permissions) {//dependent on PermissionsTable class
    "use strict";
    permissions.MethodTable = function($table, is_user_admin, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "../../api/methods/";
        this.registerLinkColumn("Name", "", "display_name", "view_url");
        this.registerColumn("", function($td, method) {
            $("<a>").attr("href", method.absolute_url).text("Revise").appendTo($td);
        });
        this.registerColumn("Description", "revision_desc");
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };
    permissions.MethodTable.prototype = Object.create(permissions.PermissionsTable.prototype);
})(permissions);