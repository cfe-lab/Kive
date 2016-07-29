(function(permissions) {//dependent on PermissionsTable class
    "use strict";
    permissions.CodeResourceRevisionTable = function($table, is_user_admin, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "../../api/coderesourcerevisions/";
        this.registerColumn("#", "revision_number");
        this.registerLinkColumn("Name", "", "display_name", "view_url");
        this.registerColumn("", function($td, revision) {
            $("<a>").attr("href", revision.absolute_url).text("Revise").appendTo($td);
        });
        this.registerColumn("Description", "revision_desc");
        this.registerDateTimeColumn("Date", "revision_DateTime");
        this.registerColumn("", function($td, revision) {
            if (revision.content_file.length !== 0) {
                $('<a>').text('Download').attr('href', revision.download_url).appendTo($td);
            }
        });
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };
    permissions.CodeResourceRevisionTable.prototype = Object.create(permissions.PermissionsTable.prototype);
})(permissions);