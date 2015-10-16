"using strict";

// Use a closure so as not to muck up the global namespace
(function(window){

    function revisions_link($td, coderesource) {
        var $a = $("<a/>").attr("href", coderesource.absolute_url).text(coderesource.name);
        $td.append($a);
    }

    var CodeResourceTable = function($table, is_user_admin, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "api/coderesources/";
        this.registerColumn("Name", revisions_link);
        this.registerColumn("Filename", "filename");
        this.registerColumn("Description", "description");
        this.registerColumn("# of revisions", "num_revisions");
        this.registerColumn("Last revision date", "last_revision_date");

        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };

    CodeResourceTable.prototype = Object.create(permissions.PermissionsTable.prototype);

    function resources_main(is_user_admin, $table, $navigation_links){
        noXSS();
        var table = new CodeResourceTable($table, is_user_admin, $navigation_links);
        table.reloadTable();
    }

    // Export the main function to the global namespace
    window.resources_main = resources_main;
})(window);
