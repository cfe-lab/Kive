"using strict";

// Use a closure so as not to muck up the global namespace
(function(window){

    function revisions_link($td, coderesource) {
        var $a = $("<a/>").attr("href", coderesource["absolute_url"]).text(coderesource["name"]);
        $td.append($a);
    }

    var CodeResourceTable = function($table, is_user_admin) {
        permissions.PermissionsTable.call(this, $table, is_user_admin);
        this.list_url = "api/coderesource/";
        this.registerColumn("Name", revisions_link);
        this.registerColumn("Filename", "filename");
        this.registerColumn("Description", "description");
        this.registerColumn("# of revisions", "num_revisions");
        this.registerColumn("Last revision date", "last_revision_date");
    };

    CodeResourceTable.prototype = Object.create(permissions.PermissionsTable.prototype);

    function resources_main(is_user_admin, $table, bootstrap){
        noXSS();
        var table = new CodeResourceTable($table, is_user_admin);
        table.buildTable(bootstrap);
    }

    // Export the main function to the global namespace
    window.resources_main = resources_main;
})(window);
