"using strict";

// Use a closure so as not to muck up the global namespace
(function(window){

    function coderevision_link($td, revision) {
        var $a = $("<a/>").attr("href", revision["absolute_url"]).text(revision["display_name"]);
        $td.append($a);
    }

    function coderevision_view_link($td, revision) {
        var $a = $("<a/>").attr("href", revision["view_url"]).text("Revise");
        $td.append($a);
    }

    var CodeResourceRevisionTable = function($table, is_user_admin, ccr_pk) {
        permissions.PermissionsTable.call(this, $table, is_user_admin);
        this.list_url = "../../api/coderesources/" + ccr_pk + "/revisions/";
        this.registerColumn("#", "revision_number");
        this.registerColumn("Name", coderevision_link);
        this.registerColumn("", coderevision_view_link);
        this.registerColumn("Description", "revision_desc");
        this.registerColumn("Date", "revision_DateTime");
    };

    CodeResourceRevisionTable.prototype = Object.create(permissions.PermissionsTable.prototype);

    function resource_revisions_main(is_user_admin, $table, crr_pk,  bootstrap){
        noXSS();
        var table = new CodeResourceRevisionTable($table, is_user_admin, crr_pk);
        table.buildTable(bootstrap);
    }

    // Export the main function to the global namespace
    window.resource_revisions_main = resource_revisions_main;
})(window);
