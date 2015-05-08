"using strict";

// Use a closure so as not to muck up the global namespace
(function(window){

    function coderevision_link($td, coderesource) {
        var $a = $("<a/>").attr("href", coderesource["absolute_url"]).text(coderesource["revision_name"]);
        $td.append($a);
    }

    var CodeResourceRevisionTable = function($table, is_user_admin, ccr_pk) {
        permissions.PermissionsTable.call(this, $table, is_user_admin);
        this.list_url = "../../api/coderesource/" + ccr_pk + "/revisions/";
        this.registerColumn("#", "revision_number");
        this.registerColumn("Name", coderevision_link);
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
