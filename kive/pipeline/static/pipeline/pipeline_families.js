var pipeline_families = (function() {
    "use strict";
    var my = {};
    
    function family_link($td, pipeline_family) {
        var $a = $("<a/>").attr("href", pipeline_family.absolute_url).text(pipeline_family.name);
        $td.append($a);
    }
    
    function format_published_version($td, pipeline_family) {
        $td.text(pipeline_family.published_version_display || "None");
    }
    
    var PipelineFamiliesTable = function($table, is_user_admin) {
        permissions.PermissionsTable.call(this, $table, is_user_admin);
        this.list_url = "api/pipelinefamilies/";
        this.registerColumn("Family", family_link);
        this.registerColumn("Description", "description");
        this.registerColumn("# revisions", "num_revisions");
        this.registerColumn("Published version", format_published_version);
    };
    PipelineFamiliesTable.prototype = Object.create(permissions.PermissionsTable.prototype);
    
    // Code that will be called on loading in the HTML document.
    my.main = function(is_user_admin, $table, bootstrap){
        noXSS();
        var table = new PipelineFamiliesTable($table, is_user_admin);
        table.buildTable(bootstrap);
    };
    
    return my;
}());
