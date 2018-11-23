(function(permissions) {//dependent on PermissionsTable class
    "use strict";
    permissions.ContainerAppTable = function($table, is_user_admin, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "/api/containerapps/";
        this.registerLinkColumn("Name", "", "name", "absolute_url");
        this.registerColumn("Description", "description");
        this.registerColumn("Inputs", "inputs");
        this.registerColumn("Outputs", "outputs");
    };
    permissions.ContainerAppTable.prototype = Object.create(permissions.PermissionsTable.prototype);
    permissions.ContainerAppTable.prototype.extractRows = function(response) {
        var rows = response.results;
        for(var i in rows) {
            if (rows[i].name === '') {
                rows[i].name = '[default]';
            }
        }
        return rows;
    };
})(permissions);
