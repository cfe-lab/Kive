var datasets = (function() {
    "use strict";
    var my = {};

    function buildDateCreated($td, row) {
        $td.text(permissions.formatDate(row.date_created));
    }
    
    function buildDownload($td, dataset) {
        $td.append($('<a/>').text('Download').attr('href', dataset.download_url));
    }
    
    function buildDescription($td, dataset) {
        $.each(dataset.description.split('\n'), function(_, txt){
            $td.append(txt);
            $td.append($('<br/>'));
        });
    }
    
    var DatasetsTable = function($table, is_user_admin) {
        permissions.PermissionsTable.call(this, $table, is_user_admin);
        this.list_url = "/api/datasets/";
        this.registerColumn("Name", function($td, dataset) {
            $td.append($('<a/>')
                    .text(dataset.name).attr('href', '/dataset_view/'+dataset.id));
        });
        this.registerColumn("Description", buildDescription);
        this.registerColumn("Date Created", buildDateCreated);
        this.registerColumn("File Size (B)", "filesize");
        this.registerColumn("", buildDownload);
    };
    DatasetsTable.prototype = Object.create(
            permissions.PermissionsTable.prototype);
    DatasetsTable.prototype.getQueryParams = function() {
        var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
        params.is_uploaded = true;
        return params;
    };
    
    my.main = function(is_user_admin, bootstrap) {
        noXSS();
        var table = new DatasetsTable($('#datasets'), is_user_admin);
        table.buildTable(bootstrap);
    };

    return my;
}());
