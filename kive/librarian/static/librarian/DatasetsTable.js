(function(permissions) {//dependent on PermissionsTable class
    "use strict";
    permissions.DatasetsTable = function($table, is_user_admin, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "/api/datasets/";
        this.session_filters_key = "datasetFilters";
        this.session_page_key = "datasetPage";
        this.registerLinkColumn("Name", "/dataset_view/");
        this.registerColumn("Description", function($td, dataset) {
            $.each(dataset.description.split('\n'), function(_, txt){
                $td.append(txt, '<br>');
            });
        });
        this.registerColumn("Created", function($td, row) {
            $td.text(permissions.formatDate(row.date_created));
        });
        this.registerColumn("File Size", function($td, dataset) {
            var content = "<em>missing</em>";
            if (dataset.has_data) {
                content = dataset.filesize_display;
            } else if (dataset.is_redacted) {
                content = "<em>redacted</em>";
            }
            $td.append(content);
        });
        this.registerColumn("", function($td, dataset) {
            if (dataset.has_data) {
                $('<a>').text('Download').attr('href', dataset.download_url).appendTo($td);
            }
        });
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };
    permissions.DatasetsTable.prototype = Object.create(permissions.PermissionsTable.prototype);
})(permissions);