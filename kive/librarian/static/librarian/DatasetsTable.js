(function(permissions) {//dependent on PermissionsTable class
    "use strict";
    permissions.DatasetsTable = function($table, is_user_admin, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "/api/datasets/";
        this.session_filters_key = "datasetFilters";
        this.session_page_key = "datasetPage";
        this.registerLinkColumn("Name", "/dataset_view/");
        this.registerColumn("Description", function($td, dataset) {
            var description = dataset.description;
            if (description.length > 80) {
                description = description.substring(0, 77) + '...';
            }
            $td.text(description);
        });
        this.registerColumn("Created", function($td, row) {
            $td.text(permissions.formatDate(row.date_created));
        });
        this.registerColumn("File Size", function($td, dataset) {
            if (dataset.has_data) {
                $td.text(dataset.filesize_display);
            } else if (dataset.is_redacted) {
                $('<em>').text('redacted').appendTo($td);
            } else {
                $('<em>').text('missing').appendTo($td);
            }
        });
        this.registerColumn("", function($td, dataset) {
            if (dataset.has_data) {
                $('<a>').text('Download').addClass('button').attr('href', dataset.download_url).appendTo($td);
            }
        });
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };
    permissions.DatasetsTable.prototype = Object.create(permissions.PermissionsTable.prototype);
})(permissions);