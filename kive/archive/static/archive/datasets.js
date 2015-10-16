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

    var DatasetsTable = function($table, is_user_admin, $active_filters, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "/api/datasets/";

        var datasetsTable = this;
        this.filterSet = new permissions.FilterSet(
            $active_filters,
            function() {
                datasetsTable.reloadTable();
            }
        );

        this.$navigation_links = $navigation_links;

        this.registerColumn("Name", function($td, dataset) {
            $td.append($('<a/>')
                    .text(dataset.name).attr('href', '/dataset_view/'+dataset.id));
        });
        this.registerColumn("Description", buildDescription);
        this.registerColumn("Date Created", buildDateCreated);
        this.registerColumn("File Size", "filesize_display");
        this.registerColumn("", buildDownload);

        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };
    DatasetsTable.prototype = Object.create(
            permissions.PermissionsTable.prototype);

    DatasetsTable.prototype.getQueryParams = function() {
        var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
        params.filters = this.filterSet.getFilters();
        return params;
    };
    
    my.main = function(is_user_admin) {
        noXSS();

        $('.advanced-filter').prepend('<input type="button" class="close ctrl" value="Close">');

        $('input[value="Advanced"]').on('click', function() {
            $(this).closest('.short-filter').fadeOut({ complete: function() {
                $(this).siblings('.advanced-filter').fadeIn()
                    .closest('li').addClass('advanced');
            } });
        });

        $('.advanced-filter input.close.ctrl').on('click', function() {
            $(this).closest('.advanced-filter').fadeOut({ complete: function() {
                $(this).siblings('.short-filter').fadeIn()
                    .closest('li').removeClass('advanced');
            } });
        });

        $('form.short-filter, form.advanced-filter').submit(function(e) {
            e.preventDefault();
            table.filterSet.addFromForm(this);
        });

        var table = new DatasetsTable(
            $('#datasets'),
            is_user_admin,
            $("#active_filters"),
            $(".navigation_links")
        );
        table.filterSet.add('uploaded');
        table.reloadTable();
    };

    return my;
}());
