"using strict";

// Use a closure so as not to muck up the global namespace
(function(window){

    function coderevision_link($td, revision) {
        if (revision.content_file.length !== 0) {

        }
        var $a = $("<a/>").attr("href", revision.absolute_url).text("Revise");
        $td.append($a);
    }

    function coderevision_view_link($td, revision) {
        var $a = $("<a/>").attr("href", revision.view_url).text(revision.display_name);
        $td.append($a);
    }

    function buildDownload($td, revision) {
        if (revision.content_file.length !== 0) {
            $td.append($('<a/>').text('Download').attr('href', revision.download_url));
        }
    }

    var CodeResourceRevisionTable = function($table, is_user_admin, cr_pk,
                                             $active_filters, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "../../api/coderesourcerevisions/";
        this.cr_pk = cr_pk;

        var CRRTable = this;
        this.filterSet = new permissions.FilterSet(
            $active_filters,
            function() {
                CRRTable.reloadTable();
            }
        );

        this.registerColumn("#", "revision_number");
        this.registerColumn("Name", coderevision_view_link);
        this.registerColumn("", coderevision_link);
        this.registerColumn("Description", "revision_desc");
        this.registerColumn("Date", "revision_DateTime");
        this.registerColumn("", buildDownload);

        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };

    CodeResourceRevisionTable.prototype = Object.create(permissions.PermissionsTable.prototype);

    CodeResourceRevisionTable.prototype.getQueryParams = function() {
        var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);

        // This adds a filter for the current CodeResource.
        var $cr_filter = this.filterSet.add("coderesource_id", this.cr_pk, true);
        $cr_filter.hide();

        params.filters = this.filterSet.getFilters();

        return params;
    };

    function resource_revisions_main(is_user_admin, $table, cr_pk, $active_filters, $navigation_links){
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

        var table = new CodeResourceRevisionTable($table, is_user_admin, cr_pk, $active_filters,
                                                  $navigation_links);
        table.reloadTable();
    }

    // Export the main function to the global namespace
    window.resource_revisions_main = resource_revisions_main;
})(window);
