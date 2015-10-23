"using strict";

// Use a closure so as not to muck up the global namespace
(function(window){

    function revisions_link($td, coderesource) {
        var $a = $("<a/>").attr("href", coderesource.absolute_url).text(coderesource.name);
        $td.append($a);
    }

    var CodeResourceTable = function($table, is_user_admin, $active_filters, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "api/coderesources/";

        var coderesourceTable = this;
        this.filterSet = new permissions.FilterSet(
            $active_filters,
            function() {
                coderesourceTable.reloadTable();
            }
        );

        this.registerColumn("Name", revisions_link);
        this.registerColumn("Description", "description");
        this.registerColumn("Filename", "filename");
        this.registerColumn("# of revisions", "num_revisions");
        this.registerColumn("Last revision date", "last_revision_date");

        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };

    CodeResourceTable.prototype = Object.create(permissions.PermissionsTable.prototype);

    CodeResourceTable.prototype.getQueryParams = function() {
        var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
        params.filters = this.filterSet.getFilters();
        return params;
    };

    function resources_main($table, is_user_admin, $active_filters, $navigation_links){
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

        var table = new CodeResourceTable($table, is_user_admin, $active_filters, $navigation_links);
        table.reloadTable();
    }

    // Export the main function to the global namespace
    window.resources_main = resources_main;
})(window);
