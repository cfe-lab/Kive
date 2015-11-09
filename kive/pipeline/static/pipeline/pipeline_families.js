var pipeline_families = (function() {
    "use strict";
    var my = {};
    
    function family_link($td, pipeline_family) {
        var $a = $("<a/>").attr("href", pipeline_family.absolute_url).text(pipeline_family.name);
        $td.append($a);
    }
    
    function format_published_version($td, pipeline_family) {
        $td.text(pipeline_family.published_version_display_name || "None");
    }
    
    my.PipelineFamiliesTable = function($table, is_user_admin, $active_filters, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "api/pipelinefamilies/";

        var pfTable = this;
        this.filterSet = new permissions.FilterSet(
            $active_filters,
            function() {
                pfTable.page = 1;
                pfTable.reloadTable();
            }
        );

        this.registerColumn("Family", family_link);
        this.registerColumn("Description", "description");
        this.registerColumn("# revisions", "num_revisions");
        this.registerColumn("Published version", format_published_version);

        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };
    my.PipelineFamiliesTable.prototype = Object.create(permissions.PermissionsTable.prototype);

    my.PipelineFamiliesTable.prototype.getQueryParams = function() {
        var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
        params.filters = this.filterSet.getFilters();
        return params;
    };
    
    // Code that will be called on loading in the HTML document.
    my.main = function(is_user_admin, $table, $active_filters, $navigation_links){
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

        var table = new my.PipelineFamiliesTable($table, is_user_admin, $active_filters, $navigation_links);
        table.reloadTable();
    };
    
    return my;
}());
