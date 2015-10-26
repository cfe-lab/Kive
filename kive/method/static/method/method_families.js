function family_link($td, method_family) {
    var $a = $("<a/>").attr("href", method_family.absolute_url).text(method_family.name);
    $td.append($a);
}

var MethodFamiliesTable = function($table, is_user_admin, $active_filters, $navigation_links) {
    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
    this.list_url = "api/methodfamilies/";

    var mfTable = this;
    this.filterSet = new permissions.FilterSet(
        $active_filters,
        function() {
            mfTable.reloadTable();
        }
    );

    this.registerColumn("Family", family_link);
    this.registerColumn("Description", "description");
    this.registerColumn("# revisions", "num_revisions");

    this.registerStandardColumn("user");
    this.registerStandardColumn("users_allowed");
    this.registerStandardColumn("groups_allowed");
};
MethodFamiliesTable.prototype = Object.create(permissions.PermissionsTable.prototype);

MethodFamiliesTable.prototype.getQueryParams = function() {
    var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
    params.filters = this.filterSet.getFilters();
    return params;
};

// Code that will be called on loading in the HTML document.
function methodfamilies_main(is_user_admin, $table, $active_filters, $navigation_links){
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

    var table = new MethodFamiliesTable($table, is_user_admin, $active_filters, $navigation_links);
    table.reloadTable();
}
