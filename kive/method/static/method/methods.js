function method_link($td, method) {
    var $a = $("<a/>").attr("href", method.absolute_url).text("Revise");
    $td.append($a);
}

function method_view_link($td, method) {
    var $a = $("<a/>").attr("href", method.view_url).text(method.display_name);
    $td.append($a);
}

var MethodTable = function($table, is_user_admin, family_pk, $active_filters, $navigation_links) {
    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
    this.list_url = "../../api/methods/";

    this.family_pk = family_pk;

    var MethodTable = this;
    this.filterSet = new permissions.FilterSet(
        $active_filters,
        function() {
            MethodTable.page = 1;
            MethodTable.reloadTable();
        }
    );
    // This adds a filter for the current MethodFamily.
    var $mf_filter = this.filterSet.add("methodfamily_id", family_pk, true);
    $mf_filter.hide();

    this.registerColumn("Name", method_view_link);
    this.registerColumn("", method_link);
    this.registerColumn("Description", "revision_desc");

    this.registerStandardColumn("user");
    this.registerStandardColumn("users_allowed");
    this.registerStandardColumn("groups_allowed");
};

MethodTable.prototype = Object.create(permissions.PermissionsTable.prototype);

MethodTable.prototype.getQueryParams = function() {
    var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
    params.filters = this.filterSet.getFilters();
    return params;
};

// Code to be called after loading.
function methods_main(is_user_admin, $table, family_pk, $active_filters, $navigation_links) {
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

    var table = new MethodTable($table, is_user_admin, family_pk, $active_filters, $navigation_links);

    table.reloadTable();
}