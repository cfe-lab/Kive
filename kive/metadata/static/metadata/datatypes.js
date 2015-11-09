function datatype_link($td, datatype) {
    var $a = $("<a/>").attr("href", datatype.absolute_url).text(datatype.name);
    $td.append($a);
}

var DatatypesTable = function($table, is_user_admin, $active_filters, $navigation_links) {
    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
    this.list_url = "api/datatypes/";

    var datatypesTable = this;
    this.filterSet = new permissions.FilterSet(
        $active_filters,
        function() {
            datatypesTable.page = 1;
            datatypesTable.reloadTable();
        }
    );

    this.registerColumn("Name", datatype_link);
    this.registerColumn("Description", "description");
    this.registerColumn("Restricts", "restricts");
    this.registerColumn("Date created", "date_created");

    this.registerStandardColumn("user");
    this.registerStandardColumn("users_allowed");
    this.registerStandardColumn("groups_allowed");
};
DatatypesTable.prototype = Object.create(permissions.PermissionsTable.prototype);

DatatypesTable.prototype.getQueryParams = function() {
    var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
    params.filters = this.filterSet.getFilters();
    return params;
};

// Code to be run after the page loads.
function datatypes_main($table, is_user_admin, $active_filters, $navigation_links) {
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

    var table = new DatatypesTable($table, is_user_admin, $active_filters, $navigation_links);
    table.reloadTable();
}