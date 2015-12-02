function cdt_link($td, cdt) {
    var $a = $("<a/>").attr("href", cdt.absolute_url).text(cdt.representation);
    $td.append($a);
}

var CompoundDatatypesTable = function($table, is_user_admin, $active_filters, $navigation_links) {
    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);

    var CDTTable = this;
    this.filterSet = new permissions.FilterSet(
        $active_filters,
        function() {
            CDTTable.page = 1;
            CDTTable.reloadTable();
        }
    );

    this.list_url = "api/compounddatatypes/";
    this.registerColumn("Scheme", cdt_link);

    this.registerStandardColumn("user");
    this.registerStandardColumn("users_allowed");
    this.registerStandardColumn("groups_allowed");
};
CompoundDatatypesTable.prototype = Object.create(
    permissions.PermissionsTable.prototype);

CompoundDatatypesTable.prototype.getQueryParams = function() {
    var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
    params.filters = this.filterSet.getFilters();
    return params;
};

CompoundDatatypesTable.prototype.buildTable = function(rows) {
    rows.sort(function(a, b) {
        return (a.representation < b.representation ?
                -1 :
                a.representation > b.representation ?
                        1 :
                        a.id - b.id);
    });
    permissions.PermissionsTable.prototype.buildTable.apply(this, [rows]);
};

$(function(){ // wait for page to finish loading before executing jQuery code
    // Security stuff to prevent cross-site scripting.
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
    
    var table = new CompoundDatatypesTable(
        $('#compounddatatypes'),
        is_user_admin,
        $("#active_filters"),
        $(".navigation_links")
    );
    table.reloadTable();
});
