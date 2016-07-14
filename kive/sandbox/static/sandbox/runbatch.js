"use strict";

/*
 */

var pollingInterval = 1000, // milliseconds
    runsTable;

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
        runsTable.filterSet.addFromForm(this);
    });

    runsTable = new RunsTable(
        $('#runs'),
        user,
        is_user_admin,
        $('.no_results'),
        runbatch_pk,
        $('#active_filters'),
        $(".navigation_links")
    );

    var storedPage = parseInt(sessionStorage.getItem('runPage') || 1);
    runsTable.filterSet.setFromPairs(sessionStorage.getItem('runFilters'));
    runsTable.page = storedPage;
    runsTable.reloadTable();
});
