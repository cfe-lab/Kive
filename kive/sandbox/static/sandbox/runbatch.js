"use strict";

/*
 */

var pollingInterval = 1000, // milliseconds
    runsTable;

$(function(){ // wait for page to finish loading before executing jQuery code
    // Security stuff to prevent cross-site scripting.
    noXSS();

    var $permissions_widget = $("#permissions_widget"),
        $users_widget = $permissions_widget.find("#id_permissions_0"),
        $groups_widget = $permissions_widget.find("#id_permissions_1"),
        $edit_link_div = $("#edit_permissions"),
        $loading_msg = $("#loading_message");

    if (is_owner || is_user_admin) {
        $loading_msg.hide();
        $permissions_widget.hide();

        $("a", $edit_link_div)
            .on("click", fillPermissionsForm);

        fillPermissionsForm.call($("a", $edit_link_div));
    }

    function fillPermissionsForm(e) {
        if (e) e.preventDefault();

        $edit_link_div.hide();
        $loading_msg.show();

        // Retrieve the list of eligible users and groups that we can add permissions to.
        $.getJSON($(this).attr("href")).done(
            function (response) {
                // The response should be a list of two lists, where the first list is
                // eligible users and the second list is eligible groups.
                // Both lists should be of 2-tuples (pk, username|groupname).
                $.each(response.users, function () {
                    $("<option>")
                        .attr("value", this.id)
                        .text(this.username)
                        .appendTo($users_widget);
                });

                $.each(response.groups, function () {
                    $("<option>")
                        .attr("value", this.id)
                        .text(this.name)
                        .appendTo($groups_widget);
                });

                $loading_msg.hide();
                $permissions_widget.show()
                    .find('.permissions-widget').trigger('sync');
            }
        ).fail(
            function (request) {
                var response = request.responseJSON,
                    detail = (
                            response ?
                            response.detail :
                            "Error while finding eligible users and groups");
                $loading_msg.text(detail);
            }
        );
    }

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

    var storedPage = parseInt(sessionStorage.getItem('batchPage_' + runbatch_pk) || 1);
    runsTable.filterSet.setFromPairs(sessionStorage.getItem('batchFilters_' + runbatch_pk));
    runsTable.page = storedPage;
    runsTable.reloadTable();

});
