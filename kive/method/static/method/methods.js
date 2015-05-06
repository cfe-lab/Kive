/**
 * Created by rliang on 15-05-05.
 */

var adminLock;


// This function is responsible for retrieving the list of MethodFamilies
// from the API.
function lock_handler(is_admin) {
    $.getJSON(
        "../../api/methods",
        {
            is_admin: is_admin
        },
        function (methods)
        {
            show_methods($("#methods table tbody"), methods, is_admin);
        }
    );
}


// FIXME update from compounddatatypes.js
function remove_handler(event) {
    event.preventDefault();
    var $a = $(this);
    $.getJSON(
        $a.attr('planUrl'),
        {
            is_admin: true
        },
        function (plan) {
            var message = 'Removing ';
            for (var key in plan) {
                var count = plan[key];
                if (count > 0) {
                    message += count + ' ' + key + ', ';
                }
            }
            message += 'are you sure?'
            if (window.confirm(message)) {
                $.ajax({
                    url: $a.attr('mainUrl') + '?is_admin=true',
                    method: 'DELETE',
                    success: function() {
                        lock_handler(true);
                    }
                })
            }
        });
}


function show_methods($table_body, methods, is_admin) {
    $table_body.empty();

    $.each(methods, function () {
        var $row = $("<tr/>");
        $row.append($("<td/>").text(this["revision_number"] + ": " + this["revision_name"]));
        $row.append($("<td/>").text(this["revision_desc"]))

        var $revise_link = $("<a/>").attr("href", this["absolute_url"]).text("Revise")
        $row.append($("<td/>").append($revise_link))

        if (is_admin) {
            var $a = ($('<a/>')
                    .attr('planUrl', this['removal_plan'])
                    .attr('mainUrl', this['url'])
                    .attr('href', '#')
                    .text('Remove')
                    .click(remove_handler));
            $row.append($('<td/>').append($a));
        }

        $table_body.append($row);
    });
}


// Code that is run after the page is finished loading.
$(function(){
    // Security stuff to prevent cross-site scripting.
    noXSS();
    adminLock = new admin_lock.AdminLock($('div.lock'), is_user_admin, lock_handler);
    lock_handler(false);
});