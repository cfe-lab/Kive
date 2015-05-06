/**
 * Created by rliang on 15-05-01.
 */

var adminLock;


// This function is responsible for retrieving the list of MethodFamilies
// from the API.
function lock_handler(is_admin) {
    $.getJSON(
        "api/methodfamilies",
        {
            is_admin: is_admin
        },
        function (method_families)
        {
            show_method_families($(".families table tbody"), method_families, is_admin);
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

// FIXME update from compounddatatypes.js
function build_list_cell(names) {
    var $ul = $('<ul/>');
    $.each(names, function() {
        $ul.append($('<li/>').text(this));
    });
    return $('<td/>').append($ul);
}


function show_method_families($tbody, method_families, is_admin) {
    $tbody.empty();

    $.each(method_families, function () {
        var $row = $("<tr/>");
        var $family_link = $("<a/>").attr("href", this["absolute_url"]).text(this["name"]);
        $row.append($("<td/>").append($family_link));
        $row.append($("<td/>").text(this["description"]));
        $row.append($("<td/>").text(this["user"]["username"]));
        $row.append(build_list_cell(this["users_allowed"]));
        $row.append(build_list_cell(this["groups_allowed"]));

        $row.append($("<td/>").text(this["num_revisions"]));
        if (is_admin) {
            var $a = ($('<a/>')
                    .attr('planUrl', this['removal_plan'])
                    .attr('mainUrl', this['url'])
                    .attr('href', '#')
                    .text('Remove')
                    .click(remove_handler));
            $row.append($('<td/>').append($a));
        }

        $tbody.append($row);
    });
}


// Code that is run after the page is finished loading.
$(function(){
    // Security stuff to prevent cross-site scripting.
    noXSS();
    adminLock = new admin_lock.AdminLock($('div.lock'), is_user_admin, lock_handler);
    lock_handler(false);
});