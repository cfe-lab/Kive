/**
 * Created by rliang on 15-05-01.
 */

var adminLock;


function lock_handler(is_admin) {
    if (is_admin) {
        show_admin_viewable()
    }
    else {
        hide_admin_viewable();
    }
}


function show_admin_viewable() {
    var $admin_only = $("tbody.admin_only"),
        $subheader_row;

    $admin_only.empty();
    $subheader_row = $("<tr/>");
    $subheader_row.append($("<td colspan=6/>").text("Other Method Families"));
    $admin_only.append($subheader_row);
    ajaxRequest = $.getJSON(
        "method_family_admin_access",
        function (admin_viewable_families) {
            // Fill in the admin_only tbody.
            $.each(admin_viewable_families, function () {
                var $row = $("<tr/>");
                var $family_link = $("<a/>").attr("href", this["url"]).text(this["name"]);
                $row.append($("<td/>").append($family_link));
                $row.append($("<td/>").text(this["description"]));
                $row.append($("<td/>").text(this["user"]["username"]));

                var $users_allowed = $("<td/>");
                var $user_list = $("<ul/>");
                $.each(this["users_allowed"], function() {
                    $user_list.append($("<li/>").text(this["username"]));
                });
                $row.append($users_allowed);

                var $groups_allowed = $("<td/>");
                var $group_list = $("<ul/>");
                $.each(this["groups_allowed"], function() {
                    $group_list.append($("<li/>").text(this["name"]));
                });
                $row.append($groups_allowed);

                $row.append($("<td/>").text(this["num_revisions"]));

                $admin_only.append($row);
            });
        });
}


function hide_admin_viewable() {
    var $admin_only = $("tbody.admin_only"),
        $subheader_row;
    $admin_only.empty();
    $subheader_row = $("<tr/>");
    $subheader_row.append($("<td colspan=6/>").text("Unlock to view other Method Families"));
    $admin_only.append($subheader_row);
}


// Code that is run after the page is finished loading.
$(function(){
    // Security stuff to prevent cross-site scripting.
    noXSS();
    adminLock = new admin_lock.AdminLock($('div.lock'), is_user_admin, lock_handler);
    hide_admin_viewable();
});