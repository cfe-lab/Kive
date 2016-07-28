$(function() {
    "use strict";
    var is_editable = false;
    var lock_icon_path = "/static/portal/img/";

    if (window.hasOwnProperty(is_owner) && is_owner) {
        is_editable = true;
    } else if (window.hasOwnProperty(is_admin) && is_admin) {
        // The lock div is only there if the user is an administrator (and not the owner).
        $("#lock").click(function() {
            is_editable = !is_editable;
            showEditReadonly();
        });
    }

    (function showEditReadonly() {
        var img_path = "lock-locked-2x.png";
        var hide = ".edit";
        var show = ".readonly";
        if (is_editable) {
            img_path = "lock-unlocked-2x.png";
            hide = show;
            show = ".edit";
        }
        $("#lock img").attr("src", lock_icon_path + img_path);
        $(hide).hide();
        $(show).show();
    })();// trigger once immediately
});