$(function() {
    "use strict";
    var is_editable = false;
    var lock_icon_path = "/static/portal/img/";

    function showEditReadonly() {
        if (!is_editable) {
            $("#lock img").attr("src", lock_icon_path + "lock-locked-2x.png");
            $(".edit").hide();
            $(".readonly").show();
        }
        else {
            $("#lock img").attr("src", lock_icon_path + "lock-unlocked-2x.png");
            $(".readonly").hide();
            $(".edit").show();
        }
    }

    if (is_owner) {
        is_editable = true;
    }
    else if (is_admin) {
        // The lock div is only there if the user is an administrator (and not the owner).
        $("#lock").on("click", function () {
            is_editable = !is_editable;
            showEditReadonly();
        });
    }

    showEditReadonly();
});