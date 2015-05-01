function lock_handler(is_admin) {
//    if (is_admin) {
//        $('a.redact').closest('td').show();
//        $('#remove').show();
//    }
//    else {
//        $('a.redact').closest('td').hide();
//        $('#remove').hide();
//    }
}

function build_list_cell(names) {
    var $ul = $('<ul/>');
    $.each(names, function() {
        $ul.append($('<li/>').text(this));
    });
    return $('<td/>').append($ul);
}

function build_table($tbody, compoundDatatypes) {
    compoundDatatypes.sort(function(a, b) {
        return (a['representation'] < b['representation']
                ? -1
                : a['representation'] > b['representation']
                ? 1
                : a['id'] - b['id']);
    });
    $.each(compoundDatatypes, function() {
        var $tr = $('<tr/>');
        $tr.append($('<td/>').text(this['representation']));
        $tr.append($('<td/>').text(this['user']));
        $tr.append(build_list_cell(this['users_allowed']));
        $tr.append(build_list_cell(this['groups_allowed']));
        $tbody.append($tr);
    });
}

$(function(){ // wait for page to finish loading before executing jQuery code
    // Security stuff to prevent cross-site scripting.
    noXSS();
    
    var adminLock = new admin_lock.AdminLock(
            $('div.lock'),
            is_user_admin,
            lock_handler);
    build_table(
            $('#compounddatatypes tbody'),
            $.parseJSON($('#initial_data').text()));
    
    lock_handler(false);
});
