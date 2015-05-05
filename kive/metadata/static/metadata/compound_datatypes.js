function lock_handler(is_admin) {
    $.getJSON(
            "api/compounddatatypes/" + (is_admin ? '' : 'granted/'),
            {},
            function (compoundDatatypes) {
                build_table(
                        $('#compounddatatypes tbody'),
                        compoundDatatypes,
                        is_admin);
            });
}

function build_list_cell(names) {
    var $ul = $('<ul/>');
    $.each(names, function() {
        $ul.append($('<li/>').text(this));
    });
    return $('<td/>').append($ul);
}

function remove_handler(event) {
    event.preventDefault();
    var $a = $(this);
    $.getJSON(
            $a.attr('planUrl'),
            {},
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
                        url: $a.attr('mainUrl'),
                        method: 'DELETE',
                        success: function() {
                            lock_handler(true);
                        }
                    })
                }
            });
}

function build_table($tbody, compoundDatatypes, is_admin) {
    $tbody.empty();
    compoundDatatypes.sort(function(a, b) {
        return (a['representation'] < b['representation']
                ? -1
                : a['representation'] > b['representation']
                ? 1
                : a['id'] - b['id']);
    });
    $.each(compoundDatatypes, function() {
        var $tr = $('<tr/>'),
            $a;
        $tr.append($('<td/>').text(this['representation']));
        $tr.append($('<td/>').text(this['user']));
        $tr.append(build_list_cell(this['users_allowed']));
        $tr.append(build_list_cell(this['groups_allowed']));
        if (is_admin) {
            $a = ($('<a/>')
                    .attr('planUrl', this['removal_plan'])
                    .attr('mainUrl', this['url'])
                    .attr('href', '#')
                    .text('Remove')
                    .click(remove_handler));
            $tr.append($('<td/>').append($a));
        }
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
