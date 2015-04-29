"use strict";

function date_filter(date) {
    var fDate = new Date(date);
    var months = [
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
    ]

    return fDate.getDate() + ' ' +
    months[fDate.getMonth()] + ' ' +
    fDate.getFullYear() + ' ' +
    fDate.getHours()+':'+fDate.getMinutes();
}

function size_filter(size_bytes){
    return size_bytes;
}

function redact_handler(id){
    $.ajax({
        type: 'POST',
        url: '/dataset_redact/' + id,
        data: {dry_run:'true'},
        success: function(data) {
            if (window.confirm(data + '\nAre you sure?')) {
                $.ajax({
                    url: '/dataset_redact/' + id,
                    data: {datasets:'true'},
                    type: 'POST',
                    success: function(data) {
                        build_table($('#dataset_body'), data.datasets);
                    }
                });
            }
        }
    });
}

function lock_handler(is_admin){
    if (is_admin) {
        $('a.redact').closest('td').show();
        $('a.remove').closest('td').show();
    }
    else {
        $('a.redact').closest('td').hide();
        $('a.remove').closest('td').hide();
    }
}

function build_table($tbody, datasets) {
    $tbody.empty();

    for (var i in datasets) {
        var dataset = datasets[i],
            $tr = $('<tr/>'),
            $td;

        function make_td(txt, filter){
            if(filter != null)
                txt = filter(txt);
            return $tr.append($('<td/>').text(txt));
        }

        make_td(dataset.user.username);
        $tr.append($('<td/>').append($('<a/>').text(dataset.name).attr('href', dataset.view_url)));

        $td = $('<td/>');
        $.each(dataset.description.split('\n'), function(_, txt){
            $td.append(txt);
            $td.append($('<br/>'))
        });
        $tr.append($td);

        make_td(dataset.date_created, date_filter);
        make_td(dataset.filesize, size_filter);

        // Users
        $td = $('<td/>');

        if(dataset.users_allowed.length == 0){
            $td.text('None');
        }else {
            var $ul = $('<ul/>');
            $.each(dataset.users_allowed, function(_, user){
                $ul.append($('<li/>').text(user.username));
            });
            $td.append($ul);
        }
        $tr.append($td);

        // Groups
        $td = $('<td/>');

        if(dataset.groups_allowed.length == 0) {
            $td.text('None');
         } else {
            var $ul = $('<ul/>');
            $.each(dataset.groups_allowed, function(_, group){
                $ul.append($('<li/>').text(group.name));
            });
            $td.append($ul);
        }
        $tr.append($td);


        $tr.append($('<td/>').append($('<a/>').text('Download').attr('href', dataset.download_url)));

        $tr.append($('<td/>').append($('<a class="remove" href="#"/>').text('Remove').click(dataset.id, function(e){
            e.preventDefault();
            console.error('Not yet');
        })));

        $tr.append($('<td/>').append($('<a class="redact" href="#"/>').text('Redact').click(dataset.id, function(e){
            e.preventDefault();
            redact_handler(e.data);
        })));

        $tbody.append($tr);
    }
}


function datasets_main(is_user_admin, bootstrap){
    noXSS();
    build_table($('#dataset_body'), bootstrap);
    var adminLock = new admin_lock.AdminLock($('div.lock'), is_user_admin, lock_handler);
    lock_handler(false);
};
