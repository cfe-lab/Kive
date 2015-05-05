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

function rebuild_table(){
    $.ajax({
        type: 'GET',
        url: '/api/datasets/',
        success: function(data) {
            build_table($('#dataset_body'), data);
        }
    });
}

function build_message(plan_dict, remove){
    var message = "This will " + (remove?"remove":"censor") + ": \n";
    for(var k in plan_dict)
        message += plan_dict[k] + " " + k + "(s)\n";

    return message + "Are you sure?";
}

function redact_handler(id, url, redaction_plan){
    $.ajax({
        type: 'GET',
        url: redaction_plan,
        success: function(data) {
            if (window.confirm(build_message(data, false))) {
                $.ajax({
                    url: url,
                    data: {is_redacted: 'true'},
                    type: 'PATCH',
                    success: function(data) {
                        rebuild_table();
                    }
                });
            }
        }
    });
}

function remove_handler(id, url, removal_plan){
    console.log(id, url, removal_plan);
    $.ajax({
        type: 'GET',
        url: removal_plan,
        success: function(data) {
            if (window.confirm(build_message(data, true))) {
                $.ajax({
                    url: url,
                    type: 'DELETE',
                    success: function(data) {
                        rebuild_table();
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
        $tr.append($('<td/>').append($('<a/>').text(dataset.name).attr('href', '/dataset_view/'+dataset.id)));

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

        $tr.append($('<td/>').append($('<a class="remove" href="#"/>').text('Remove').click(
            {removal_plan: dataset.removal_plan, url: dataset.url, id: dataset.id},
            function(e){
            e.preventDefault();
            remove_handler(e.data.id, e.data.url, e.data.removal_plan);
        })));

        $tr.append($('<td/>').append($('<a class="redact" href="#"/>').text('Redact').click(
            {redaction_plan: dataset.redaction_plan, url: dataset.url, id: dataset.id},
            function(e){
            e.preventDefault();
            redact_handler(e.data.id, e.data.url, e.data.redaction_plan);
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
