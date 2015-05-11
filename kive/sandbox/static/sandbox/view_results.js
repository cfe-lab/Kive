
function redact_handler(event) {
    var redact_url = event.data;
    
    event.preventDefault();
    $.ajax({
        url: redact_url,
        method: 'POST',
        data: {dry_run: true},
        success: function(data) {
            if (window.confirm(data + '\nAre you sure?')) {
                $.ajax({
                    url: redact_url,
                    method: 'POST',
                    success: function(data) {
                        build_table($('#outputs tbody'), data);
                    }
                })
            }
        }
    });
}

function remove_handler(event) {
    var remove_url = '../../remove_run/' + run_id;
    
    event.preventDefault();
    $.ajax({
        url: remove_url,
        method: 'POST',
        data: {dry_run: true},
        success: function(data) {
            if (window.confirm(data + '\nAre you sure?')) {
                $.ajax({
                    url: remove_url,
                    method: 'POST',
                    success: function(data) {
                        location = '../../runs';
                    }
                })
            }
        }
    });
}

/** Add a link if the URL is not blank. Add a table cell either way. */
function add_link($tr, name, url) {
    var $td = $('<td/>');
    if (url !== '') {
        $td.append($('<a/>').attr('href', url).text(name));
    }
    $tr.append($td);
}

function build_table($tbody, outputs) {
    $tbody.empty();
    for (var i in outputs) {
        var output = outputs[i],
            $tr = $('<tr/>'),
            $td;
        $tr.append($('<td/>').text(output['step_name']));
        $td = $('<td/>').text(output['output_name']);
        if (output['is_invalid']) {
            $td.append('(');
            $td.append($('<span class="error-msg">Data invalid</span>'));
            $td.append(')');
        }
        $tr.append($td);
        if (output['size'] === 'redacted') {
            $tr.append($('<td colspan="2"><em>redacted</em></td>'));
        }
        else {
            $tr.append($('<td/>').text(output['size']));
            $tr.append($('<td/>').text(output['date']));
        }
        add_link($tr, 'View', output['view_url']);
        add_link($tr, 'Download', output['down_url']);
        $td = $('<td/>');
        if (output['redact_url'] !== '') {
            $td.append($('<a class="redact" href="#">Redact</a>').click(
                    output['redact_url'],
                    redact_handler));
        }
        $tr.append($td);
        $tbody.append($tr);
    }
}

var RunsToProcessTable = function($table, is_user_admin, rtp_id) {
    permissions.PermissionsTable.call(this, $table, is_user_admin);
    this.list_url = "/api/runs/" + rtp_id + "/run_outputs/";
    this.registerColumn("Source", "step_name");
    this.registerColumn("Output", "output_name");
    this.registerColumn("Size", "size");
    this.registerColumn("Date created", "date");
    this.registerColumn("", function($td, output) {
        var href;
        if (output.id !== null) {
            href = '../../' + output.type + '_view/' + output.id;
            $td.append($('<a>View</a>').attr('href', href));
        }
    });
    this.registerColumn("", function($td, output) {
        var href;
        if (output.id !== null) {
            href = '../../' + output.type + '_download/' + output.id;
            $td.append($('<a>Download</a>').attr('href', href));
        }
    });
}
RunsToProcessTable.prototype = Object.create(
        permissions.PermissionsTable.prototype);

RunsToProcessTable.prototype.extractRows = function(response) {
    var run = response.run;
    if (run === null) {
        return [];
    }
    return run.output_summary;
}

$(function(){ // wait for page to finish loading before executing jQuery code
    // Security stuff to prevent cross-site scripting.
    noXSS();
    
    var table = new RunsToProcessTable($('#outputs'), is_user_admin, rtp_id);
    table.buildTable(table.extractRows($.parseJSON($('#outputs_json').text())));
    
    // TODO: $('#remove').click(remove_handler);
});
