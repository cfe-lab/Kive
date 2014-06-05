// Mark a row of a table as selected.
function select(row) {
    row.parent().children().removeClass("selected");
     row.addClass("selected");
}

function get_selected_row(table) {
    return (table.children("tbody").find(".selected").first());
}

// Fetch a list of objects from the server and call a callback on them.
function do_ajax(url, data, callback) {
    $.ajax({
        type: "POST",
        url: url,
        data: data,
        datatype: "json",
        success: function (result) {
            callback(JSON.parse(result));
        }
    });
}

// Get TransformationInputs for a pipeline.
function fetch_inputs(pipeline_pk, callback) {
    do_ajax("get_pipeline_inputs/", {"pk": pipeline_pk}, callback);
}

// Make a table header.
function make_thead(header_fields) {
    thead = $("<thead><tr></tr></thead>");
    header_fields.forEach( function (field) {
        thead.append($("<td>".concat(field).concat("</td>")));
    });
    return (thead);
}

// Make data rows for a list of objects.
function fill_table(objs, fields, table) {
    tbody = table.children("tbody");
    objs.forEach(function(obj) {
        values = [];
        fields.forEach(function (field) {
            values.push(obj.fields[field]);
        });
        tr = make_tr(values);
        tr.attr("id", obj.pk);
        tbody.append(tr);
    });
}

// Make a new row for a table.
function make_tr(values) {
    row = $("<tr></tr>");
    values.forEach(function (value) {
        row.append($("<td>".concat(value).concat("</td>")));
    });
    return (row);
}

// Make tables for selecting input data.
function make_data_tables(pipeline_pk) {
    $("#data_panel").empty();
    fetch_inputs(pipeline_pk, function (objs) {
        objs.forEach(function (obj) {
            input = obj[0];
            datasets = obj[1];

            table = $("<table width=100%></table>");
            input_name = input.fields.dataset_idx.toString().concat(": ").concat(input.fields.dataset_name);
            table.append(make_thead([input_name]));
            table.append($("<tbody></tbody>"));
            fill_table(datasets, ["name"], table);
            table.children("tbody").find("tr").on("click", function () { select ($(this)); });
            $("#data_panel").append(table);
        });
    });
}

function run_pipeline() {
    row = get_selected_row($("#pipeline_table"));
    pipeline_pk = row.find(":selected").first().attr("id");

    dataset_pks = [];
    /*
    $("#data_panel").children("table").each(function (index, table) {
        dataset_pks.push(get_selected_row(table).attr("id"));
    });
    */
}

$(document).ready(function(){ // wait for page to finish loading before executing jQuery code

    // stuff copied from method
    $(document).ajaxSend(function(event, xhr, settings) {
        /*
            from https://docs.djangoproject.com/en/1.3/ref/contrib/csrf/#csrf-ajax
            On each XMLHttpRequest, set a custom X-CSRFToken header to the value of the CSRF token.
            ajaxSend is a function to be executed before an Ajax request is sent.
        */
        function getCookie(name) {
            var cookieValue = null;
            if (document.cookie && document.cookie != '') {
                var cookies = document.cookie.split(';');
                for (var i = 0; i < cookies.length; i++) {
                    var cookie = jQuery.trim(cookies[i]);
                    // Does this cookie string begin with the name we want?
                    if (cookie.substring(0, name.length + 1) == (name + '=')) {
                        cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                        break;
                    }
                }
            }
            return cookieValue;
        }
        function sameOrigin(url) {
            // url could be relative or scheme relative or absolute
            var host = document.location.host; // host + port
            var protocol = document.location.protocol;
            var sr_origin = '//' + host;
            var origin = protocol + sr_origin;
            // Allow absolute or scheme relative URLs to same origin
            return (url == origin || url.slice(0, origin.length + 1) == origin + '/') ||
                (url == sr_origin || url.slice(0, sr_origin.length + 1) == sr_origin + '/') ||
                // or any other URL that isn't scheme relative or absolute i.e relative.
                !(/^(\/\/|http:|https:).*/.test(url));
        }
        function safeMethod(method) {
            return (/^(GET|HEAD|OPTIONS|TRACE)$/.test(method));
        }

        if (!safeMethod(settings.type) && sameOrigin(settings.url)) {
            xhr.setRequestHeader("X-CSRFToken", getCookie('csrftoken'));
        }
    });

    $("#pipeline_table").children("tbody").find("tr").on("click", function () {
        select($(this));
        pipeline_pk = $(this).find("select").children(":selected").first().attr("id");
        make_data_tables(pipeline_pk);
    });

    $("#run_button").on("click", run_pipeline);
});
