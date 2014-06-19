////////////////////////////////////////////////////////////////////////////////
// Utility functions for manipulating DOM objects.
////////////////////////////////////////////////////////////////////////////////

// Mark a row of a table as selected.
function select(row) {
     row.parent().children().removeClass("selected");
     row.addClass("selected");
}

// Find the row of a table which is selected.
function get_selected_row(table) {
    return (table.children("tbody").find(".selected").first());
}

////////////////////////////////////////////////////////////////////////////////
// Functions for getting at what the user wants to do.
////////////////////////////////////////////////////////////////////////////////

// Get the currently selected pipeline.
function get_selected_pipeline() {
    row = get_selected_row($("#pipeline_table"));
    return (row.find("select").first().val());
}

// Get all the currently selected input datasets (in order).
function get_selected_datasets() {
    dataset_pks = [];
    $("#data_panel").find("select").each(function (index, elem) {
        dataset_pks.push($(elem).val());
    });
    return (dataset_pks);
}

////////////////////////////////////////////////////////////////////////////////
// Functions for communicating with the server.
////////////////////////////////////////////////////////////////////////////////

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
function fetch_inputs(callback) {
    do_ajax("get_pipeline_inputs/", {"pipeline_pk": get_selected_pipeline()}, callback);
}

// Get TransformationOutputs for a pipeline.
function fetch_outputs(callback) {
    do_ajax("get_pipeline_outputs/", 
            {"pipeline_pk": get_selected_pipeline(), "dataset_pks": get_selected_datasets()}, 
            callback);
}

// Run the selected pipeline with the selected inputs.
function run_pipeline() {
    pipeline_pk = get_selected_pipeline();
    dataset_pks = get_selected_datasets();

    if (dataset_pks.some(function (elem, idx, arr) { return (elem == null); })) {
        $("#run_error").html("select one dataset for each input");
    } else if (!pipeline_pk) {
        $("#run_error").html("select a pipeline");
    } else {
        $("#run_error").html("");
        $("#run_button").html("running...");
        $("#run_button").attr("disabled", true);
        do_ajax("run_pipeline/", {"pipeline_pk": pipeline_pk, "dataset_pks": dataset_pks}, function(res) {
            $("#run_button").prop("disabled", false);
            $("#run_button").html("run");
            make_results_tables();
            poll_progress(res.pk);
        });
    }
}

// Poll the progress of a Run.
function poll_progress(run_pk) {
    $.ajax({
        type: "POST",
        url: "poll_run_progress/",
        data: {"run_pk": run_pk},
        datatype: "json",
        success: function(data) {
            $("#progress").append($("<p>test</p>"));
        },
        complete: function(jqxhr) { 
            if (jqxhr.responseText == "False") {
                poll_progress(run_pk) 
            }
        },
        timeout: 10000
    });
}

////////////////////////////////////////////////////////////////////////////////
// More complex functions for filling out the page.
////////////////////////////////////////////////////////////////////////////////

// Make tables for selecting input data.
function make_data_tables() {
    $("#data_panel").empty();
    fetch_inputs(function (objs) {
        objs.forEach(function (obj) {
            input = obj[0];
            datasets = obj[1];
            input_name = input.fields.dataset_idx + ": " + input.fields.dataset_name;

            table = ["<table width=100%><thead><tr><th>"];
            table.push(input_name);
            table.push("</th></tr></thead>")
            table.push('<tbody><tr><td><select size=5 class="data_select">')
            datasets.forEach(function (ds) {
                table.push("<option value=" + ds.pk + " uploaded=" + (ds.fields.created_by == null) + ">");
                table.push(ds.fields.name + "</option>");
            });
            table.push("</select></td></tr></tbody></table>");
            table = $(table.join(""));
            table.find("select").on("change", make_results_tables);
            $("#data_panel").append(table);
        });
    });
}

// Make tables for displaying output data.
// TODO: some duplication from make_data_tables().
function make_results_tables() {
    $("#results_panel").empty();
    fetch_outputs(function (objs) {
        objs.forEach(function (obj) {
            outcable = obj[0];
            datasets = obj[1];
            output_name = outcable.fields.dataset_idx + ": " + outcable.fields.dataset_name;

            table = ["<table width=100%><thead><tr><th>"];
            table.push(output_name);
            table.push("</th></tr></thead>")
            table.push("<tbody>")
            datasets.forEach(function (ds) {
                table.push("<tr><td>" + ds.fields.date_created + "</td></tr>");
            });
            if (datasets.length == 0) {
                table.push('<tr><td class="greyedout">no results yet</td></tr>');
            }
            table.push("</tbody></table>");
            $("#results_panel").append($(table.join("")));
        });
    });
}

function filter_uploaded_inputs() {
    if ($(this).prop("checked")) {
        $("#data_panel").find('option[uploaded="false"]').remove();
    } else {
        make_data_tables();
    }
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

    // When you click a pipeline, the input and output tables should repopulate.
    $("#pipeline_table").children("tbody").find("tr").on("click", function () {
        select($(this));
        make_data_tables();
        make_results_tables();
    });

    // When you click "run", the pipeline should be run.
    $("#run_button").on("click", run_pipeline);

    // Checking the "show only uploaded data" box should filter the inputs.
    $("#uploads_checkbox").on("click", filter_uploaded_inputs);
});
