$(function() {
    noXSS();

    $("#id_single-dataset_file").on("change", function () {
        var path = $(this).val().split("\\");
        var filename = path[path.length - 1].split(".").slice(0, -1).join(".");
        $("#id_single-name").val(filename);
    });

    $("#id_externalfiledirectory").on("change", function () {
        efd_id = this.value;

        $("#id_external_path").empty();
        if (efd_id !== "") {
            $.getJSON(
                "/api/externalfiledirectories/" + efd_id + "/list_files/",
                {}, // specify data as an object
                function(result) {
                    var options = [];
                    $.each(result, function(index, value) {
                        options.push(
                            $('<option>').attr('value', value.path).text(value.display_name)
                        );
                    });
                    $("#id_external_path").empty().append(options);
                }
            );
        }
    });
});