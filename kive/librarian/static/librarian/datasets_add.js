$(function() {
    noXSS();

    $("#id_single-dataset_file").on("change", function () {
        var path = $(this).val().split("\\");
        var filename = path[path.length - 1].split(".").slice(0, -1).join(".");
        $("#id_single-name").val(filename);
    });

    $("#id_single-externalfiledirectory").on("change", function () {
        var efd_id = this.value;
        var options = [];

        $("#id_single-external_path").empty();
        if (efd_id === "") {
            options.push(
                // FIXME this is a bit of a hack -- stash the original value somewhere in the form
                $('<option>').attr('value', "").text("--- choose an external file directory ---")
            );
            $("#id_single-external_path").append(options);

        }
        else {
            $.getJSON(
                "/api/externalfiledirectories/" + efd_id + "/list_files/",
                {}, // specify data as an object
                function(result) {
                    $.each(result.list_files,
                        function(index, value) {
                            options.push(
                                $('<option>').attr('value', value[0]).text(value[1])
                            );
                        }
                    );

                    $("#id_single-external_path").append(options);
                }
            );
        }
    });
});