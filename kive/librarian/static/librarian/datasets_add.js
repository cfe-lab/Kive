$(function() {
    noXSS();

    if ($("#id_single-externalfiledirectory").val() === "") {
        $("#id_single-external_path").prop("disabled", true);
    }

    function setName(path) {
        var split_path = path.split(/[\\/]/);
        var filename_split_by_periods = split_path[split_path.length - 1].split(".");
        var fn_no_ext = filename_split_by_periods[0];
        if (filename_split_by_periods.length > 1) {
            fn_no_ext = filename_split_by_periods.slice(0, -1).join(".");
        }
        $("#id_single-name").val(fn_no_ext);
    }

    $("#id_single-dataset_file").on("change", function () {
        setName($(this).val());
    });

    $("#id_single-externalfiledirectory").on("change", function () {
        var efd_id = this.value;
        var options = [];

        if (efd_id === "") {
            $("#id_single-external_path").val("");
            $("#id_single-external_path").prop("disabled", true);
            $("#id_single-dataset_file").prop("disabled", false);
        }
        else {
            $("#id_single-external_path").prop("disabled", false);
            $("#id_single-dataset_file").val("");
            $("#id_single-dataset_file").prop("disabled", true);
        }
    });

    $("#id_single-external_path").on("keyup", function () {
        setName($(this).val());
    });
});