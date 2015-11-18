$(function() {
    noXSS();

    $("#id_single-dataset_file").on("change", function () {
        var path = $(this).val().split("\\");
        var filename = path[path.length - 1].split(".").slice(0, -1).join(".");
        $("#id_single-name").val(filename);
    });
});