// wait for page to finish loading before executing jQuery code
$(function(){
    noXSS();

    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        var $this = $(this);
        $this.wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
    });

    $('a[rel="ctrl"]').on('click', function (e) {
        $(this).siblings('.fulltext').show().css({ top: e.pageY, left: e.pageX, 'z-index': 3 });
        setTimeout(function() { $('.fulltext').fadeOut(300); }, 5000);
    });

    $("#id_content_file").on("change", function() {
        path = $(this).val().split("\\");
        filename = path[path.length-1].split(".").slice(0, -1).join(".");
        $("#id_resource_name").val(filename);
    });
});
