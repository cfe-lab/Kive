$(function() {
    // Pack help text into an unobtrusive icon
    $('.helptext').each(function() {
        var $this = $(this);
        $this.wrapInner('<span class="fulltext">').prepend('<a rel="ctrl">?</a>');
    });

    $('a[rel="ctrl"]').on('click', function (e) {
        var data = $(this).data(),
            fulltext = data.fulltext,
            left = '',
            right = '',
            top = '',
            bottom = '';

        if (e.pageX + data.width > document.body.scrollWidth) {
            right = 0;
        } else {
            left = e.pageX;
        }
        if (e.pageY + data.height > document.body.scrollHeight) {
            bottom = 0;
        } else {
            top = e.pageY;
        }
        fulltext.show()
            .css({
                top: top, 
                left: left, 
                right: right,
                bottom: bottom,
                'z-index': 999
            })
        ;
        setTimeout(function() { 
            fulltext.fadeOut(300); 
        }, 5000);
    }).each(function() {
        var $this = $(this),
            fulltext = $this.siblings('.fulltext').show(0);
        function elementIsPosFixed(el) {
            var fixed = false;
            $(el).parents().addBack().each(function() {
                fixed = $(this).css("position") === "fixed";
                return !fixed;
            });
            return fixed;
        }
        if (elementIsPosFixed(fulltext)) {
            fulltext.css('position', 'fixed');
        }
        $this.data({
            width: fulltext.outerWidth(),
            height: fulltext.outerHeight(),
            fulltext: fulltext
        });
        fulltext
            .hide(0)
            .detach().appendTo('body')
            .addClass('detached')
            .click(function() { fulltext.hide(); })
        ;
    });
});