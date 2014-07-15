
$(document).ready(function(){ // wait for page to finish loading before executing jQuery code
    var nForms = $('#cdmForms > tr').length;

    // modify name attribute of forms
    for (var i = 0; i < 1 + nForms; i++) {
        $('#id_datatype_' + i).attr('name', 'datatype_' + i);
        $('#id_column_name_' + i).attr('name', 'column_name_' + i);
        $('#id_column_idx_' + i).attr('name', 'column_idx_' + i);
    }

    $("#addForm").click(function() {  // query button by id selector
        // copies the existing row rather than writing new HTML
        // also changes ID/name fields in a dynamic, flexible way.
        var $forms = $('#cdmForms'),
            newrow = $('tr:last', $forms).clone(),
            index_cell = $('td', newrow).eq(0);
        
        index_cell.html(parseInt(index_cell.html()) + 1);
        
        $('input, select', newrow).each(function() {
            var this_n = parseInt(this.id.match(/[0-9]+$/)[0]) + 1,
                stub_regexp = /^(.+_)[0-9]+$/;
            
            $(this).attr({
                id:   this.id.match(stub_regexp)[1] + this_n,
                name: $(this).attr('name').match(stub_regexp)[1] + this_n
            });
        });
        
        $forms.append(newrow);
        nForms++;
    });
    
    $("#removeForm").click(function() {
        // zero rows not allowed
        if (nForms > 1) {
            nForms--;
            $('#cdmForms').find('tr:last').remove();
        }
    });

    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        $(this).wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
    });
    
    $('a[rel="ctrl"]').on('click', function (e) {
        $(this).siblings('.fulltext').show().css({ top: e.pageY, left: e.pageX });
        setTimeout("$('.fulltext').fadeOut(300);", 2000);
    });
});

