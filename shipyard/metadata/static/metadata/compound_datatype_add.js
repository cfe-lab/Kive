
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
        var cdmForms = $('#cdmForms'),
            new_row = $('tr:last', cdmForms).clone();
        
        $('td', new_row)[0].innerHTML++;
        
        $('input, select', new_row).each(function() {
            var this_n = parseInt( this.id.match(/[0-9]+$/)[0] ) + 1,
                stub_regexp = /^(.+_)[0-9]+$/;
            
            $(this).attr({
                id:   this.id.match(stub_regexp)[1] + this_n,
                name: $(this).attr('name').match(stub_regexp)[1] + this_n
            });
        });
        
        cdmForms.append(new_row);
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
        $(this).siblings('.fulltext').show().css({ top: e.pageY, left: e.pageX, 'z-index': 3 });
        setTimeout("$('.fulltext').fadeOut(300);", 2000);
    });
});

