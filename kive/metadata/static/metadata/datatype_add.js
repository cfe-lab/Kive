
$(function() {

    /*
    $("#id_Python_type").on('change', function() {
        if (this.value == 'str') {
            $("#int_constraints").hide();
            $("#str_constraints").show();
        }
        else if (this.value == 'int' || this.value == 'float') {
            $("#int_constraints").show();
            $("#str_constraints").hide();
        }
        else {
            $("#int_constraints").show();
            $("#str_constraints").show();
        }
    }
    ).change(); // trigger on load
    */

    // Ajax transaction to get Python type
    $('#id_to_hide, #int_constraints').hide();
    
    $('#id_restricts').on('change',
        function() {
            selected_options = $(this).val();

            if (selected_options.length > 0) {
                $.ajax({
                    type: "GET",
                    url: "api/datatypes/",
                    data: { base_for: selected_options }, // specify data as an object
                    datatype: "json", // type of data expected back from server
                    success: function(result) {
                        if (result.length > 1) {
                            // reject this combination of restrictions
                            $('#str_constraints, #int_constraints').hide(300); // animated with delay 300 ms
                            $('#bad_restrictions').text('Incompatible restriction of Datatypes');
                        } else {
                            $('#bad_restrictions').text('');
                            python_type = result[0].name;

                            if (python_type == 'integer' || python_type == 'float') {
                                $('#int_constraints').show(300);
                                $('#str_constraints').hide(300);
                            } else {
                                $('#int_constraints').hide(300);
                                $('#str_constraints').show(300);
                                
                                $('#id_minlen, #id_maxlen').prop('disabled', 'boolean' == python_type);// disabled if python type == 'boolean', otherwise enabled
                            }
                            
                            $('#id_to_hide').select(python_type);
                        }
                    }
                });
            }
        }
    ).change();

    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        $(this).wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
    });
    
    $('a[rel="ctrl"]').on('click', function (e) {
        $(this).siblings('.fulltext').show().css({ top: e.pageY, left: e.pageX, 'z-index': 3 });
        setTimeout(function() { $('.fulltext').fadeOut(300); }, 5000);
    });

});

