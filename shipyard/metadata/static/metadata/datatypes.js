
$(document).ready(function(){ // wait for page to finish loading before executing jQuery code
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
    });

    $("#id_Python_type").change(); // trigger on load
});

/*
window.onload = function () {
    'use strict';
    // trigger drop-down menu
    document.getElementById('id_Python_type').onchange();
}
function switchConstraintForm(value) {
    if (value == 'str') {
        document.getElementById('id_minval').disabled = true;
        document.getElementById('id_maxval').disabled = true;
        document.getElementById('id_minlen').disabled = false;
        document.getElementById('id_maxlen').disabled = false;
        document.getElementById('id_regexp').disabled = false;
    }
    else if (value == 'int' || value == 'float') {
        document.getElementById('id_minval').disabled = false;
        document.getElementById('id_maxval').disabled = false;
        document.getElementById('id_minlen').disabled = true;
        document.getElementById('id_maxlen').disabled = true;
        document.getElementById('id_regexp').disabled = true;
    }
    else {
        document.getElementById('id_minval').disabled = true;
        document.getElementById('id_maxval').disabled = true;
        document.getElementById('id_minlen').disabled = true;
        document.getElementById('id_maxlen').disabled = true;
        document.getElementById('id_regexp').disabled = true;
    }
}
*/