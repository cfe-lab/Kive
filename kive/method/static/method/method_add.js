$(function(){ // wait for page to finish loading before executing jQuery code
    "use strict";
    
    var cr_id;
    var $coderesource = $("#id_coderesource");
    var $dependencyForms = $("#dependencyForms");
    
    function td($elem) {
        return $('<td>').append($elem);
    }
    function makeTextInput(name, maxlen) {
        maxlen = maxlen || 255;
        return $('<input>').attr({
            id: "id_" + name,
            name: name,
            maxlength: maxlen,
            type: "text"
        });
    }
    function makeNumberInput(name) {
        return $('<input>').attr({
            id: "id_" + name,
            name: name,
            'class': "shortIntField",
            type: "number"
        });
    }
    function makeSelect(name, cssClass) {
        var $sel = $('<select>').attr({
            id: "id_" + name,
            name: name
        });
        if (cssClass) {
            $sel.addClass(cssClass);
        }
        return $sel;
    }
    function populateCRRs(cr_id, $select) {
        if (cr_id !== undefined && cr_id !== "") {
            $.getJSON("/api/coderesources/" + cr_id + "/revisions/")
                .then(function(results) {
                    var $options = results.map(function(result) {
                        return $('<option>').val(result.id).text(
                            result.revision_number + ': ' + result.revision_name );
                    });
                    $select.empty().append($options);
                }, function(err) {
                    $select.html('<option value="">ERROR! Could not retrieve CodeResourceRevisions</option>');
                });
        } else {
            $select.html('<option value="">--- select a CodeResource first ---</option>');
        }
    }
    /* http://stackoverflow.com/questions/2735067/how-to-convert-a-dom-node-list-to-an-array-in-javascript */
    function toArray(obj) {
        var array = [];
        // iterate backwards ensuring that length is an UInt32
        for (var i = obj.length >>> 0; i--;) {
            array[i] = obj[i];
        }
        return array;
    }

    // trigger ajax on CR drop-down to populate revision select
    $coderesource.on('change', function() {
        cr_id = this.value;
        populateCRRs(cr_id, $("#id_driver_revisions"));
    }).change(); // trigger on load

    /* populate CR revision dropdown on selection of CodeResource
     * By delegating this event to #dependencyForms rather than each select.coderesource
     * directly, new dynamically-generated selects retain this behaviour. */
    $dependencyForms.on('change', 'select.coderesource', function() {
        var suffix = this.id.split('_')[2];
        cr_id = this.value;
        populateCRRs(cr_id, $("#id_revisions_" + suffix));
    }).change(); // trigger on load

    var dep_coderesource_options = toArray(document.getElementById("id_coderesource_0").options),
        numberOfDepForms = $dependencyForms.children('tr').length;

    // modify name attributes for extra input forms received from server
    for (var i = 0; i < numberOfDepForms; i++) {
        $('#id_coderesource_' + i).attr('name', 'coderesource_' + i);
        $('#id_revisions_' + i).attr('name', 'revisions_' + i);
        $('#id_path_' + i).attr('name', 'path_' + i);
        $('#id_filename_' + i).attr('name', 'filename_' + i);
    }
    
    // query button by id selector
    $("#addDependencyForm").click(function() {
        numberOfDepForms ++;
        var i = numberOfDepForms - 1; // zero-based index
        var $opts = dep_coderesource_options.map(function(opt) {
            return $('<option>').val(opt.value).text(opt.text);
        });
        var $select = makeSelect("coderesource_" + i, "coderesource")
            .append($opts);
        var $select2 = makeSelect("revisions_" + i, "revisions")
            .append(
                $('<option>').val('').text('--- select a CodeResource first ---')
            ).val('');
        var $path = makeTextInput('path_' + i);
        var $filename = makeTextInput('filename_' + i);
        var $dependencyRow = $('<tr>').append(
            td($select),
            td($select2),
            td($path),
            td($filename)
        );
        $dependencyForms.find('tr:last')
            .after($dependencyRow);
    });

    $("#removeDependencyForm").click(function() {
        if (numberOfDepForms > 1) {
            numberOfDepForms --;
            $dependencyForms.find('tr:last').remove();
        }
    });


    // add or subtract input forms
    var numberOfInputForms = $('#extraInputForms').children('tr').length;
    var io_cdt_options = toArray(document.getElementById("id_compounddatatype_in_0").options);
    
    // modify name attributes for extra input forms received from server
    for (i = 0; i < numberOfInputForms; i++) {
        $('#id_dataset_name_in_' + i).attr('name', 'dataset_name_in_' + i);
        $('#id_compounddatatype_in_' + i).attr('name', 'compounddatatype_in_' + i);
        $('#id_min_row_in_' + i).attr('name', 'min_row_in_' + i);
        $('#id_max_row_in_' + i).attr('name', 'max_row_in_' + i);
    }
    
    // append row to table
    $("#addInputForm").click(function () {
        numberOfInputForms += 1;
        var i = numberOfInputForms - 1; // 0-indexing
        var $dataset_name = makeTextInput("dataset_name_in_" + i, 128);
        var opts = io_cdt_options.map(function(opt) {
            return $('<option>').val(opt.value).text(opt.text);
        });
        var $select = makeSelect("compounddatatype_in_" + i)
            .append(opts);
        var $min_row = makeNumberInput("min_row_in_" + i);
        var $max_row = makeNumberInput("max_row_in_" + i);
        var $inputRow = $('<tr>').append(
            td(numberOfInputForms),
            td($dataset_name),
            td($select),
            td($min_row),
            td($max_row)
        );
        $('#extraInputForms').find('tr:last')
            .after($inputRow);
    });
    $("#removeInputForm").click(function() {
        if (numberOfInputForms > 1) {
            numberOfInputForms -= 1;
            $('#extraInputForms').find('tr:last').remove();
        }
    });
    
    // add or subtract output forms
    var numberOfOutputForms = $('#extraOutputForms').children('tr').length;
    // we can reuse options
    
    // modify name attributes for extra input forms received from server
    for (i = 0; i < numberOfOutputForms; i++) {
        $('#id_dataset_name_out_' + i).attr('name', 'dataset_name_out_' + i);
        $('#id_compounddatatype_out_' + i).attr('name', 'compounddatatype_out_' + i);
        $('#id_min_row_out_' + i).attr('name', 'min_row_out_' + i);
        $('#id_max_row_out_' + i).attr('name', 'max_row_out_' + i);
    }

    // append row to table
    $("#addOutputForm").click(function () {
        numberOfOutputForms ++;
        var i = numberOfOutputForms - 1; // 0-indexing
        
        var $dataset_name = makeTextInput("dataset_name_out_" + i, 128);
        var $opts = io_cdt_options.map(function(opt) {
            return $('<option>').val(opt.value).text(opt.text);
        });
        var $select = makeSelect("compounddatatype_out_" + i)
            .append($opts);
        var $min_row = makeNumberInput("min_row_out_" + i);
        var $max_row = makeNumberInput("max_row_out_" + i);
        
        var $outputRow = $('<tr>').append(
            td(numberOfOutputForms),
            td($dataset_name),
            td($select),
            td($min_row),
            td($max_row)
        );

        $('#extraOutputForms').find('tr:last')
            .after($outputRow);
    });
    $("#removeOutputForm").click(function() {
        if (numberOfOutputForms > 1) {
            numberOfOutputForms --;
            $('#extraOutputForms').find('tr:last').remove();
        }
    });

    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        var $this = $(this);
        $this.wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
    });
    
    $('a[rel="ctrl"]').on('click', function (e) {
        $(this).siblings('.fulltext').show().css({ top: e.pageY, left: e.pageX, 'z-index': 3 });
        setTimeout(function() { $('.fulltext').fadeOut(300); }, 5000);
    });

    // hide Method Family form if a pre-existing family is selected
    $('#id_family').on('change', function() {
        var this_family = $(this).val();
        if (this_family === "") {
            $('#id_name').prop('disabled', false);
            $('#id_description').prop('disabled', false);
        } else {
            // TODO: ajax query populates these fields with values from DB
            $('#id_name').prop('disabled', true);
            $('#id_description').prop('disabled', true);
        }
    }).change(); // trigger on load

    // set default MethodFamily name to name of CodeResource
    $coderesource.on("change", function () {
        var $selected = $(this).children("option:selected"),
            revisionName = $selected.val() == "" ? "" : $selected.text();
        $("#id_revision_name").val(revisionName);
    });
});