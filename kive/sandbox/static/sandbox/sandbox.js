/* Display an error on the page. */
function display_error(message) {
    $("#errors").text(message);
}

/* Display a table, hiding columns not indicated in "data-displaycols". */
function columnPresentation (tab) {
    var data = tab.data();
    
    if (typeof data.displaycols != 'undefined') {
        cells = $('td,th', tab);
        dcols = data.displaycols.toString().split(',');
        
        for (var i = 0; i < dcols.length; i++) {
            cells = cells.not(':nth-child('+ dcols[i] + ')');
        }
        cells.hide();
    }
}

/* Custom jQuery function to retrieve a tuple's primary key according to the table's metadata.
   This is either:
   1- a cell's contents
   2- the value of a select field within a cell
   */
$.fn.get_pkey = function(pkey) {
    if (this.prop('tagName') == "TR") {
        var row_key = $('td', this).eq( parseInt( pkey ) );
        
        if (pkey.toString().match(/:selected$/)) {
            row_key = row_key.find(':selected').val();
        } else {
            row_key = row_key.html();
        }
        
        return parseInt(row_key);
    }
};

$(function(){ // wait for page to finish loading before executing jQuery code
    // Security stuff to prevent cross-site scripting.
    noXSS();

    // Run a pipeline when "submit" is pressed.
    $("#run_pipeline").on("submit", function(e) {
        var tbselects = $('.tbselect-value'),
            submit = $('input[type="submit"]', this);

        // Check if all inputs have been selected
        if (tbselects.filter(function() { return this.value === ''; }).length === 0) {
            tbselects.detach().appendTo(this);
        } else {
            display_error("Not all inputs have been set.");
            e.preventDefault();
        }
    });
    
    $('#choose_inputs').on('submit', function(e) {
        var tbselect = $('.tbselect-value');
        
        // Check if a pipeline has been selected
        if (tbselect.val() !== '') {
            tbselect.attr('name', 'pipeline').detach().appendTo(this);
        } else {
            display_error("No pipeline selected.");
            e.preventDefault();
        }
    });
    
    $('.advanced-filter').prepend('<input type="button" class="close ctrl" value="Close">');

    $('input[value="Advanced"]').on('click', function() {
        $(this).closest('.short-filter').fadeOut({ complete: function() {
            $(this).siblings('.advanced-filter').fadeIn()
                .closest('li').addClass('advanced');
        } });
    });

    $('.advanced-filter input.close.ctrl').on('click', function() {
        $(this).closest('.advanced-filter').fadeOut({ complete: function() {
            $(this).siblings('.short-filter').fadeIn()
                .closest('li').removeClass('advanced');
        } });
    });
    

    /*
     TABLE FILTER

     Prepare the HTML using 2 or 3 elements.
     #filter_ctrl is mandatory and must contain a <form> with <input> or <select>s.
        Each form element must have a data-link attribute specifying the column it operates on.
        e.g. <input data-link="title" type="text">
     #active_filters is optional. Don't put anything in it, it'll get emptied out.
        The only reason to put this in your HTML code is if you're specific about where it goes.
        Otherwise it is dynamically written to before table#table_results.

     This script supports wildcards (*) and logical OR (|).
     Bug: Single letters between two |'s malfunctions. (i.e. a|b|c will not work. however, a|bc|d will work.)

     Note:
     Every time a filter is added or removed, ALL filters are removed and then added back.
     This is simpler to code but it could stand to be more efficient.
     */

    var tables = $('.results'),
        cpanels = $('.filter_ctrl');
    if (tables.length + cpanels.length > 0) {
        tables.each(function() {
            var $this = $(this),
                cols = [],
                filters_html = $this.siblings('.active_filters'),
                pkey = $this.data('pkey');
        
            if (filters_html.length == 0)
                $this.before('<div class="active_filters">');
            
            $('thead tr > *', this).each(function() {
                cols.push(this.innerHTML);
            });
        
            filters_html.data('filters_js', []);
            
            if (!!pkey.match(/:selected$/)) {
                pkey = cols.indexOf(pkey.replace(":selected", "")) + ":selected";
            } else {
                pkey = cols.indexOf(pkey);
            }
            
            $this.data({
                'cols': cols,
                'pkey': pkey,
                'selected': []
            });
        
            columnPresentation($this);
        });
        
        // After filters_js is populated, call this function to apply the filters to the table.
        // TODO: only return true once connection was successfully made.
        var filterTable_ajax = function(tab) { 
            var key, val, nth_cell,
                activeFilters = tab.siblings('.active_filters'),
                noResults = tab.siblings('.no_results'),
                selectedValue = tab.siblings('.tbselect-value').val(),
                numResults = $('tbody tr', tab).show().length,
                filters = activeFilters.data('filters_js') || [];
            
            tab.show();
            noResults.hide();
            activeFilters.html('');
            
            // TODO: make this generic (put all the table data in the dict, not 
            // just compound_datatype specifically).
            request_data = {
                filter_data: JSON.stringify(filters),
                compound_datatype: tab.data('compounddatatype')
            };
            
            $.getJSON(tab.data('ajax-url'), request_data, function (data) {
                display_error("");
                var tbody = tab.find('tbody'),
                    new_tbody = [],
                    cols = tab.data('cols'),
                    thumbnail_column_index = cols.indexOf('Thumbnail'),
                    bg = 'background-color';
                
                for (var i = 0; i < data.length; i++) {
                    var new_row = [];
                    for (var j = 0; j < cols.length; j++) {
                        new_row[j] = data[i][cols[j]];
                    }
                    if (thumbnail_column_index >= 0) {
                        new_row[thumbnail_column_index] = (
                                "<canvas class=\"preview\">Warning: Kive " +
                                "does not support your web browser.</canvas>"); 
                    }
                    new_tbody.push(new_row);
                }
                
                for (i = 0; i < new_tbody.length; i++) {
                    new_tbody[i] = '<tr><td>' + new_tbody[i].join('</td><td>') + '</td></tr>';
                }
                numResults = new_tbody.length;
                if (numResults > 0) {
                    var new_cells = tbody.html( new_tbody.join("\n") ).find('th,td');
                    if (thumbnail_column_index >= 0) {
                        $('canvas.preview', tbody).each(
                                load_preview_canvas).closest('td').addClass(
                                        'preview-canvas');
                    }
                    columnPresentation(tab);
                
                    // Was a row already selected, and is it still in the returned set?
                    if (selectedValue !== "") {
                        // Check index row. nth-child(), as a CSS function, is 1-indexed
                        // rather than 0-indexed like everything else. We use jQuery.filter()
                        // to search the rows. Any matches have their click event triggered.
                        console.log('tbody td:nth-child(' + (parseInt(tab.data('pkey')) + 1) + ')');
                        var selectedRow = $('tbody td:nth-child(' + (parseInt(tab.data('pkey')) + 1) + ')', tab)
                                .filter(function() {
                                    return $('select', this).val() == selectedValue;
                                }).closest('tr').click();
                    
                        // If no rows were matched, the selection is cleared to be empty.
                        if (selectedRow.length == 0) {
                            tab.siblings('.tbselect-value').val('');
                        }
                    }
                
                    new_cells.each(function() {
                        var old_col = $(this).css(bg);
                        $(this).css(bg, '#ffd')
                            .animate({ 'background-color': old_col }, {
                                duration: 500,
                                complete: function() { $(this).css(bg, ''); }
                            });
                    });
                }
                else {
                    tab.hide();
                    noResults.show();
                    tab.siblings('.tbselect-value').val('');
               }
            }).fail(function() {
                /* Contingency in case of Django error. */
                display_error("Whoops! Something went wrong.");
            });
            
            for (var i = 0; i < filters.length; i++) {
                key = filters[i].key;
                val = filters[i].val.toString();
                var bool = filters[i].val === true;
                
                nth_cell = tab.data('cols').indexOf(key) + 1;
                
                if (typeof filters[i].invisible === 'undefined' || !filters[i].invisible) {
                    // Write the UI element for this filter.
                    if (bool) {
                        activeFilters.append('\
                            <div class="filter" data-key="' + key + '" data-val="' + val.replace(/"/g, "\\$&") + '">\
                                <span class="field">' + key + '</span>\
                                <a class="remove">&times;</a>\
                            </div>\
                        ');
                    } else {
                        activeFilters.append('\
                            <div class="filter" data-key="' + key + '" data-val="' + val.replace(/"/g, "\\$&") + '">\
                                <span class="field">' + key + ': </span>\
                                <span class="value">' + val.replace(/(.)\|+(.)/g, '$1 <span class="logic_gate">OR</span> $2') + '</span> \
                                <a class="remove">&times;</a>\
                            </div>\
                        ');
                    }
                }
            }

            if (filters.length == 0) {
                tab.find("caption").html("showing most recent 10 datasets");
            } else {
                tab.find("caption").html("");
            }
        }
        
        $('form', cpanels).on('submit',function(e) {
            e.preventDefault();
            var val_fields = $('input[type="text"], input[type="hidden"], select', this),
                bool_fields = $('input[type="checkbox"], input[type="radio"]', this),
                filters_html = $(this).closest('.filter_ctrl').siblings('.active_filters');
                filters_js = filters_html.data('filters_js') || [];
            
            val_fields.each(function() {
                if (this.value.length > 0) {
                    var dupe = false,
                        key = $(this).data('link');
                    
                    for (var i = 0; i < filters_js.length; i++) {
                        if (filters_js[i].key == key 
                            && filters_js[i].val == this.value) {
                            dupe = true;
                            break;
                        }
                    }
                    
                    if (!dupe) {
                        filters_js.push({ key: key, val: this.value });
                    }
                }
            }).val('');
            
            bool_fields.each(function() {
                var key = $(this).data('link');
                if (this.checked) {
                    for (var i = 0; i < filters_js.length; i++) {
                        if (filters_js[i].key == key) {
                            return;
                        }
                    }
                    
                    filters_js.push({ key: $(this).data('link'), val: true });
                } else {
                    for (var i = 0; i < filters_js.length; i++) {
                        if (filters_js[i].key == key) {
                            filters_js.splice(i, 1);
                            return;
                        }
                    }
                }
            });
            
            filterTable_ajax(filters_html.siblings('.results'));
        });
    
        // Mechanism for removing filter elements.
        // TODO: only execute this function once filterTable_ajax returns true.
        $('.active_filters').on('click', 'a.remove', function() {
            var filter = $(this).closest('.filter'),
                container = filter.closest('.active_filters'),
                filters = container.data('filters_js');
            filter = filter.data();
        
            for (var i = 0; i < filters.length; i++) {
                if (filters[i].key == filter.key && filters[i].val == filter.val) {
                    filters.splice(i, 1);
                    container.data('filters_js', filters);
                    break;
                }
            }
            filterTable_ajax(container.siblings('.results'));
        });

        $(".results tbody tr").click(function () {
            var tab = $(this).closest('.results'),
                tbselect = tab.siblings('input.tbselect-value');

            $('tr', tab).removeClass('selected');

            // classList.contains is faster than jQuery .hasClass or .is('.class').
            if (this.classList.contains('selected')) {
                tbselect.val('');
            }
            else {
                tbselect.val( $(this).get_pkey(tab.data('pkey')) );
                $(this).addClass('selected');
            }

            tbselect.trigger('change');
        });
        $(".results .pipeline_selector").change(function () {
            var tab = $(this).closest('.results'),
                tbselect = tab.siblings('input.tbselect-value'),
                parent_row = $(this).parent("tr");

            new_pkey = parent_row.get_pkey(tab.data("pkey"))
            tbselect.val(new_pkey)
            tbselect.trigger("change")
        });
        
        /* Browsers will remember input values, even for hidden fields, on page refresh
         * In this function we add this behaviour to our table select widget, which is not a native <form> element.
         * The widget stores its selected value in an <input type="hidden">.
         * On pageload, these inputs all have an empty string value, unless the browser is "remembering".
         */
        $('.tbselect-value').filter(function() { return this.value !== ''; }).each(function() {
            var tab = $(this).siblings('.results'),
                remembered_value = this.value,
                pkey = tab.data('pkey');
            
            $('tbody tr', tab).filter(function() {
                return $(this).get_pkey(pkey) == remembered_value;
            }).click();
        });
        
        submit = $('input[type="submit"]').prop('disabled', false);
    }
});
