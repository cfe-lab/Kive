/*
 * run_data is a JSON object of the form {"run": integer, "status": string, "finished": bool},
 * where "run" is the primary key of the Run, "status" is a string describing the Run's status,
 * and "finished" is true if the Run is done or False otherwise.
 */

/* How long to wait for a server response. */
var timeout = 1000;

/* Ask the server for a progress report of the run. */
function poll_run_progress(run_data) {
    setTimeout(function() {
        $.ajax({
            type: "POST",
            url: "poll_run_progress",
            data: run_data,
            datatype: "json",
            success: function (new_data) { 
                new_data = $.parseJSON(new_data);
                show_run_progress(new_data);
                if (new_data["finished"]) {
                    run_elem = $('<input type="hidden" name="run" value="' + run_data["run"] + '"/>');
                    $("#inputs_form").append(run_elem);
                    $("#submit").unbind("click");
                    $("#inputs_form").attr("action", "view_results");
                } else {
                    poll_run_progress(new_data); 
                }
            }
        });
    }, timeout);
}

/* Display the progress of a run on the page. */
function show_run_progress(run_data) {
    $("#submit").val(run_data["status"]);
}

$(function(){ // wait for page to finish loading before executing jQuery code
    // Security stuff to prevent cross-site scripting.
    noXSS();
    
    // Run a pipeline when "submit" is pressed.
    $("#run_pipeline").on("submit", function(e) {
        e.preventDefault();
        
        $('input[type="submit"]', this).prop('disabled', 1);
        var tbselects = $('.tbselect-value').detach().appendTo(this);
        
        if (tbselects.filter(function() { return this.value === ''; }).length === 0) {
            console.log($(this).serialize());
            /*
            $.ajax({
                type: "POST",
                url: "run_pipeline",
                data: $(this).serialize(),
                datatype: "json",
                success: function (result) { 
                    run_data = $.parseJSON(result);
                    show_run_progress(run_data);
                    poll_run_progress(run_data); 
                }
            });*/
        }
        else console.log('not all fields are filled');
        
        return false;
    });
    
    $('.tbselect-value').filter(function() { return this.value !== ''; }).each(function() {
        console.log('remembered input value detected');
        /* TODO: browsers will remember input values, even for hidden fields, on page refresh.
         * Write a function to sync input tables with their hidden input representation.
         */
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
        var columnPresentation = function(tab) {
            var data = tab.data();
            
            if (typeof data.displaycols != 'undefined') {
                cells = $('td,th', tab);
                dcols = data.displaycols.toString().split(',');
                
                for (var i = 0; i < dcols.length; i++) {
                    cells = cells.not(':nth-child('+ dcols[i] + ')');
                }
                
                cells.hide();
            }
        };
        
        tables.each(function() {
            var $this = $(this),
                cols = [],
                filters_html = $this.siblings('.active_filters');
        
            if (filters_html.length == 0)
                $this.before('<div class="active_filters">');
            
            $('thead tr > *', this).each(function() {
                cols.push(this.innerHTML);
            });
        
            filters_html.data('filters_js', []);
            $this.data({
                'cols': cols,
                'pkey': cols.indexOf($this.data('pkey')),
                'selected': []
            });
        
            columnPresentation($this);
        });
        
        // After filters_js is populated, call this function to apply the filters to the table.
        var filterTable_ajax = function(tab) { 
            var key, val, nth_cell,
                activeFilters = tab.siblings('.active_filters'),
                noResults = tab.siblings('.no_results'),
                numResults = $('tbody tr', tab).show().length,
                filters = activeFilters.data('filters_js') || [];
            
            tab.show();
            noResults.hide();
            activeFilters.html('');
            
            request_data = {
                "filter_data": JSON.stringify(filters), 
                "compound_datatype": tab.data("compoundatatype")
            };
            
            $.getJSON("filter_datasets", request_data, function (data) {
                var tbody = tab.find('tbody'),
                    new_tbody = [];
                
                for (var i = 0; i < data.length; i++) {
                    var cols = tab.data('cols'),
                        new_row = [];
                    
                    for (var j = 0; j < cols.length; j++) {
                        new_row[j] = data[i][cols[j]];
                    }
                    
                    new_tbody.push(new_row);
                }
                
                for (i = 0; i < new_tbody.length; i++) {
                    new_tbody[i] = '<tr><td>' + new_tbody[i].join('</td><td>') + '</td></tr>';
                }
                
                numResults = new_tbody.length;
                
                if (numResults == 0) {
                    tab.hide();
                    noResults.show();
                }
                else {
                    tbody.html( new_tbody.join("\n") ).find('th,td')
                        .css('background-color','#ffd')
                        .animate({ 'background-color': '#fff' }, 2000, 'swing', function() { 
                            $(this).css('background-color', ''); 
                        });
                    columnPresentation(tab);
                }
            });
            
            for (var i = 0; i < filters.length; i++) {
                key = filters[i].key;
                val = filters[i].val;
            
                nth_cell = tab.data('cols').indexOf(key) + 1;
            
                if (typeof filters[i].invisible === 'undefined' || !filters[i].invisible) {
                    // Write the UI element for this filter.
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
    
        $('form', cpanels).on('submit',function(e) {
            e.preventDefault();
            var fields = $('input[type="text"], input[type="hidden"], select', this),
                filters_html = $(this).closest('li').find('.active_filters');
                filters_js = filters_html.data('filters_js') || [];
            
            fields.each(function() {
                if (this.value.length > 0)
                    filters_js.push({ key: $(this).data('link'), val: this.value });
            }).val('');
            
            filterTable_ajax(filters_html.siblings('.results'));
            return false;
        });
    
        // Mechanism for removing filter elements.
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
    
        $('.results').on('click', 'tbody tr', function() {
            var tab = $(this).closest('.results'),
                row_key = parseInt( $('td', this).eq( tab.data('pkey') ).html() ),
                tbselect = tab.siblings('input.tbselect-value');
            
            $('tr', tab).removeClass('selected');
            if (this.classList.contains('selected')) {
                // classList.contains is faster than jQuery .hasClass or .is('.class').
                tbselect.val('');
            }
            else {
                tbselect.val(row_key);
                $(this).addClass('selected');  
            }
        });
    }
});
