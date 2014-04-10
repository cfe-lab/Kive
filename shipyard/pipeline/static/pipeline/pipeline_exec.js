
/*
 TABLE FILTER
 
 Prepare the HTML using 2 or 3 elements.
 table#table_results is mandatory and must have a <thead> containing column headers and <tbody> containing the data.
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
 This is simpler to code but it could be more efficient.
 */

$(function() {
    var tables = $('.results'),
        cpanels = $('.filter_ctrl');
    if (tables.length + cpanels.length == 0) return;
    
    tables.each(function() {
        var $this = $(this),
            data = $this.data(),
            cols = [],
            filters_html = $this.siblings('.active_filters');
        
        if (filters_html.length == 0)
            $this.before('<div>', { class: 'active_filters' });
        
        $('thead tr > *', this).each(function() {
            cols.push(this.innerHTML);
        });
        
        filters_html.data('filters_js', []);
        $this.data({
            'cols': cols,
            'pkey': cols.indexOf(data.pkey),
            'selected': []
        });
        
        if (typeof data.displaycols != 'undefined') {
            for (var i = 0, cells = $('td,th', this), dcols = data.displaycols.toString().split(','); i < dcols.length; i++)
                cells = cells.not(':nth-child('+ dcols[i] + ')');
            cells.hide();
        }
        
        if (typeof data.truncatecols != 'undefined') {
            for (var i = 0, cells = $(), tcols = data.truncatecols.toString().split(','); i < tcols.length; i++)
                cells = cells.add('tbody td:nth-child('+ tcols[i] + ')', this);
            cells.addClass('truncate');
        }
    });
    
    // After filters_js is populated, call this function to apply them to the table.
    var filterTable = function(tab) { 
        var key, val, nth_cell, regex_search,
            activeFilters = tab.siblings('.active_filters'),
            noResults = tab.siblings('.no_results'),
            numResults = $('tbody tr', tab).show().length,
            filters = activeFilters.data('filters_js');
        
        tab.show();
        noResults.hide();
        activeFilters.html('');
        
        for (var i=0; i < filters.length; i++) {
            key = filters[i].key;
            val = filters[i].val;
            nth_cell = tab.data('cols').indexOf(key) + 1;
            
            console.log(nth_cell);
            // Search terms are contained within zero-width word boundary delimeters.
            // Special regexp characters are escaped. Then, wildcards (*) and logical ORs (|) are interpreted.
            regex_search = new RegExp('\\b(' + val.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, "\\$&").replace(/\\\*/g, '.*').replace(/([\w\*])\\\|([\w\*])/g, '$1|$2')  + ')\\b', 'i');
            $('tbody tr:visible td:nth-child(' + nth_cell + ')', tab).each(function() {
                console.log(1);
                if (this.innerHTML.search(regex_search) == -1) {
                    var $tr = $(this).closest('tr').hide();
                    if ($tr.hasClass('selected'))
                        $tr.trigger('click');
                    numResults--;
                }
            });
            
            // Write the UI element for this filter.
            activeFilters.append('\
                <div class="filter" data-key="' + key + '" data-val="' + val.replace(/"/g, "\\$&") + '">\
                    <span class="field">' + key + ': </span>\
                    <span class="value">' + val.replace(/([\w\*])\|([\w\*])/g, '$1 <span class="logic_gate">OR</span> $2') + '</span> \
                    <a class="remove">&times;</a>\
                </div>\
            ');
        }
        
        if (numResults < 1) {
            tab.hide();
            noResults.show();
        }
    }
    
    $('form', cpanels).on('submit',function() {
        var fields = $('input[type="text"], select', this),
            filters_html = $(this).closest('.panel').find('.active_filters');
            filters_js = filters_html.data('filters_js');
            
        fields.each(function() {
            if (this.value.length > 0)
                filters_js.push({ key: $(this).data('link'), val: this.value });
        }).val('');
        
        filterTable(filters_html.siblings('.results'));
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
        filterTable(container.siblings('.results'));
    });
    
    var updateOutput = function() {
        var selected1 = $('#panel_1 .results').data('selected'),
            selected2 = $('#panel_2 .results').data('selected');
        
        if (selected1.length > 0 && selected2.length > 0)
            $('#panel_3 p').html('Selected: ' + selected1 + ' &times; ' + selected2);
        else 
            $('#panel_3 p').html('No combinations of rows are currently selected.');
    };
    $('.results').on('click', 'tbody tr', function() {
        var $this = $(this),
            tab = $this.closest('.results'),
            row_key = parseInt( $('td', this).eq( tab.data('pkey') ).html() ),
            selected = tab.data('selected');
        
        if (this.classList.contains('selected')) // classList.contains faster than jQuery hasClass or is('.class').
            selected.splice(selected.indexOf(row_key), 1);
        else
            selected.push( row_key );
        
        this.classList.toggle('selected');
        tab.data('selected', selected);
        
        updateOutput();
    });
    
    $('td.truncate', tables).on('wheel', function (e) {
        var scrollWindow = $(this).trigger('mouseout').closest('.results').get(0);
        if (typeof scrollWindow.scrollByLines !== 'undefined')
            scrollWindow.scrollByLines(e.originalEvent.deltaY);
        else
            scrollWindow.scrollTop += e.originalEvent.deltaY * 16;
    });
});