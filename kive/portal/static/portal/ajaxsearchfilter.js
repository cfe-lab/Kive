/**
 * AjaxSearchFilter
 * @requires permissions classes (PermissionsTable and FilterSet)
 */
var AjaxSearchFilter = (function(permissions) {
	var table, asf;

	var init = function(_table, $asf) {
		if ($asf === undefined) {
			$asf = 'body';
		}
		this.search_field = $('.asf-search-field', $asf);
		this.search_filters = $('.search_filters, .asf-search-filters', $asf);
		this.active_filters = $('.asf-active-filters', $asf);
		this.main_search = $('.asf-main-search', $asf);
		this.form = $('.asf-form', $asf);
		this.advanced_form = $('.asf-advanced-form', $asf);
		this.search_button = $('.asf-search-button', this.main_search);

    	table = _table;
    	var use_session_storage = table.hasOwnProperty("session_filters_key") && 
    			table.hasOwnProperty("session_page_key");
    	asf = this;
	    this.activateEvents();

	    if (table.ajax_request !== undefined) {
	    	table.ajax_request.complete(trackTableWidth);
	    }

        table.filterSet = new permissions.FilterSet(
            this.active_filters,
            function() {
                table.page = 1;
                table.reloadTable();
                if (use_session_storage) {
	                sessionStorage.setItem(
	                    table.session_filters_key,
	                    table.filterSet.getPairs()
	                );
                }
            }
        );
        table.getQueryParams = function() {
            var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
            params.filters = this.filterSet.getFilters();
            if (use_session_storage) {
			    sessionStorage.setItem(table.session_page_key, table.page);
            }
            return params;
        };
    };
    init.prototype.activateEvents = function() {
	    $('body')          .on( 'keydown', filterKeyHandler );
	    $(window)          .on( 'resize', trackTableWidth );
	    this.form          .on( 'submit', submitDatasetSearch );
	    this.advanced_form .on( 'submit', submitDatasetSearch );
	    this.search_field  .on( 'keydown', searchInputKeyHandler );
	    this.search_field  .on( 'focus', unfocusFilters );
	    this.active_filters.on( 'click', '.filter', focusOnThis );
	    this.main_search   .on( 'click', focusSearchField );
	    $('.advanced.ctrl').on( 'click', toggleAdvancedSearch );
	    this.advanced_form.find('.close.ctrl').on( 'click', toggleAdvancedSearch );
    };
    init.prototype.deactivateEvents = function() {
	    $('body')          .off( 'keydown', filterKeyHandler );
	    $(window)          .off( 'resize', trackTableWidth );
	    this.form          .off( 'submit', submitDatasetSearch );
	    this.advanced_form .off( 'submit', submitDatasetSearch );
	    this.search_field  .off( 'keydown', searchInputKeyHandler );
	    this.search_field  .off( 'focus', unfocusFilters );
	    this.active_filters.off( 'click', '.filter', focusOnThis );
	    this.main_search   .off( 'click', focusSearchField );
	    $('.advanced.ctrl').off( 'click', toggleAdvancedSearch );
	    this.advanced_form.find('.close.ctrl').off( 'click', toggleAdvancedSearch );
    };
    init.prototype.reloadTable = function() {
    	table.reloadTable();
	    if (table.ajax_request !== undefined) {
	    	table.ajax_request.complete(trackTableWidth);
	    }
    };

    function trackTableWidth() {
	    asf.main_search.add(asf.advanced_form)
	    	.css('width', table.$table.outerWidth());
    }
    function toggleAdvancedSearch() {
    	var destination, goingTo;
    	asf.form.toggle();
    	asf.advanced_form.toggle();

    	destination = asf.main_search;
    	goingTo = "prependTo";
    	if (asf.main_search.has(asf.active_filters).length > 0) {
    		destination = $(asf.advanced_form);
    		goingTo = "appendTo";
    	}
    	asf.active_filters.detach()
    		[goingTo](destination);
    }
    function submitDatasetSearch(e) {
        e.preventDefault();
        table.filterSet.addFromForm(this);
	    if (table.ajax_request !== undefined) {
	    	table.ajax_request.complete(trackTableWidth);
	    }
	};
    function focusSearchField(e) {
        // prevent this event from bubbling
        if ( $(e.target).is(asf.main_search) ) {
            asf.search_field.focus();
        }
    };
    function focusOnThis() {
    	$(this).addClass('focus').siblings().removeClass('focus');
    };
    function unfocusFilters() {
    	$('.filter.focus').removeClass('focus');
    };
	function filterKeyHandler(e) {
        var focus_filter = $('.filter.focus'),
            filter_key = focus_filter.data('key'),
            filter_value = focus_filter.find('.value').text() || undefined,
            right_key = e.keyCode === 39,
            left_key = e.keyCode === 37,
            backspace = e.keyCode === 8,
            del = e.keyCode === 46,
            esc = e.keyCode === 27,
            tab = e.keyCode === 9,
            meaningful_key_pressed = right_key || left_key || backspace || del || esc || tab,
            next_el, prev_el
        ;

        if (focus_filter.length > 1) {
            focus_filter.not(focus_filter.eq(0)).removeClass('focus');
            focus_filter = focus_filter.eq(0);
        }
        if (focus_filter.length && meaningful_key_pressed) {
            focus_filter.removeClass('focus');
            if (left_key || backspace) {
                prev_el = focus_filter.prev();
                (prev_el.length ? prev_el : focus_filter).addClass('focus');
            } else if (right_key || del || tab) {
                next_el = focus_filter.next();
                if (!tab && next_el.length) {
                    next_el.addClass('focus');
                } else {
                    asf.search_field.focus();
                    e.preventDefault();
                }
            }
            if (backspace || del) {
                table.filterSet.remove(filter_key, filter_value);
			    if (table.ajax_request !== undefined) {
			    	table.ajax_request.complete(trackTableWidth);
			    }
                asf.search_filters.find('input, select').each(function() {
                    var $this = $(this);
                    if ($this.data('filter-name') == filter_key) {
                        $this.val("");
                        return false;
                    }
                });
                e.stopPropagation();
                e.preventDefault();
            }
        }
    };
    function searchInputKeyHandler(e) {
        var last_filter;
        if (this.selectionStart === 0) {
            last_filter = asf.active_filters.find('.filter:last-child');
            if (e.keyCode === 37) {// left
                $(this).blur();
                last_filter.addClass('focus');
            } else if (e.keyCode === 8) {// backspace
                table.filterSet.remove(
                    last_filter.data('key'),
                    last_filter.find('.value').text() || undefined
                );
			    if (table.ajax_request !== undefined) {
			    	table.ajax_request.complete(trackTableWidth);
			    }
            }
        }
        e.stopPropagation();
    };


    return init;
})(permissions);