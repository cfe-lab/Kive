var permissions = (function() {
    "use strict";
    var my = {};
    
    /** A base class for building a table with permissions columns.
     * 
     * Also includes a lock button for administrators to show all records and
     * links to remove or redact records.
     * 
     * Derived classes should register their own columns. For example:
     * 
     *  var BookTable = function($table, is_user_admin) {
     *      permissions.PermissionsTable.call(this, $table, is_user_admin);
     *      this.list_url = "api/books";
     *      this.registerColumn("Title", "title");
     *      this.registerColumn("Author", "author");
     *      this.registerColumn("Pub. Date", function($td, row) {
     *          $td.text(permissions.formatDate(row.published));
     *      });
     *  }
     *  BookTable.prototype = Object.create(permissions.PermissionsTable.prototype);
     *  BookTable.prototype.constructor = BookTable; // http://stackoverflow.com/a/8454111/4794
     *  
     *  Special columns that derived classes may include in the data rows are:
     *  
     *  user: the owner of the row
     *  users_allowed: user names that have been granted access
     *  groups_allowed: group names that have been granted access
     *  removal_plan: a URL to retrieve a removal plan summary
     *  redaction_plan: a URL to retrieve a redaction plan summary
     *  url: if removal_plan or redaction_plan are present, this is used for
     *      DELETE or PATCH requests.
     *  
     *  Derived classes can also use this.setReloadInterval to make the table
     *  continuously poll the server.
     */
    my.PermissionsTable = function($table, is_user_admin, $navigation_links) {
        this.$table = $table;
        this.is_user_admin = is_user_admin;
        this.is_locked = true;
        this.image_path = '/static/portal/img';
        this.registered_columns = [];
        this.$lockImage = $('<img/>');
        this.$lockSpan = $('<span/>');

        this.page_size = 25;
        this.page = 1;

        if ($navigation_links) {
            this.$navigation_links = $navigation_links;
            this.$navigation_links.on('click', '.prev.link', prevLink.bind(this));
            this.$navigation_links.on('click', '.next.link', nextLink.bind(this));
            this.$navigation_links.append(
                $('<span class="record_count"/>'),
                $('<a class="nav prev">prev</a>'),
                $('<span class="page_num"/>'),
                $('<a class="nav next">next</a>')
            );
        }
    };
    
    /**
     * Register a column to be added to the table.
     * 
     *  @param header: a string to label the column header
     *  @param source: Either a field name, or a function to build a cell in
     *      the column. Function signature must be f($td, row, data).
     *  @param data: optional data object to pass to the source function
     */
    my.PermissionsTable.prototype.registerColumn = function(
            header,
            source,
            data) {
        var column = { header: header, builder: source, data: data };
        if (typeof column.builder !== 'function') {
            column.builder = defaultBuilder;
            column.data = source;
        }
        this.registered_columns.push(column);
    };

    /**
     * Sets the table to refresh per given interval.
     *
     * @param interval: the interval at which to poll in ms
     */
    my.PermissionsTable.prototype.setReloadInterval = function(interval) {
        // Schedule reload if it's been requested.
        if (interval !== undefined) {
            this.timeout_id = setInterval(this.reloadTable.bind(this), interval);
        }
    };

    /**
     * Stops the reload cycle if present.
     */
    my.PermissionsTable.prototype.clearReloadInterval = function() {
        if (this.timeout_id || this.timeout_id === 0) {
            clearInterval(this.timeout_id);
        }
        this.timeout_id = undefined;
    };

    /**
     * Register one of three standard columns to be added to the table.
     *
     *  @param columnName: "user", "users_allowed", or "groups_allowed"
     */
    my.PermissionsTable.prototype.registerStandardColumn = function(columnName) {
        if (columnName === "user") {
            this.registerColumn('Creator', 'user');
        }
        else if (columnName === "users_allowed") {
            this.registerColumn('Users with access', buildListCell, columnName);
        }
        else if (columnName === "groups_allowed") {
            this.registerColumn('Groups with access', buildListCell, columnName);
        }
    };

    /**
     * Register a column to be added to the table with a formatted date/time.
     *
     *  @param header: a string to label the column header
     *  @param columnName: the name of the column to format
     */
    my.PermissionsTable.prototype.registerDateTimeColumn = function(
            header,
            columnName) {
        this.registerColumn(header, buildDateTimeCell, columnName);
    };

    /**
     * Register a column which is a label with a link.
     *
     *  @param header: a string to label the column header
     *  @param base_url: the first part of the URL of the link
     *  @param label_field: the property of the item which provides the label
     *  @param url_field: the property of the item which provides the rest of the URL
     */
    my.PermissionsTable.prototype.registerLinkColumn = function(
            header,
            base_url,
            label_field,
            url_field) {
        label_field = label_field || "name";
        url_field = url_field || "id";
        this.registerColumn(header, function($td, item) {
            $('<a>').text(item[label_field])
                .attr('href', base_url + item[url_field])
                .appendTo($td);
        });
    };
    
    /**
     * Attach some error messages to a cell in the table.
     * 
     * @param $td: a cell in the table, wrapped in a jQuery object
     * @param errors: an array of strings
     */
    my.PermissionsTable.prototype.setErrors = function($td, errors) {
        if (errors === undefined || errors.length === 0) {
            return;
        }
        var errorList = $('<ul/>');
        $.each(errors, function() {
            errorList.append($('<li/>').text(this));
        });
        $td.addClass('with-error').append(
                $('<div class="error-tip"/>').append(errorList));
        $td.hover(function() {
            $(this).find('.error-tip').show();
        },
        function(){
            $(this).find('.error-tip').hide();
        });
    };
    
    function defaultBuilder($td, row, field_name) {
        if (row[field_name] !== null) {
            $td.text(row[field_name]);
        }
    }
    
    my.PermissionsTable.prototype.buildTable = function(rows) {
        var $tr,
            permissions_table = this,
            $rows = [],
            lock_icon = this.image_path +
                (this.is_locked ? '/lock-locked-2x.png' : '/lock-unlocked-2x.png');

        if (this.$thead === undefined) {
            $tr = $('<tr/>');
            this.buildHeaders($tr);
            this.$thead = $('<thead/>').append($tr);
            this.$table.append(this.$thead);
        }

        if (this.$tbody !== undefined) {
            this.$tbody.empty();
        }
        else if (rows.length > 0) {
            this.$tbody = $('<tbody/>');
            this.$table.append(this.$tbody);
        }
        this.$lockImage.attr('src', lock_icon);
        this.$lockSpan.text(this.is_locked ? '' : 'Administrator');

        $.each(rows, function() {
            var row = this;
            $tr = $('<tr/>');
            permissions_table.buildCells($tr, row);
            $rows.push($tr);
        });
        permissions_table.$tbody.append($rows);
    };
    
    my.PermissionsTable.prototype.buildHeaders = function($tr) {
        this.buildPermissionHeaders($tr);
    };
    
    my.PermissionsTable.prototype.buildCells = function($tr, row) {
        this.buildPermissionCells($tr, row);
    };
    
    my.PermissionsTable.prototype.buildPermissionHeaders = function($tr) {
        var $a;
        $.each(this.registered_columns, function() {
            $tr.append($('<th/>').text(this.header));
        });
        if (this.is_user_admin) {
            $a = ($('<a href="javascript:void(0)"/>')
                    .append(this.$lockImage, this.$lockSpan)
                    .click(this, clickLock));

            $tr.append($('<th colspan="2"/>')
                    .addClass('lock')
                    .append($('<div/>').append($a)));
        }
    };
    
    my.PermissionsTable.prototype.buildPermissionCells = function($tr, row) {
        var cells = [];
        $.each(this.registered_columns, function() {
            var $td = $('<td/>');
            this.builder($td, row, this.data);
            cells.push($td);
        });
        if ( ! this.is_locked) {
            if (row.removal_plan !== undefined && row.removal_plan !== null) {
                cells.push($('<td/>').append($('<a/>')
                        .attr('plan', row.removal_plan)
                        .attr('href', row.url)
                        .text('Remove')
                        .click(this, clickRemove)));
            }
            if (row.redaction_plan !== undefined && row.redaction_plan !== null) {
                cells.push($('<td/>').append($('<a/>')
                        .attr('plan', row.redaction_plan)
                        .attr('href', row.url)
                        .text('Redact')
                        .click(this, clickRedact)));
            }
        }
        cells.push($('<td>&nbsp;</td>'));
        $tr.append(cells);
    };
    
    my.PermissionsTable.prototype.toggleLock = function() {
        this.is_locked = ! this.is_locked;
        this.reloadTable();
    };

    function navLink(event) {// called with bind(), apply(), or call()
        event.preventDefault();
        // At this point, the page number has already been incremented/decremented by
        // prevLink or nextLink.
        this.reloadTable();
    }

    function prevLink(event) {// called with bind(), apply(), or call()
        this.page = this.page - 1;
        navLink.call(this, event);
    }

    function nextLink(event) {// called with bind(), apply(), or call()
        this.page = this.page + 1;
        navLink.call(this, event);
    }

    function handleTableUpdate(callback, response) {// called with bind(), apply(), or call()
        var rows = this.extractRows(response);
        if (this.$navigation_links !== undefined) {
            var $nav_links = this.$navigation_links;
            var $page_num = $nav_links.children('.page_num');
            var $prev = $nav_links.children('.nav.prev');
            var $next = $nav_links.children('.nav.next');

            $nav_links.children('.record_count').text(response.count + ' found');
            if (response.count > this.page_size) {
                var first_row = this.page_size * (this.page - 1) + 1;
                var last_row = Math.min(
                    this.page_size * this.page,
                    response.count
                );
                $page_num.text("Page " + this.page)
                    .attr("title", first_row + " to " + last_row);

                if ("previous" in response && response.previous !== null) {
                    $prev.addClass('link').removeClass('nolink');
                } else {
                    $prev.addClass('nolink').removeClass('link');
                }
                if ("next" in response && response.next !== null) {
                    $next.addClass('link').removeClass('nolink');
                } else {
                    $next.addClass('nolink').removeClass('link');
                }
            }
        }
        this.buildTable(rows);
        this.setCaption("");
        if (typeof callback === 'function') {
            callback(this);
        }
    }

    function handleTableFail(request) {// called with bind(), apply(), or call()
        var response = request.responseJSON,
            detail = (
                response ?
                    response.detail :
                    "Failed to reload table");
        if (this.$tbody !== undefined) {
            this.$tbody.empty();
        }
        this.setCaption(detail);
    }
    
    my.PermissionsTable.prototype.reloadTable = function(callback) {
        var permissions_table = this;
        if (permissions_table.ajax_request !== undefined) {
            permissions_table.ajax_request.abort();
            permissions_table.ajax_request = undefined;
        }
        var query_params = permissions_table.getQueryParams();
        query_params.page_size = permissions_table.page_size;
        query_params.page = permissions_table.page;

        permissions_table.ajax_request = $.getJSON(permissions_table.list_url, query_params)
            .done(handleTableUpdate.bind(permissions_table, callback))
            .fail(handleTableFail.bind(permissions_table))
            .always(function() {
                permissions_table.ajax_request = undefined;
            });
    };
    
    my.PermissionsTable.prototype.setCaption = function(text) {
        var $caption = this.$table.find('caption');
        if ( ! $caption.length) {
            $caption = $('<caption/>');
            this.$table.append($caption);
        }
        $caption.text(text);
    };

    /** Get query parameters from the current state of the table and page.
     * 
     */
    my.PermissionsTable.prototype.getQueryParams = function() {
        return { is_granted: this.is_locked };
    };
    
    /** Extract row data from an AJAX response.
     * @param response: the JSON object returned from the server
     */
    my.PermissionsTable.prototype.extractRows = function(response) {
        if ("count" in response) {
            return response.results;
        }
        return response;
    };
    
    /** Choose which redaction field to set when redacting.
     * @param plan_url: the redaction plan URL that this field should enact
     */
    my.PermissionsTable.prototype.getRedactionField = function(plan_url) {
        return "is_redacted";
    };
    
    function buildListCell($td, row, field_name) {
        var $ul = $('<ul/>').addClass('cell-list'),
            names = row[field_name];
        $.each(names, function() {
            $ul.append($('<li/>').text(this));
        });
        $td.append($ul);
    }
    
    function buildDateTimeCell($td, row, field_name) {
        $td.text(my.formatDate(row[field_name]));
    }
    
    function clickLock(event) {
        event.preventDefault();
        var permissions_table = event.data;
        permissions_table.toggleLock();
    }

    my.PermissionsTable.prototype.buildConfirmationMessage = function(
            plan,
            action) {
        // We handle the Datasets and ExternalFiles entries differently.
        var datasets = plan.Datasets,
            external_files = plan.ExternalFiles;

        if (datasets === undefined) {
            datasets = 0;
        }
        if (external_files === undefined) {
            external_files = 0;
        }
        var internal_files = datasets - external_files;

        var message = "This will " + action + ":\n";
        if (internal_files !== 0) {
            message += internal_files + " Datasets\n";
        }
        if (external_files !== 0) {
            message += external_files + " Datasets with external files (external files will not be removed)\n";
        }

        for(var k in plan) {
            if (k !== "ExternalFiles" && k !== "Datasets" && plan[k] !== 0) {
                message += plan[k] + " " + k + "\n";
            }
        }

        return message + "Are you sure?";
    };

    function clickRemove(event) {
        // jQuery sets `this` for event handlers
        //jshint validthis: true
        var $a = $(this),
            permissions_table = event.data;
        event.preventDefault();
        event.stopPropagation();
        $.getJSON(
                $a.attr('plan'),
                {},
                function (plan) {
                    var message = permissions_table.buildConfirmationMessage(
                            plan,
                            "remove");
                    if (window.confirm(message)) {
                        $.ajax({
                            url: $a.attr('href'),
                            method: 'DELETE',
                            success: function() {
                                permissions_table.reloadTable();
                            }
                        }).fail(
                            function (request) {
                                var response = request.responseJSON,
                                    detail = (
                                            response ?
                                            response.detail :
                                            "Failed to remove");
                                window.alert(detail);
                            }
                        );
                    }
                }).fail(function (request) {
                    window.alert('Failed to build removal plan: ' +
                            request.statusText);
                });
    }

    function clickRedact(event){
        // jQuery sets `this` for event handlers
        //jshint validthis: true
        var $a = $(this),
            permissions_table = event.data,
            plan_url = $a.attr('plan');
        event.preventDefault();
        $.getJSON(
            plan_url,
            {},
            function (plan) {
                var message = permissions_table.buildConfirmationMessage(
                        plan,
                        "redact"),
                    redaction_field = permissions_table.getRedactionField(
                            plan_url),
                    redaction_data = {};
                if (window.confirm(message)) {
                    redaction_data[redaction_field] = true;
                    $.ajax({
                        url: $a.attr('href'),
                        method: 'PATCH',
                        data: redaction_data,
                        success: function() {
                            permissions_table.reloadTable();
                        }
                    }).fail(
                        function (request) {
                            var response = request.responseJSON,
                                detail = (
                                        response ?
                                        response.detail :
                                        "Failed to redact");
                            window.alert(detail);
                        }
                    );
                }
            });
    }
    
    my.formatDate = function(text) {
        var monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
            date = new Date(text),
            min = date.getMinutes();
        if (Number.isNaN(min)) {
            return null;
        }
        if (min < 10) {
            min = "0" + min;
        }
        return (date.getDate() + " " + monthNames[date.getMonth()] + " " + 
                date.getFullYear() + " " + date.getHours() + ":" + min);
    };
    
    /**
     * Build and apply a set of search filters.
     * 
     * @param $active: a jQuery object that will hold all the active filters
     * @param onChange: a function that is called when a filter is added or
     *  removed
     */
    my.FilterSet = function($active, onChange) {
        this.$active = $active;
        this.onChange = onChange;
    };
    
    function addFilter(filterSet, key, value) {
        var $filter,
            $duplicates;
        $filter = $('<div class="filter"/>').data({ key: key, val: value });

        if (key != "smart") {
            $filter.append($('<span class="field"/>').text(key + ':'));
        }
        $filter.append($('<span class="value"/>').text(value));
        $duplicates = $('div.filter', filterSet.$active).filter(function() {
            var $f = $(this);
            return $f.data('key') === key && $f.data('val') === value;
        });
        if ( ! $duplicates.length) {
            $filter.append($('<a class="remove">&times;</a>').click(function() {
                $filter.detach();
                filterSet.onChange();
            }));
            filterSet.$active.append($filter);
        }

        return $filter;
    }

    my.FilterSet.prototype.remove = function(key, value, skip_trigger) {
        var $filters = this.$active.children();
        if (typeof value == 'undefined') {
            $filters.filter(function() {
                return $(this).data('key') == key;
            }).detach();
        } else {
            $filters.filter(function() {
                return $(this).data('key') == key && $(this).data('val') == value;
            }).detach();
        }

        if (skip_trigger === undefined) {
            this.onChange();
        }
    };
    
    my.FilterSet.prototype.add = function(key, value, skip_trigger) {
        var $filter = addFilter(this, key, value);
        if (skip_trigger === undefined) {
            this.onChange();
        }
        return $filter;
    };

    my.FilterSet.prototype.addDate = function(
            key,
            value,
            yearsOffset,
            monthsOffset,
            daysOffset,
            skip_trigger) {
        yearsOffset = yearsOffset || 0;
        monthsOffset = monthsOffset || 0;
        daysOffset = daysOffset || 0;
        var newMonth = value.getMonth() + monthsOffset,
            expectedMonth = (((newMonth % 12) + 12) % 12);
        value.setFullYear(value.getFullYear() + yearsOffset);
        value.setMonth(newMonth);
        value.setDate(value.getDate() + daysOffset);
        if (daysOffset === 0 && value.getMonth() !== expectedMonth) {
            // We wrapped around into the next month.
            value.setDate(0);
        }
        var textValue = my.formatDate(value);
        return this.add(key, textValue, skip_trigger);
    };

    my.FilterSet.prototype.addFromForm = function(form) {
        var filterSet = this,
            $fields = $('input[type="text"], input:checked', form);
        $fields = $($fields.get().reverse());
        $fields.each(function() {
            var $field = $(this),
                value = $field.val();
            if (value.length === 0) {
                return;
            }
            if ($field.is('.datetime')) {
                value = my.formatDate(value);
                if (value === undefined || value === null) {
                    return;
                }
            }
            addFilter(
                    filterSet,
                    $field.attr('name'),
                    $field.is(':checked') ? undefined : value);
            if ($field.is(':checked')) {
                $field.attr('checked', false);
            }
            else {
                $field.val('');
            }
        });
        this.onChange();
    };

    /* Set search filters from key-value pairs in a string.
     * 
     * Example: "key1=value1&key2=value2"
     * Keys and values are URI encoded.
     */
    my.FilterSet.prototype.setFromPairs = function(pairs) {
        var filterSet = this,
            pairsArray,
            pair,
            value;
        pairsArray = pairs === null || pairs.length === 0 ? [] : pairs.split('&');
        this.$active.find('.filter').remove();
        for (var i = pairsArray.length-1; i >= 0; i--) {
            pair = pairsArray[i].split('=');
            value = pair[1] === undefined ? undefined : decodeURIComponent(pair[1]);
            addFilter(
                    filterSet,
                    decodeURIComponent(pair[0]),
                    value);
        }
        this.onChange();
    };
    
    /* Get key-value pairs in a string for all search filters.
     * 
     * Example: "key1=value1&key2=value2"
     * Keys and values are URI encoded.
     */
    my.FilterSet.prototype.getPairs = function() {
        var filters = this.getFilters(),
            pairs = '';
        for (var i = 0; i < filters.length; i++) {
            var filter = filters[i];
            if (pairs.length) {
                pairs += '&';
            }
            pairs += encodeURIComponent(filter.key);
            if (filter.val !== undefined) {
                pairs += '=' + encodeURIComponent(filter.val);
            }
        }
        return pairs;
    };
    
    my.FilterSet.prototype.getFilters = function() {
        var filters = [];
        this.$active.find('.filter').each(function() {
            filters.push($(this).data());
        });
        return filters;
    };

    return my;
}());
