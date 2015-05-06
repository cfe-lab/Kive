"use strict";

var permissions = (function() {
    var my = {};
    
    /** A base class for building a table with permissions columns.
     * 
     * Also includes a lock button for administrators to show all records and
     * links to remove or redact records.
     * 
     * Derived classes should override the lockHandler() method. For example:
     * 
     *  var BookTable = function($table, is_user_admin) {
     *      permissions.PermissionsTable.call(this, $table, is_user_admin);
     *      this.list_url = "api/books";
     *      this.registerColumn("Title", "title");
     *      this.registerColumn("Author", "author");
     *      this.registerColumn("Pub. Date", function($td, row) {
     *          $td.text(utils.formatDate(row.published));
     *      });
     *  }
     *  BookTable.prototype = Object.create(permissions.PermissionsTable.prototype);
     */
    my.PermissionsTable = function($table, is_user_admin) {
        this.$table = $table;
        this.is_user_admin = is_user_admin;
        this.is_locked = true;
        this.registered_columns = [];
    }
    
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
    }
    
    function defaultBuilder($td, row, field_name) {
        $td.text(row[field_name])
    }
    
    my.PermissionsTable.prototype.buildTable = function(rows) {
        var $tr,
            $a,
            permissions_table = this;
        
        if (this.$tbody !== undefined) {
            this.$tbody.empty();
        }
        else {
            $tr = $('<tr/>');
            this.registerColumn('Creator', 'user');
            this.registerColumn(
                    'Users with access',
                    buildListCell,
                    'users_allowed');
            this.registerColumn(
                    'Groups with access',
                    buildListCell,
                    'groups_allowed');
            this.buildHeaders($tr);
            this.$table.append($('<thead/>').append($tr));
            this.$tbody = $('<tbody/>');
            this.$table.append(this.$tbody);
        }
        this.$lockImage.attr(
                'src',
                this.is_locked
                ? '/static/portal/img/lock-locked-2x.png'
                : '/static/portal/img/lock-unlocked-2x.png');
        this.$lockSpan.text(this.is_locked ? '' : 'Administrator:');
        $.each(rows, function() {
            var $a,
                row = this;
            $tr = $('<tr/>');
            permissions_table.buildCells($tr, row);
            permissions_table.$tbody.append($tr);
        });
    }
    
    my.PermissionsTable.prototype.buildHeaders = function($tr) {
        this.buildPermissionHeaders($tr);
    }
    
    my.PermissionsTable.prototype.buildCells = function($tr, row) {
        this.buildPermissionCells($tr, row);
    }
    
    my.PermissionsTable.prototype.buildPermissionHeaders = function($tr) {
        var $a;
        $.each(this.registered_columns, function() {
            $tr.append($('<th/>').text(this.header));
        });
        this.$lockImage = $('<img/>');
        this.$lockSpan = $('<span/>');
        if (this.is_user_admin) {
            $a = ($('<a href="javascript:void(0)"/>')
                    .append(this.$lockImage)
                    .click(this, clickLock));

            $tr.append($('<th colspan="2"/>')
                    .addClass('lock')
                    .append($('<div/>').append($a, this.$lockSpan)));
        }
    }
    
    my.PermissionsTable.prototype.buildPermissionCells = function($tr, row) {
        var $td;
        $.each(this.registered_columns, function() {
            var $td = $('<td/>');
            this.builder($td, row, this.data);
            $tr.append($td);
        });
        if ( ! this.is_locked) {
            $td = $('<td/>');
            if (row.removal_plan !== undefined) {
                $tr.append($('<td/>').append($('<a/>')
                        .attr('plan', row.removal_plan)
                        .attr('href', row.url)
                        .text('Remove')
                        .click(this, clickRemove)));
            }
            if (row.redaction_plan !== undefined) {
                $tr.append($('<td/>').append($('<a/>')
                        .attr('plan', row.redaction_plan)
                        .attr('href', row.url)
                        .text('Redact')
                        .click(this, clickRedact)));
            }
            $tr.append($td);
        }
    }
    
    my.PermissionsTable.prototype.toggleLock = function() {
        this.is_locked = ! this.is_locked;
        this.reloadTable();
    }
    
    my.PermissionsTable.prototype.reloadTable = function() {
        var permissions_table = this;
        $.getJSON(
                this.list_url,
                { is_granted: this.is_locked },
                function (rows) {
                    permissions_table.buildTable(rows);
                });
    }
    
    function buildListCell($td, row, field_name) {
        var $ul = $('<ul/>'),
            names = row[field_name];
        $.each(names, function() {
            $ul.append($('<li/>').text(this));
        });
        $td.append($ul);
    }
    
    function clickLock(event) {
        event.preventDefault();
        var permissions_table = event.data;
        permissions_table.toggleLock();
    }

    function buildMessage(plan, action) {
        var message = "This will " + action + ":\n";
        for(var k in plan) {
            if (plan[k] != 0) {
                message += plan[k] + " " + k + "(s)\n";
            }
        }

        return message + "Are you sure?";
    }

    function clickRemove(event) {
        var $a = $(this),
            permissions_table = event.data;
        event.preventDefault();
        $.getJSON(
                $a.attr('plan'),
                {},
                function (plan) {
                    var message = buildMessage(plan, "remove");
                    if (window.confirm(message)) {
                        $.ajax({
                            url: $a.attr('href'),
                            method: 'DELETE',
                            success: function() {
                                permissions_table.reloadTable();
                            }
                        })
                    }
                });
    }

    function clickRedact(event){
        var $a = $(this),
            permissions_table = event.data;
        event.preventDefault();
        $.getJSON(
                $a.attr('plan'),
                {},
                function (plan) {
                    var message = buildMessage(plan, "redact");
                    if (window.confirm(message)) {
                        $.ajax({
                            url: $a.attr('href'),
                            method: 'PATCH',
                            data: {is_redacted: "true"},
                            success: function() {
                                permissions_table.reloadTable();
                            }
                        })
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
    }



    /** Create an administrator lock button. (Only for backward compatibility.)
     * 
     * @param $lock_div: jQuery object for the div that will hold the lock
     * button.
     * @param is_user_admin: Is the current user an administrator? If true,
     * will add the lock button to $lock_div, otherwise no change.
     * @param lock_handler: a function that gets called when the lock is
     * clicked. Takes a boolean value: is_admin.
     */
    my.AdminLock = function($lock_div, is_user_admin, lock_handler) {
        if ( ! is_user_admin) {
            $lock_div.hide();
            return;
        }
        
        this.is_admin = false;
        this.lock_handler = lock_handler;
        this.$image = $('<img/>');
        this.$span = $('<span/>');
        
        this.displayLock();
        $lock_div.append(
                $('<a/>').append(this.$image).click(this, toggleLock),
                this.$span)
    }
    
    my.AdminLock.prototype.displayLock = function() {
        this.$image.attr(
                'src',
                this.is_admin
                ? '/static/portal/img/lock-unlocked-2x.png'
                : '/static/portal/img/lock-locked-2x.png');
        this.$span.text(this.is_admin ? 'Administrator:' : '');
    }
    
    function toggleLock(event) {
        var adminLock = event.data;
        adminLock.is_admin = ! adminLock.is_admin;
        adminLock.displayLock();
        adminLock.lock_handler(adminLock.is_admin);
    }

    return my;
}());
    