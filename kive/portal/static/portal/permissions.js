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
     *      this.basic_headers = ["Title", "Author", "Publisher"];
     *      this.basic_fields = ["title", "author", "publisher"];
     *  }
     *  BookTable.prototype = Object.create(permissions.PermissionsTable.prototype);
     *  BookTable.prototype.lockHandler = function() {
     *      // might not need to override this.
     *      alert("Table is " + (this.is_locked ? "locked." : "unlocked."));
     *      
     *      reloadTable();
     *  }
     */
    my.PermissionsTable = function($table, is_user_admin) {
        this.$table = $table;
        this.is_user_admin = is_user_admin;
        this.is_locked = true;
        this.basic_headers = [];
        this.basic_fields = [];
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
            $.each(this.basic_headers.concat(
                    ['Creator', 'Users with access', 'Groups with Access']),
                    function() {
                $tr.append($('<th/>').text(this));
            });
            this.$lockImage = $('<img/>');
            this.$lockSpan = $('<span/>');
            if (this.is_user_admin) {
                $a = ($('<a href="javascript:void(0)"/>')
                        .append(this.$lockImage)
                        .click(this, clickLock));

                $tr.append($('<th/>').addClass('lock').append($a, this.$lockSpan));
            }
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
            $.each(permissions_table.basic_fields, function() {
                var field_name = this;
                $tr.append($('<td/>').text(row[field_name]));
            });
            $tr.append($('<td/>').text(row['user']));
            $tr.append(buildListCell(row['users_allowed']));
            $tr.append(buildListCell(row['groups_allowed']));
            if ( ! permissions_table.is_locked) {
                $a = ($('<a/>')
                        .attr('removal_plan', row['removal_plan'])
                        .attr('href', row['url'])
                        .text('Remove')
                        .click(permissions_table, clickRemove));
                $tr.append($('<td/>').append($a));
            }
            permissions_table.$tbody.append($tr);
        });
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
    
    function buildListCell(names) {
        var $ul = $('<ul/>');
        $.each(names, function() {
            $ul.append($('<li/>').text(this));
        });
        return $('<td/>').append($ul);
    }
    
    clickLock = function(event) {
        event.preventDefault();
        var permissions_table = event.data;
        permissions_table.toggleLock();
    }
    
    clickRemove = function(event) {
        var $a = $(this),
            permissions_table = event.data;
        event.preventDefault();
        $.getJSON(
                $a.attr('removal_plan'),
                {},
                function (plan) {
                    var message = 'Removing ';
                    for (var key in plan) {
                        var count = plan[key];
                        if (count > 0) {
                            message += count + ' ' + key + ', ';
                        }
                    }
                    message += 'are you sure?'
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
    
    toggleLock = function(event) {
        var adminLock = event.data;
        adminLock.is_admin = ! adminLock.is_admin;
        adminLock.displayLock();
        adminLock.lock_handler(adminLock.is_admin);
    }

    return my;
}());
    