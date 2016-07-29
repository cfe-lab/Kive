var choose_inputs = (function() {
    "use strict";
    var my = {};
    
    function buildName($td, row) {
        var inner_div = $('<div>').text(row.name);
        $td.append(inner_div).addClass('primary').data('id', row.id);
    }
    function buildDateCreated($td, row) {
        $td.text(permissions.formatDate(row.date_created)).addClass('date');
    }

    function buildFileSize($td, dataset) {
        var el = $('<em>');
        if (dataset.has_data) {
            el = dataset.filesize_display;
        } else if (dataset.is_redacted) {
            el.text('redacted');
        } else {
            el.text('missing');
        }
        $td.append(el);
    }

    my.DatasetsTable = function($table, is_user_admin, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "/api/datasets/";

        this.registerColumn("Name", buildName);
        this.registerColumn("Date", buildDateCreated);
        this.registerColumn("File Size (B)", buildFileSize);
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");

        this.page_size = 8;
    };
    my.DatasetsTable.prototype = Object.create(permissions.PermissionsTable.prototype);
 
    my.DatasetsTable.prototype.extractRows = function(response) {
        var datasets = [],
            caption,
            count;
        if (response.detail !== undefined) {
            caption = response.detail;
        } else {
            datasets = response.results;
            count = response.count;
            if (count === 0) {
                caption = 'No datasets match your query.';
            } else if (count === datasets.length) {
                caption = 'Showing all matching datasets.';
            } else {
                caption = 'Showing ' + datasets.length +
                        ' most recent matching datasets out of ' + 
                        count + '.';
            }
        }
        
        this.setCaption(caption);
        return datasets;
    };
    my.DatasetsTable.prototype.getMaxYPosition = function() {
        return window.innerHeight;
    };
    my.DatasetsTable.prototype.checkOverflow = function() {
        var dataset_table = this,
            available_space = this.getMaxYPosition() -
                this.$table.offset().top - this.$table.outerHeight() + 10,
            rows = this.$table.find("tbody tr"),
            row_height = rows.eq(0).outerHeight(),
            rows_over, new_pg_size, current_item_number, new_pg;

        if (!row_height) {
            row_height = this.$table.find("tfoot tr").outerHeight() || 10;
        }

        if (available_space && rows.length) {
            rows_over = Math.ceil(- available_space / row_height);
            new_pg_size = rows.length - rows_over;

            if (new_pg_size !== this.page_size) {
                current_item_number = this.page_size * (this.page - 1);
                new_pg = Math.floor(current_item_number / new_pg_size) + 1;

                if (new_pg_size < 1) {
                    rows.remove();
                    if (showPageError) {
                        showPageError("There's no room in this window to show search results.", '.results-table-error');
                    }
                } else {
                    $('.results-table-error').hide();
                    this.page = new_pg;
                    // check if a table reload is needed
                    if (new_pg_size < this.page_size) {
                        // check if a table reload is already in progress
                        if (this.ajax_request === undefined) {
                            // prune rows without doing a table reload
                            rows.slice(new_pg_size).remove();
                            this.page_size = new_pg_size;
                        } else {
                            // if a table reload is in progress, check overflow again when it's complete.
                            this.ajax_request.done(function() {
                                dataset_table.checkOverflow();
                            });
                        }
                    } else {
                        this.page_size = new_pg_size;
                        this.reloadTable();
                    }
                } 
            }
        }
    };

    return my;
}());
