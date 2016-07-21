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

    my.DatasetsTable = function(
            $table,
            is_user_admin,
            input_index,
            compounddatatype_id,
            $active_filters,
            $navigation_links
        ) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "/api/datasets/";
        this.input_index = input_index;

        this.compounddatatype_id = compounddatatype_id;
        var datasetsTable = this;
        this.filterSet = new permissions.FilterSet(
                $active_filters,
                function() {
                    datasetsTable.page = 1;
                    datasetsTable.reloadTable();
                });
        this.registerColumn("Name", buildName);
        this.registerColumn("Date", buildDateCreated);
        this.registerColumn("File Size (B)", buildFileSize);

        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");

        this.page_size = 8;

        this.$table.click(function(e) {
            var $target = $(e.target),
                $tr = $target.closest('tr');
            if ( ! $target.is('input')) {
                e.preventDefault();
                $tr.find('input').prop('checked', true);
            }
        });
    };
    my.DatasetsTable.prototype = Object.create(
            permissions.PermissionsTable.prototype);
    my.DatasetsTable.prototype.getQueryParams = function() {
        var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
        params.filters = this.filterSet.getFilters();
        params.filters.push({ key: "cdt", val: this.compounddatatype_id });
        params.page_size = 10;
        return params;
    };
 
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
            }
            else if (count === datasets.length) {
                caption = 'Showing all matching datasets.';
            }
            else {
                caption = 'Showing ' + datasets.length +
                        ' most recent matching datasets out of ' + 
                        count + '.';
            }
        }
        
        this.setCaption(caption);
        return datasets;
    };

    return my;
}());
