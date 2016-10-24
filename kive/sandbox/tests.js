(function() {
    "use strict";
    
    /* Move to async module tests */
    xdescribe("Pipeline Families table for sandbox", function() {
        beforeEach(function() {
            this.$table = $('<table>');
            this.$navigation_links = $("<div/>");
            this.is_user_admin = false;
            this.table = new permissions.PipelineRunTable(
                    this.$table,
                    this.is_user_admin,
                    this.$navigation_links
            );
            this.table.drawThumbnails = function() {}; // disable AJAX call
            this.table.image_path = "portal/static/portal/img";
            this.rows = [{
                name: "Example",
                members: [],
                users_allowed: [],
                groups_allowed: []
            }];
        });
        
        it('should build table', function() {
            this.table.buildTable(this.rows);
            
            var $rows = this.$table.find('tr'),
                $cells = $rows.eq(1).find('td');
            
            expect($cells.length).toBe(7, 'cell count');
            expect($cells.eq(0).text()).toBe('Example');
            expect($cells.eq(1).html()).toContain('<canvas ');
            expect($cells.eq(6).html()).toBe('&nbsp;', 'hidden column for admin');
        });
        
        it('should build list of members', function() {
            this.rows[0].members = [{ id: 17, display_name: 'first' }];
            this.table.buildTable(this.rows);
            
            var $rows = this.$table.find('tr'),
                $cells = $rows.eq(1).find('td');
            
            expect($cells.eq(2).html()).toContain('first');
        });
        
        it('should select published version by default', function() {
            this.rows[0].members = [{ id: 23, display_name: 'second', published: false },
                                    { id: 17, display_name: 'first', published: true}];
            this.table.buildTable(this.rows);
            
            var $rows = this.$table.find('tr'),
                $cells = $rows.eq(1).find('td'),
                $select = $cells.eq(2).find('select');
            
            expect($select.find('option:selected').text()).toBe('first');
        });

        it('should select most recent published version by default', function() {
            this.rows[0].members = [{ id: 23, display_name: 'second', published: true },
                                    { id: 17, display_name: 'first', published: true}];
            this.table.buildTable(this.rows);

            var $rows = this.$table.find('tr'),
                $cells = $rows.eq(1).find('td'),
                $select = $cells.eq(2).find('select');

            expect($select.find('option:selected').text()).toBe('second');
        });
    });
    
    describe("Pipeline inputs table for sandbox", function() {
        beforeEach(function() {
            this.$table = $('<table>');
            this.$navigation_links = $("<div/>");
            this.is_user_admin = false;
            this.input_index = 1;
            this.compounddatatype_id = 17;
            this.rows = [{
                name: "some_dataset.csv",
                has_data: true,
                users_allowed: [],
                groups_allowed: []
            }];
        });
        
        it('should build table', function() {
            var table = new permissions.DatasetsTable(
                    this.$table,
                    this.is_user_admin,
                    this.input_index,
                    this.compounddatatype_id,
                    undefined,
                    this.$navigation_links
            );
            table.image_path = "portal/static/portal/img";
            table.buildTable(this.rows);
            
            var $rows = this.$table.find('tr'),
                $cells = $rows.eq(1).find('td');
            
            expect($cells.length).toBe(7, 'cell count');
            expect($cells.eq(0).text()).toBe('some_dataset.csv');
        });
        
    });
})();
