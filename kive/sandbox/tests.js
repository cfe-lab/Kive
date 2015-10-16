(function() {
    "use strict";
    
    describe("Pipeline Families table for sandbox", function() {
        beforeEach(function() {
            this.$table = $('<table>');
            this.$navigation_links = $("<div/>")
            this.is_user_admin = false;
            this.rows = [{
                name: "Example",
                members: [],
                users_allowed: [],
                groups_allowed: []
            }];
        });
        
        it('should build table', function() {
            var table = new choose_pipeline.PipelineFamiliesTable(
                    this.$table,
                    this.is_user_admin,
                    this.$navigation_links
            );
            table.drawThumbnails = function() {}; // disable AJAX call
            table.buildTable(this.rows);
            
            var $rows = this.$table.find('tr'),
                $cells = $rows.eq(1).find('td');
            
            expect($cells.length).toBe(7, 'cell count');
            expect($cells.eq(0).text()).toBe('Example');
            expect($cells.eq(1).html()).toContain('<canvas ');
            expect($cells.eq(6).html()).toBe('&nbsp;', 'hidden column for admin');
        });
        
        it('should build list of members', function() {
            this.rows[0].members = [{ id: 17, display_name: 'first' }];
            var table = new choose_pipeline.PipelineFamiliesTable(
                    this.$table,
                    this.is_user_admin);
            table.drawThumbnails = function() {};
            table.buildTable(this.rows);
            
            var $rows = this.$table.find('tr'),
                $cells = $rows.eq(1).find('td');
            
            expect($cells.eq(2).html()).toContain('first');
        });
        
        it('should select published version by default', function() {
            this.rows[0].members = [{ id: 23, display_name: 'second', published: false },
                                    { id: 17, display_name: 'first', published: true}];
            var table = new choose_pipeline.PipelineFamiliesTable(
                    this.$table,
                    this.is_user_admin);
            table.drawThumbnails = function() {};
            table.buildTable(this.rows);
            
            var $rows = this.$table.find('tr'),
                $cells = $rows.eq(1).find('td'),
                $select = $cells.eq(2).find('select');
            
            expect($select.find('option:selected').text()).toBe('first');
        });

        it('should select most recent published version by default', function() {
            this.rows[0].members = [{ id: 23, display_name: 'second', published: true },
                                    { id: 17, display_name: 'first', published: true}];
            var table = new choose_pipeline.PipelineFamiliesTable(
                    this.$table,
                    this.is_user_admin);
            table.drawThumbnails = function() {};
            table.buildTable(this.rows);

            var $rows = this.$table.find('tr'),
                $cells = $rows.eq(1).find('td'),
                $select = $cells.eq(2).find('select');

            expect($select.find('option:selected').text()).toBe('second');
        });
    });
    
    describe("Pipeline inputs table for sandbox", function() {
        beforeEach(function() {
            this.$table = $('<table>');
            this.$navigation_links = $("<div/>")
            this.is_user_admin = false;
            this.input_index = 1;
            this.compounddatatype_id = 17;
            this.rows = [{
                name: "some_dataset.csv",
                users_allowed: [],
                groups_allowed: []
            }];
        });
        
        it('should build table', function() {
            var table = new choose_inputs.DatasetsTable(
                    this.$table,
                    this.is_user_admin,
                    this.input_index,
                    this.compounddatatype_id,
                    undefined,
                    this.$navigation_links
            );
            table.buildTable(this.rows);
            
            var $rows = this.$table.find('tr'),
                $cells = $rows.eq(1).find('td');
            
            expect($cells.length).toBe(7, 'cell count');
            expect($cells.eq(0).text()).toBe('some_dataset.csv');
        });
        
        it('should respond to click on any field', function() {
            var table = new choose_inputs.DatasetsTable(
                    this.$table,
                    this.is_user_admin,
                    this.input_index,
                    this.compounddatatype_id);
            table.buildTable(this.rows);
            
            var $row = this.$table.find('tr').eq(1),
                $radio = $row.find('input');

            $row.click();
            expect($radio.is(':checked')).toBe(true, 'checked');
        });
    });
})();
