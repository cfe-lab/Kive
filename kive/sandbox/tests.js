(function() {
    "use strict";
    
    describe("Pipeline Families table for sandbox", function() {
        beforeEach(function() {
            this.$table = $('<table>');
            this.is_user_admin = false;
            this.rows = [{ name: "Example", members: [] }];
        });
        
        it('should build table', function() {
            new choose_pipeline.PipelineFamiliesTable(
                    this.$table,
                    this.is_user_admin,
                    this.rows);
            
            var $rows = this.$table.find('tr'),
                $cells = $rows.eq(1).find('td');
            
            expect($cells.length).toBe(4);
            expect($cells.eq(0).text()).toBe('Example');
            expect($cells.eq(1).html()).toContain('<canvas ');
            expect($cells.eq(3).html()).toBe('&nbsp;', 'hidden column for admin');
        });
        
        it('should build list of members', function() {
            this.rows[0].members = [{ id: 17, display: 'first' }];
            new choose_pipeline.PipelineFamiliesTable(
                    this.$table,
                    this.is_user_admin,
                    this.rows);
            
            var $rows = this.$table.find('tr'),
                $cells = $rows.eq(1).find('td');
            
            expect($cells.eq(2).html()).toContain('first');
        });
        
        it('should select published version by default', function() {
            this.rows[0].members = [{ id: 23, display: 'second' },
                                    { id: 17, display: 'first' }];
            this.rows[0].published_version = 17;
            new choose_pipeline.PipelineFamiliesTable(
                    this.$table,
                    this.is_user_admin,
                    this.rows);
            
            var $rows = this.$table.find('tr'),
                $cells = $rows.eq(1).find('td'),
                $select = $cells.eq(2).find('select');
            
            expect($select.find('option:selected').text()).toBe('first');
        });
    });
})();
