(function() {
    "use strict";

    describe("PermissionsTable", function() {
        beforeEach(function() {
            this.$table = $('<table/>');
            this.foo = 23;
            this.ExamplesTable = function($table, is_user_admin) {
                permissions.PermissionsTable.call(this, $table, is_user_admin);
                this.list_url = "/api/examples/";
                this.image_path = "portal/static/portal/img";
                this.registerColumn("Name", "name");
            };
            this.ExamplesTable.prototype = Object.create(
                    permissions.PermissionsTable.prototype);

            this.table = new this.ExamplesTable(this.$table, true);
            this.examples = [{name: "Jimmy"},
                             {name: "Bobby"}];
        });
    
        it("should build a header and rows", function() {
            this.table.buildTable(this.examples);
            
            expect(this.$table.find('tr').length).toBe(3);
        });
    });
    
    describe("FilterSet", function() {
        beforeEach(function() {
            this.$form = $('<form>');
            this.$active = $('<div>');
            this.changeCount = 0;
            var testCase = this,
                onChange = function() { testCase.changeCount++; };
            
            this.filterSet = new permissions.FilterSet(
                    this.$active,
                    onChange);
        });
        
        it("should start empty", function() {
            expect(this.$active.children().length).toBe(0);
            expect(this.changeCount).toBe(0);
        });
        
        it("should add a filter", function() {
            this.filterSet.add("name", "Bob");
            
            expect(this.$active.children().length).toBe(1);
            expect(this.$active.text()).toBe('name:Bob×', 'includes x to remove');
            expect(this.changeCount).toBe(1, 'change count');
        });
        
        it("should get a filter", function() {
            this.filterSet.add("name", "Bob");
            var filters = this.filterSet.getFilters();
            
            expect(filters).toEqual([{ key: "name", val: "Bob" }]);
        });
        
        it("should add multiple filters", function() {
            this.filterSet.add("name", "Bob");
            this.filterSet.add("age", "23");
            var filters = this.filterSet.getFilters();
            
            expect(filters).toEqual([{ key: "age", val: "23" },
                                     { key: "name", val: "Bob" }]);
            expect(this.changeCount).toBe(2, 'change count');
        });
        
        it("should add duplicate key", function() {
            this.filterSet.add("name", "Bob");
            this.filterSet.add("name", "Jim");
            var filters = this.filterSet.getFilters();
            
            expect(filters).toEqual([{ key: "name", val: "Jim" },
                                     { key: "name", val: "Bob" }]);
        });
        
        it("should ignore exact duplicate", function() {
            this.filterSet.add("name", "Bob");
            this.filterSet.add("name", "Bob");
            var filters = this.filterSet.getFilters();
            
            expect(filters).toEqual([{ key: "name", val: "Bob" }]);
            expect(this.changeCount).toBe(2, 'change count');
        });
        
        it("should have link to remove a filter", function() {
            this.filterSet.add("name", "Bob");
            var $remove = this.$active.find('.remove');
            
            expect($remove.length).toBe(1);
            $remove.click();
            
            var filters = this.filterSet.getFilters();
            
            expect(filters).toEqual([]);
            expect(this.changeCount).toBe(2, 'change count: add and remove');
        });
        
        describe("adding from a form", function() {
            beforeEach(function() {
                this.$form = $('<form>');
                this.$name = $('<input type="text" name="name">');
                this.$age = $('<input type="text" name="age">');
                this.$birthdate = $(
                        '<input type="text" name="bdate" class="datetime">');
                this.$active = $('<input type="checkbox" name="active">');
                this.$form.append(
                        this.$name,
                        this.$age,
                        this.$birthdate,
                        this.$active);
            });
            
            it("should add filters from a form", function() {
                this.$name.val('Bob');
                this.$age.val('23');
                this.filterSet.addFromForm(this.$form[0]);
                
                var filters = this.filterSet.getFilters();
                
                expect(filters).toEqual([{ key: "name", val: "Bob" },
                                         { key: "age", val: "23" }]);
                expect(this.changeCount).toBe(1, 'change count');
            });
            
            it("should add boolean filter from a checkbox", function() {
                this.$active.prop("checked", true);
                this.filterSet.addFromForm(this.$form[0]);
                
                var filters = this.filterSet.getFilters();
                
                expect(filters).toEqual([{ key: "active" }]);
            });
            
            it("should format date", function() {
                this.$birthdate.val('february 12 1956');
                this.filterSet.addFromForm(this.$form[0]);
                
                var filters = this.filterSet.getFilters();
                
                expect(filters).toEqual(
                        [{ key: "bdate", val: "12 Feb 1956 0:00" }]);
            });
            
            it("should add blank fields after adding", function() {
                this.$name.val('Bob');
                this.$active.prop("checked", true);
                this.filterSet.addFromForm(this.$form[0]);
                
                expect(this.$name.val()).toBe('');
                expect(this.$active.prop("checked")).toBe(false);
            });
        });
    });
})();