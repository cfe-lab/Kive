(function() {
    "use strict";

    describe("PermissionsTable", function() {
        beforeEach(function() {
            this.$table = $('<table/>');
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

        it("should use &nbsp; for null", function() {
            this.examples[0].name = null;
            this.table.buildTable(this.examples);

            expect(this.$table.find('tr').eq(1).find('td').text()).toBe('\xa0');
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
            
            expect(filters).toEqual(
                jasmine.arrayContaining([{ key: "age", val: "23" },
                                     { key: "name", val: "Bob" }])
            );
            expect(this.changeCount).toBe(2, 'change count');
        });
        
        it("should add duplicate key", function() {
            this.filterSet.add("name", "Bob");
            this.filterSet.add("name", "Jim");
            var filters = this.filterSet.getFilters();
            
            expect(filters).toEqual(
                jasmine.arrayContaining([{ key: "name", val: "Jim" },
                                     { key: "name", val: "Bob" }])
            );
        });
        
        it("should ignore exact duplicate", function() {
            this.filterSet.add("name", "Bob");
            this.filterSet.add("name", "Bob");
            var filters = this.filterSet.getFilters();
            
            expect(filters).toEqual([{ key: "name", val: "Bob" }]);
            expect(this.changeCount).toBe(2, 'change count');
        });

        it("should add a date filter", function() {
            this.filterSet.addDate("created", new Date("2001-03-15 0:00"));
            var filters = this.filterSet.getFilters();

            expect(filters).toEqual([{ key: "created", val: "15 Mar 2001 0:00" }]);
        });

        it("should add a date filter with offsets", function() {
            var yearsOffset = 1,
                monthsOffset = 2,
                daysOffset = -3;
            this.filterSet.addDate(
                "created",
                new Date("2001-03-15 0:00"),
                yearsOffset,
                monthsOffset,
                daysOffset);
            var filters = this.filterSet.getFilters();

            expect(filters).toEqual([{ key: "created", val: "12 May 2002 0:00" }]);
        });

        it("should stick to the end of the month", function() {
            var yearsOffset = 0,
                monthsOffset = 1,
                daysOffset = 0;
            this.filterSet.addDate(
                "created",
                new Date("2001-01-31 0:00"),
                yearsOffset,
                monthsOffset,
                daysOffset);
            var filters = this.filterSet.getFilters();

            expect(filters).toEqual([{ key: "created", val: "28 Feb 2001 0:00" }]);
        });

        it("should wrap around the year end", function() {
            var yearsOffset = 0,
                monthsOffset = -1,
                daysOffset = 0;
            this.filterSet.addDate(
                "created",
                new Date("2001-01-31 0:00"),
                yearsOffset,
                monthsOffset,
                daysOffset);
            var filters = this.filterSet.getFilters();

            expect(filters).toEqual([{ key: "created", val: "31 Dec 2000 0:00" }]);
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
                
                expect(filters).toEqual(
                    jasmine.arrayContaining([{ key: "name", val: "Bob" },
                                         { key: "age", val: "23" }])
                );
                expect(this.changeCount).toBe(1, 'change count');
            });
            
            it("should set filters from pairs", function() {
                this.filterSet.setFromPairs('name=Bob&age=23');
                
                var filters = this.filterSet.getFilters();
                
                expect(filters).toEqual(
                    jasmine.arrayContaining([{ key: "name", val: "Bob" },
                                         { key: "age", val: "23" }])
                );
                expect(this.changeCount).toBe(1, 'change count');
            });
            
            it("should set filters from pairs and replace existing", function() {
                this.filterSet.add('name', 'Tom');
                this.filterSet.setFromPairs('name=Bob&age=23');
                
                var filters = this.filterSet.getFilters();
                
                expect(filters).toEqual(
                    jasmine.arrayContaining([{ key: "name", val: "Bob" },
                                         { key: "age", val: "23" }])
                );
                expect(this.changeCount).toBe(2, 'change count');
            });
            
            it("should set filters from empty pairs string", function() {
                this.filterSet.setFromPairs('');
                
                var filters = this.filterSet.getFilters();
                
                expect(filters).toEqual([]);
                expect(this.changeCount).toBe(1, 'change count');
            });
            
            it("should set filters from null pairs string", function() {
                this.filterSet.setFromPairs(null);
                
                var filters = this.filterSet.getFilters();
                
                expect(filters).toEqual([]);
                expect(this.changeCount).toBe(1, 'change count');
            });
            
            it("should build pairs from filters", function() {
                this.filterSet.add('age', '23');
                this.filterSet.add('name', 'Bob');
                
                var pairs = this.filterSet.getPairs();
                
                expect(['name=Bob&age=23', 'age=23&name=Bob']).toContain(pairs);
            });
            
            it("should build empty pairs string", function() {
                var pairs = this.filterSet.getPairs();
                
                expect(pairs).toEqual('');
            });
            
            it("should handle special characters in filter pairs", function() {
                this.filterSet.add('comment', '1=2');
                this.filterSet.add('name', 'Tom & Jerry');
                var pairs = this.filterSet.getPairs();
                this.filterSet.setFromPairs(pairs);
                
                var filters = this.filterSet.getFilters();
                
                expect(filters).toEqual(
                    jasmine.arrayContaining([{ key: "name", val: "Tom & Jerry" },
                                         { key: "comment", val: "1=2" }])
                );
            });
            
            it("should add boolean filter from a checkbox", function() {
                this.$active.prop("checked", true);
                this.filterSet.addFromForm(this.$form[0]);
                
                var filters = this.filterSet.getFilters();
                
                expect(filters).toEqual([{ key: "active" }]);
            });
            
            it("should handle boolean filter in filter pairs", function() {
                this.filterSet.add('active');
                var pairs = this.filterSet.getPairs();
                this.filterSet.setFromPairs(pairs);
                
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