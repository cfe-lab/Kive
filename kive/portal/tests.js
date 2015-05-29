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
    
    it("should have built a header and rows", function() {
        this.table.buildTable(this.examples);
        
        expect(this.$table.find('tr').length).toBe(3);
    });
});