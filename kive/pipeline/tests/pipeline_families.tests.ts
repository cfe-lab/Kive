"use strict";
(window as any).$ = $;
require("script-loader!../../portal/static/portal/permissions.js");
require("script-loader!../static/pipeline/PipelineFamiliesTable.js");
declare var permissions: { [key: string]: any };
let {PipelineFamiliesTable} = permissions;

describe('Pipeline families', function() {
    beforeEach(function() {
        this.$table = $('<table/>');
        this.$navigation_links = $("<div/>");
        this.is_user_admin = false;
        this.initial_data = [{
            name: 'Example',
            num_revisions: 1,
            published_version_display_name: null,
            user: 'John Doe',
            users_allowed: [ 'User 1', 'User 2' ],
            groups_allowed: [ 'Everyone' ]
        }];
    });

    it('should build a table', function() {
        var table = new PipelineFamiliesTable(
            this.$table, this.is_user_admin, this.$navigation_links
        );
        table.image_path = "portal/static/portal/img";
        table.buildTable(this.initial_data);

        var $rows = this.$table.find('tr');
        expect($rows.length).toBe(2);
        var $cells = $rows.eq(1).find('td');
        expect($cells.eq(0).text()).toBe('Example'); // Name
        expect($cells.eq(2).text()).toBe('1'); // User
        expect($cells.eq(3).text()).toBe('John Doe'); // User
        expect($cells.eq(4).text()).toBe('User 1User 2'); // Users allowed (actually a <ul>)
        expect($cells.eq(5).text()).toBe('Everyone'); // Groups allowed
    });
});