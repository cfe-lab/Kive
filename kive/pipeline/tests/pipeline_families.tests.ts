import "jasmine";
import 'jasmine-html';
import 'jquery';
declare var pipeline_families: any;

"use strict";

xdescribe('Pipeline families', function() {
    beforeEach(function() {
        this.$table = $('<table/>');
        this.$navigation_links = $("<div/>");
        this.is_user_admin = false;
        this.initial_data = [{
            name: 'Example',
            num_revisions: 1,
            published_version_display_name: null,
            users_allowed: [],
            groups_allowed: []
        }];
    });
    
    it('should build a table', function() {
        var table = new pipeline_families.PipelineFamiliesTable(
            this.$table, this.is_user_admin, this.$navigation_links
        );
        table.image_path = "portal/static/portal/img";
        table.buildTable(this.initial_data);
        
        var $rows = this.$table.find('tr');
        expect($rows.length).toBe(2);
        var $cells = $rows.eq(1).find('td');
        expect($cells.eq(0).text()).toBe('Example'); // Name
        expect($cells.eq(3).text()).toBe('None'); // Published version
    });
    
    it('should display published version', function() {
        this.initial_data[0].published_version_display_name = "1: First";
        
        var table = new pipeline_families.PipelineFamiliesTable(
            this.$table, this.is_user_admin, this.$navigation_links
        );
        table.image_path = "portal/static/portal/img";
        table.buildTable(this.initial_data);
        
        var $rows = this.$table.find('tr');
        var $cells = $rows.eq(1).find('td');
        expect($cells.eq(3).text()).toBe('1: First'); // Published version
    });
});