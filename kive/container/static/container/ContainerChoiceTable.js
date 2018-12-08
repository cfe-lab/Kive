(function(permissions) {//dependent on PermissionsTable class
	"use strict";
	permissions.ContainerChoiceTable = function($table, is_user_admin, $navigation_links) {
	    permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
	    this.list_url = "api/containerchoices/";
	    this.registerColumn("Name", "name");
	    this.registerColumn("Description", "description");
	    this.registerColumn("Containers", buildContainers);
        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
	};
	permissions.ContainerChoiceTable.prototype = Object.create(permissions.PermissionsTable.prototype);
})(permissions);

function buildContainers($td, row) {
    var $form = $('<form method="GET" action="container_inputs">'),
        $select = $('<select name="app">'),
        app,
		app_name,
		app_count = 0,
        i, j, container, $option;

	for (i in row.containers) {
		container = row.containers[i];
		for (j in container.apps) {
            app = container.apps[j];
			$option = $('<option>').attr('value', app.id);
			app_name = container.tag;
			if (app.name !== '') {
				app_name += ' / ' + app.name;
			}
			$option.text(app_name);
			if (app_count === 0) {
				$option.attr('selected', true);
			}
			$select.append($option);
			app_count += 1;
		}
	}
    if (app_count === 0) {
    	$td.append($('<p>No apps found.</p>'))
    } else {
		$form.append($select, '&nbsp;<input type="submit" value="Choose">');
		$td.append($form);
	}
}
