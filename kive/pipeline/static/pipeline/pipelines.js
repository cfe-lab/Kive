var pipelines = (function() {
    "use strict";
    var my = {};

    function pipeline_link($td, pipeline) {
        var $a = $("<a/>").attr("href", pipeline.view_url);
        if (pipeline.revision_name === '') {
            $a.text(pipeline.revision_number + ': ').append('<span class="placeholder">anonymous</span>');
        }
        else {
            $a.text(pipeline.display_name);
        }
        // revision_number revision_name
        $td.append($a);
    }

    function pipeline_revise_link($td, pipeline) {
        var $a = $("<a/>").attr("href", pipeline.absolute_url).text("Revise");
        $td.append($a);
    }
    
    function revision_desc($td, pipeline) {
        if (pipeline.revision_desc && pipeline.revision_desc !== '') {
            $td.append(pipeline.revision_desc);
        } else {
            $td.append('<span class="placeholder">(none)</span>');
        }
    }

    function published_version($td, pipeline, data) {
        var $form = $('<form class="publish">'),
            $button_group = $('<div class="button-group">'),
            $input = $('<input type="submit">'),
            $pipeline_pk_input = $('<input>', {
                type: 'hidden',
                name: "pipeline_pk",
                value: pipeline.id
            }),
            $action_input = $('<input type="hidden" name="action">'),
            family_pk = data.family_pk;
        
        $button_group.append($pipeline_pk_input, $action_input, $input);
        $form.append($button_group);

        if (pipeline.published) {
            $button_group.prepend('<input type="button" class="left-button" disabled value="Published">');
            $action_input.val("unpublish");
            $input.val("×").addClass('right-button close-button');
        } else {
            $action_input.val("publish");
            $input.val("Publish");
        }
        $form.submit(
            function(e) {
                e.preventDefault();

                var action = $action_input.val(),
                    revision = $pipeline_pk_input.val(),
                    published = true;

                if (action === "unpublish") {
                    published = false;
                }

                $.ajax({
                    type: "PATCH",
                    url: "/api/pipelines/"  + pipeline.id,
                    data: { published: published },
                    datatype: "json",
                    success: function(){
                        data.table.reloadTable();
                    }
                }).fail(function(data) {
                    $('.errortext').text('API error ' + data.status + ': ' + data.responseJSON.detail);
                });
            }
        );
        $td.append($form);
    }

    var PipelineTable = function($table, is_user_admin, family_pk, $active_filters, $navigation_links) {
        permissions.PermissionsTable.call(this, $table, is_user_admin, $navigation_links);
        this.list_url = "../../api/pipelines/";
        this.family_pk = family_pk;

        var plTable = this;
        this.filterSet = new permissions.FilterSet(
            $active_filters,
            function() {
                plTable.page = 1;
                plTable.reloadTable();
            }
        );
        // This adds a filter for the current CodeResource.
        var $pf_filter = this.filterSet.add("pipelinefamily_id", this.family_pk, true);
        $pf_filter.hide();

        this.registerColumn("Name", pipeline_link);
        this.registerColumn("", pipeline_revise_link);
        this.registerColumn("Description", revision_desc);
        this.registerColumn(
            "Published version",
            published_version,
            {
                family_pk: family_pk,
                table: this
            });

        this.registerStandardColumn("user");
        this.registerStandardColumn("users_allowed");
        this.registerStandardColumn("groups_allowed");
    };
    PipelineTable.prototype = Object.create(permissions.PermissionsTable.prototype);

    PipelineTable.prototype.getQueryParams = function() {
        var params = permissions.PermissionsTable.prototype.getQueryParams.call(this);
        params.filters = this.filterSet.getFilters();
        return params;
    };

    // Code that will be called on loading in the HTML document.
    my.main = function(is_user_admin, $table, family_pk, $active_filters, $navigation_links) {
        noXSS();

        $('.advanced-filter').prepend('<input type="button" class="close ctrl" value="Close">');

        $('input[value="Advanced"]').on('click', function() {
            $(this).closest('.short-filter').fadeOut({ complete: function() {
                $(this).siblings('.advanced-filter').fadeIn()
                    .closest('li').addClass('advanced');
            } });
        });

        $('.advanced-filter input.close.ctrl').on('click', function() {
            $(this).closest('.advanced-filter').fadeOut({ complete: function() {
                $(this).siblings('.short-filter').fadeIn()
                    .closest('li').removeClass('advanced');
            } });
        });

        $('form.short-filter, form.advanced-filter').submit(function(e) {
            e.preventDefault();
            table.filterSet.addFromForm(this);
        });

        var table = new PipelineTable($table, is_user_admin, family_pk, $active_filters, $navigation_links);
        table.reloadTable();
    };

    return my;
}());
