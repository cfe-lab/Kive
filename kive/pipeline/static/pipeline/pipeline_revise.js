$(function() {
    // Security stuff to prevent cross-site scripting.
    noXSS();

    // change pipeline revision drop-down triggers ajax to redraw canvas
    $('#id_pipeline_select').on('change', function () {
        var pipeline_id = $('#id_pipeline_select').val();
        window.pipeline_revision = new Pipeline(canvasState);

        if (pipeline_id === null) {
            window.submit_to_url = $('#id_family_pk').val();
            return;
        }
        
        var $canvas = $(canvasState.canvas);
        $canvas.fadeOut({ complete: function() {
            $.ajax({
                type: "GET",
                url: "/api/pipelines/" + pipeline_id + "/",
                datatype: "json",
                success: function(pipeline_raw) {

                    submit_to_url = pipeline_raw.family_pk;
                    pipeline_revision.load(pipeline_raw);
                    pipeline_revision.draw();
                    $canvas.fadeIn();

                    $('#id_publish').val(
                        pipeline_raw.is_published_version ?
                        'Cancel publication' :
                        'Make published version'
                    );
                }
            });
        }});
    }).change();

    $('#id_revert').on('click', function() {
        // Reload the current pipeline.
        $('#id_pipeline_select').change();
    });

    $('#id_update').on('click', function() {
        var pipeline = window.pipeline_revision,
            pipeline_id = pipeline.pipeline.id,
            steps = pipeline.canvasState.getSteps();
        
        for (var i = 0; i < steps.length; i++) {
            steps[i].updateSignal("update in progress");
        }
        pipeline.canvasState.valid = false;
        
        $.getJSON("/api/pipelines/" + pipeline_id + "/step_updates/").done(function(updates) {
            window.pipeline_revision.applyStepUpdates(updates);
        });
    });
    
    $('#id_publish').on('click', function() {
        if(pipeline_revision.isPublished()) {
            pipeline_revision.unpublish($("#id_family_pk").val(), function() {
                $('#id_publish').val('Make published version');
            });
        } else {
            pipeline_revision.publish($("#id_family_pk").val(), function() {
                $('#id_publish').val('Cancel publication');
            });
        }
    });
});