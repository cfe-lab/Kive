$(function() {
    // Security stuff to prevent cross-site scripting.
    noXSS();

    var render_pipeline = function() {
        var $canvas = $(canvasState.canvas);

        $original_pipeline = $("#initial_data").text();
        if ($original_pipeline === "") {
            return;
        }

        var pipeline_raw = JSON.parse($("#initial_data").text());
        window.pipeline_revision = new Pipeline(canvasState);

        $canvas.fadeOut(
            {
                complete: function () {
                    window.submit_to_url = pipeline_raw.family_pk;
                    window.pipeline_revision.load(pipeline_raw);
                    window.pipeline_revision.draw();
                    $canvas.fadeIn();
                }
            }
        );
    };

    $('#id_revert').on('click', function() {
        // Reload the current pipeline.
        render_pipeline();
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
        }).fail(function() {
            for (var i = 0; i < steps.length; i++) {
                steps[i].updateSignal("unavailable");
            }
            pipeline.canvasState.valid = false;
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

    render_pipeline();
});