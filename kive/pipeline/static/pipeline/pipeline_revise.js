$(function() {

    // change pipeline revision drop-down triggers ajax to redraw canvas
    $('#id_pipeline_select').on('change', function () {
        var pipeline_id = $('#id_pipeline_select').val();
        var pipeline = new Pipeline(canvasState);

        if (pipeline_id === null) {
            submit_to_url = $('#id_family_pk').val();
            return;
        }

//        $.ajax({
//            type: "GET",
//            url: "/api/pipelines/" + pipeline_id + "/",
//            datatype: "json",
//            success: function(pipeline_raw) {
//                pipeline.load(pipeline_raw);
//                pipeline.draw();
//
//                $('#id_publish').val(
//                    pipeline_raw['is_published_version']?
//                    'Cancel publication' :
//                    'Make published version'
//                );
//            }
//        });

        $.ajax({
            type: "POST",
            url: "/get_pipeline/",
            data: { pipeline_id: pipeline_id },
            datatype: "json",
            success: function(result) {
                // prepare to redraw canvas
                canvasState.reset();
                submit_to_url = result['family_pk'];
                var i, j, k; // counters
                var node, cables, cable, connector, shape, source, magnet;
                draw_pipeline(canvasState, result);
                canvasState.testExecutionOrder();

                for (var i = 0; i < canvasState.shapes.length; i++) {
                    canvasState.detectCollisions(canvasState.shapes[i], 0.5);
                }

                $('#id_publish').val(
                    result['is_published_version']?
                    'Cancel publication' :
                    'Make published version'
                );
            }
        });
    }).change();

    $('#id_revert').on('click', function() {
        // Reload the current pipeline.
        $('#id_pipeline_select').change();
    });

    $('#id_publish').on('click', function() {
        $.ajax({
            type: "POST",
            url: "/activate_pipeline/",
            data: { pipeline_id: $('#id_pipeline_select').val() },
            datatype: "json",
            success: function(result) {
                $('#id_publish').val(
                    result['is_published'] ?
                    'Cancel publication' : 'Make published version'
                );
            }
        });
    });
});