/*
 *
 * Includes:
 * /static/pipeline/drydock_objects.js
 * /static/pipeline/drydock.js
 * /static/pipeline/pipeline_add.js
 * /static/pipeline/pipeline_load.js
 */


function setupPipelineView(pipeline_dict) {
    var self = this;

    // Instance variables
    self.pipeline = new Pipeline(canvasState);
    self.pipeline.load(pipeline_dict);
    self.pipeline.draw();
}
