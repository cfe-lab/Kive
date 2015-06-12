// place in global namespace to access from other files
//var submit_to_url = '/pipeline_add';

var pipeline = (function(exports){
    "use strict";

    var namespace = {};

    // Pipeline constructor
    var Pipeline = function(canvasState){
        this.pipeline = null;
        this.canvasState = canvasState;
    };

    Pipeline.prototype.load = function(pipeline) {
        /**
         * This method loads a pipeline and sets up the canvas to
         * draw the pipeline.
         *
         * See: /api/pipelines/
         *
         * @param pipeline: serialized pipeline array
         */
        this.pipeline = pipeline;
        this.canvasState.reset();
        this.draw_inputs();
        this.draw_steps();
        this.draw_outputs();
    }

    Pipeline.prototype.serialize = function(patch) {
        /**
         * This method serializes the pipeline into an object that can be
         * fed to the backend REST API.
         *
         * See: /api/pipelines/
         *
         * @param patch: (optional) If this is only a trivial update
         *  then use this to generate the patch data for the update.
         */
    };

    Pipeline.prototype.update = function(runstat, look_for_md5)  {
        /**
         * Updates the progress of this pipeline with the status
         *
         * See: /api/runs/
         *
         * @param status_dict: A dictionary containing the status
         *  components of a run.
         */
        var self = this;

        // Mark all the inputs as complete
        $.each(self.canvasState.shapes, function(_, shape){
            if(shape instanceof drydock_objects.RawNode ||
                    shape instanceof drydock_objects.CdtNode)
                shape.status = 'CLEAR';
        });

        // Update each pipeline step
        $.each(runstat.step_progress, function(method_pk, step){
            var shape = self.canvasState.findMethodNode(parseInt(method_pk));

            if(shape instanceof drydock_objects.MethodNode) {
                shape.status = step.status;
                shape.log_id = step.log_id;
                shape.rtp_id = step.rtp_id;
            }
        });

        // Update the outputs
        $.each(runstat.output_progress, function(output_pk, output) {
            var shape = self.canvasState.findOutputNode(parseInt(output_pk));

            if(shape instanceof drydock_objects.OutputNode) {
                shape.status = output.status;
                shape.dataset_id = output.dataset_id;
                shape.md5 = output.md5;

                shape.rtp_id = runstat.rtp_id;
                shape.found_md5 = (shape.md5 === look_for_md5);
            }
        });

        // Invalidate to force redraw
        self.canvasState.valid = false;
        self.canvasState.draw();
    };

    Pipeline.prototype.draw = function() {
        /**
         * Forces a redraw of this pipeline on its associated canvas
         */

        this.canvasState.testExecutionOrder();

        for (var i = 0; i < this.canvasState.shapes.length; i++)
          this.canvasState.detectCollisions(this.canvasState.shapes[i], 0.5);

        this.canvasState.draw();
    };


    // Private members
    Pipeline.prototype.draw_inputs = function() {
        /**
         * Sets up the canvas state with the inputs for a pipeline
         */
        var self = this,
            canvas_x_ratio = this.canvasState.canvas.width / this.canvasState.scale,
            canvas_y_ratio = this.canvasState.canvas.height / this.canvasState.scale;

        if(self.pipeline === null) throw "draw_inputs() called with no pipeline?";

        // Over each input for the pipeline
        $.each(self.pipeline.inputs, function(_, node) {

            // Node has no structure => no CDT, so it's raw
            if(node.structure === null)
                self.canvasState.addShape(new drydock_objects.RawNode(
                    node.x * canvas_x_ratio,
                    node.y * canvas_y_ratio,
                    node.dataset_name
                ));
            else
                self.canvasState.addShape(new drydock_objects.CdtNode(
                    node.structure.compounddatatype,
                    node.x * canvas_x_ratio,
                    node.y * canvas_y_ratio,
                    node.dataset_name
                ));

            // TODO: Not sure why this is set to true? I'm sure it's important,
            // but I'd like an explaination
            self.canvasState.dragging = true;
        });
    };

    Pipeline.prototype.draw_steps = function(canvasState, method_node_offset) {
        var self = this,
            canvas_x_ratio = this.canvasState.canvas.width / this.canvasState.scale,
            canvas_y_ratio = this.canvasState.canvas.height / this.canvasState.scale;

        if(self.pipeline === null) throw "draw_inputs() called with no pipeline?";
        var method_node_offset = self.pipeline.inputs.length;

        // Over each pipeline step
        $.each(self.pipeline.steps, function(_, node) {

            // START DIRTY HACK
            // Eventually the structure changes will be propogated into
            // MethodNode, but until then, the data are reformed to fit into the
            // old structure
            var inputs = {}, outputs = {};
            var input_lst = $.map(node.inputs, function(input){
                return {
                    datasetname: input.dataset_name,
                    cdt_pk: input.structure !== null ? input.structure.compounddatatype : null,
                    idx: input.dataset_idx
                };
            });

            var output_lst = $.map(node.outputs, function(output){
                return {
                    datasetname: output.dataset_name,
                    cdt_pk: output.structure !== null ? output.structure.compounddatatype : null,
                    idx: output.dataset_idx
                };
            });

            for(var i = 0; i < input_lst.length; i++) inputs[String(input_lst[i].idx)] = input_lst[i];
            for(var i = 0; i < output_lst.length; i++) outputs[String(output_lst[i].idx)] = output_lst[i];
            // END DIRTY HACK

            var method_node = new drydock_objects.MethodNode(
                    node.transformation,
                    node.transformation_family,
                    node.x * canvas_x_ratio,
                    node.y * canvas_y_ratio,
                    null, // fill
                    node.name,
                    inputs,
                    outputs
            );

            // Add `n draw
            self.canvasState.addShape(method_node);
            method_node.draw(self.canvasState.ctx);

            // Connect method inputs
            $.each(node.cables_in, function(cable_idx, cable) {
                var source = null,
                    connector = null,
                    magnet = null;

                // cable from pipeline input, identified by dataset_name
                if(cable.source_step == 0) {

                    // Find the source for this
                    $.each(self.canvasState.shapes, function(_, shape) {
                        // TODO: Check that this is correct, shouldn't we be using PKs?
                        if(!(shape instanceof drydock_objects.MethodNode) && shape.label === cable.source_dataset_name) {
                            source = shape;
                            return false; // break
                        }
                    });

                    // Not found?
                    if(source === null) {
                        console.error("Failed to redraw Pipeline: missing data node");
                        return false; // Bail
                    }

                    // data nodes only have one out-magnet, so use 0-index
                    connector = new drydock_objects.Connector(source.out_magnets[0]);

                    // connect other end of cable to the MethodNode
                    magnet = method_node.in_magnets[cable_idx];
                    connector.x = magnet.x;
                    connector.y = magnet.y;
                    connector.dest = magnet;

                    source.out_magnets[0].connected.push(connector);
                    method_node.in_magnets[cable_idx].connected.push(connector);
                    self.canvasState.connectors.push(connector);
                } else {
                    // cable from another MethodNode

                    // this requires that pipeline_steps in JSON is sorted by step_num
                    source = self.canvasState.shapes[method_node_offset + cable.source_step - 1];

                    // find the correct out-magnet
                    $.each(source.out_magnets, function(j, magnet){

                        if(magnet.label === cable.source_dataset_name) {
                            connector = new drydock_objects.Connector(magnet);
                            magnet = method_node.in_magnets[cable_idx];
                            connector.x = magnet.x;
                            connector.y = magnet.y;
                            connector.dest = magnet;

                            source.out_magnets[j].connected.push(connector);
                            method_node.in_magnets[cable_idx].connected.push(connector);
                            self.canvasState.connectors.push(connector);
                            return false; // break;
                        }
                    });
                }
            });
        });
    };

    Pipeline.prototype.draw_outputs = function (canvasState, pipeline, method_node_offset) {
        var self = this,
            canvas_x_ratio = this.canvasState.canvas.width / this.canvasState.scale,
            canvas_y_ratio = this.canvasState.canvas.height / this.canvasState.scale;

        if(self.pipeline === null) throw "draw_outputs() called with no pipeline?";

        var method_node_offset = self.pipeline.inputs.length;

        $.each(self.pipeline.outcables, function(_, this_output){

            // identify source Method
            var source = self.canvasState.shapes[method_node_offset + this_output.source_step - 1];

            // Over each out magnet for that source
            $.each(source.out_magnets, function(j, magnet) {
                if(magnet.label === this_output.source_dataset_name) {
                    var connector = new drydock_objects.Connector(magnet),
                        output = self.pipeline.outputs[this_output.output_idx - 1],
                        output_node = new drydock_objects.OutputNode(
                            output.x * canvas_x_ratio,
                            output.y * canvas_y_ratio,
                            this_output.output_name,
                            this_output.pk
                         );

                    self.canvasState.addShape(output_node);

                    connector.x = this_output.x * canvas_x_ratio;
                    connector.y = this_output.y * canvas_y_ratio;

                    connector.dest = output_node.in_magnets[0];
                    connector.dest.connected = [connector];  // bind cable to output node
                    connector.source = magnet;

                    source.out_magnets[j].connected.push(connector);  // bind cable to source Method
                    self.canvasState.connectors.push(connector);
                    return false; // break
                }
            });
        });
    };

    // Export to the global namespace
    var namespace = {
        Pipeline: Pipeline
    };

    return namespace;
})();

window.Pipeline = pipeline.Pipeline;
