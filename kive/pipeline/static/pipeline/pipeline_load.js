// place in global namespace to access from other files
var submit_to_url = '/pipeline_add';

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
            if(shape instanceof RawNode || shape instanceof CDtNode)
                shape.status = 'CLEAR';
        });

        // Update each pipeline step
        $.each(runstat.step_progress, function(method_pk, step){
            var shape = self.canvasState.findMethodNode(parseInt(method_pk));

            if(shape instanceof MethodNode) {
                shape.status = step.status;
                shape.log_id = step.log_id;
                shape.rtp_id = step.rtp_id;
            }
        });

        // Update the outputs
        $.each(runstat.output_progress, function(output_pk, output) {
            var shape = self.canvasState.findOutputNode(parseInt(output_pk));

            if(shape instanceof OutputNode) {
                shape.status = output.status;
                shape.dataset_id = output.dataset_id;
                shape.md5 = output.md5;

                shape.rtp_id = runstat.rtp_id;
                shape.found_md5 = (shape.md5 === look_for_md5);
            }
        });

        // Invalidate to force redraw
        self.canvasState = false;
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
                self.canvasState.addShape(new RawNode(
                    node.x * canvas_x_ratio,
                    node.y * canvas_y_ratio,
                    node.dataset_name
                ));
            else
                self.canvasState.addShape(new CDtNode(
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

            var method_node = new MethodNode(
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
                        if(!(shape instanceof MethodNode) && shape.label === cable.source_dataset_name) {
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
                    connector = new Connector(source.out_magnets[0]);

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

                        // TODO: Should we be using PKs here?
                        if(magnet.label === cable.source_dataset_name) {
                            connector = new Connector(magnet);
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
            $.each(source.out_magnets, function(_, magnet) {
                if(magnet.label === this_output.source_dataset_name) {
                    var connector = new Connector(magnet),
                        output = self.pipeline.outputs[this_output.output_idx - 1],
                        output_node = new OutputNode(
                            output.x * canvas_x_ratio,
                            output.y * canvas_y_ratio,
                            this_output.output_name,
                            this_output.id
                         );

                    self.canvasState.addShape(output_node);

                    connector.x = this_output.x * canvas_x_ratio;
                    connector.y = this_output.y * canvas_y_ratio;

                    connector.dest = output_node.in_magnets[0];
                    connector.dest.connected = [connector];  // bind cable to output node
                    connector.source = magnet;

                    magnet.connected.push(connector);  // bind cable to source Method
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

    if( typeof exports !== "undefined" && exports !== null)
        window[exports] = namespace;

    return namespace;
})();

window.Pipeline = pipeline.Pipeline;

function draw_pipeline(canvasState, pipeline) {
    draw_inputs(canvasState, pipeline);
    draw_steps(canvasState, pipeline, pipeline.pipeline_inputs.length);
    draw_outputs(canvasState, pipeline, pipeline.pipeline_inputs.length);
}

// Draw pipeline inputs on the canvas.
function draw_inputs(canvasState, pipeline) {
    var pipeline_inputs = pipeline.pipeline_inputs, // Array[]
        node, i;
    for (i = 0; i < pipeline_inputs.length; i++) {
        node = pipeline_inputs[i];
        if (node.CDT_pk === null) {
            canvasState.addShape(new RawNode(
                node.x * canvasState.canvas.width / canvasState.scale, 
                node.y * canvasState.canvas.height / canvasState.scale, 
                node.dataset_name
            ));
        } else {
            canvasState.addShape(new CDtNode(
                node.CDT_pk, 
                node.x * canvasState.canvas.width / canvasState.scale, 
                node.y * canvasState.canvas.height / canvasState.scale, 
                node.dataset_name
            ));
        }
        canvasState.dragging = true;
        //canvasState.selection.push(canvasState.shapes[canvasState.shapes.length-1]);
//        canvasState.doUp();
    }
}

// Draw pipeline steps on the canvas.
function draw_steps(canvasState, pipeline, method_node_offset) {
    var pipeline_steps = pipeline.pipeline_steps,
        node, inputs, outputs, method_node, i, j, k, cables, cable, source;

    for (i = 0; i < pipeline_steps.length; i++) {
        node = pipeline_steps[i];
        inputs = pipeline_steps[i].inputs;
        outputs = pipeline_steps[i].outputs;
        method_node = new MethodNode(
            node.transf_pk,
            node.family_pk,
            node.x * canvasState.canvas.width / canvasState.scale,
            node.y * canvasState.canvas.height / canvasState.scale,
            null,// fill
            node.name,
            inputs, 
            outputs
        );

        canvasState.addShape(method_node);
        method_node.draw(canvasState.ctx);  // to update Magnet x and y

        // connect Method inputs
        cables = node.cables_in;
        for (j = 0; j < cables.length; j++) {
            cable = cables[j];
            if (cable.source_step == 0) {
                // cable from pipeline input, identified by dataset_name
                source = null;
                for (k = 0; k < canvasState.shapes.length; k++) {
                    shape = canvasState.shapes[k];
                    if (!(shape instanceof MethodNode) && shape.label === cable.source_dataset_name) {
                        source = shape;
                        break;
                    }
                }
                if (source === null) {
                    console.error("Failed to redraw Pipeline: missing data node");
                    return;
                }

                // data nodes only have one out-magnet, so use 0-index
                connector = new Connector(source.out_magnets[0]);

                // connect other end of cable to the MethodNode
                magnet = method_node.in_magnets[j];
                connector.x = magnet.x;
                connector.y = magnet.y;
                connector.dest = magnet;

                source.out_magnets[0].connected.push(connector);
                method_node.in_magnets[j].connected.push(connector);
                canvasState.connectors.push(connector);
            } else {
                // cable from another MethodNode

                // this requires that pipeline_steps in JSON is sorted by step_num
                //  (adjust for 0-index)
                source = canvasState.shapes[method_node_offset + cable.source_step - 1];

                // find the correct out-magnet
                for (k = 0; k < source.out_magnets.length; k++) {
                    magnet = source.out_magnets[k];
                    if (magnet.label === cable.source_dataset_name) {
                        connector = new Connector(magnet);
                        magnet = method_node.in_magnets[j];
                        connector.x = magnet.x;
                        connector.y = magnet.y;
                        connector.dest = magnet;
                        
                        source.out_magnets[k].connected.push(connector);
                        method_node.in_magnets[j].connected.push(connector);
                        canvasState.connectors.push(connector);
                        break;
                    }
                }
            }
        }
        // done connecting input cables
    }
}

// Draw pipeline outputs on the canvas.
function draw_outputs(canvasState, pipeline, method_node_offset) {
    var pipeline_outputs = pipeline.pipeline_outputs,
        i, k, this_output, source, magnet, connector, output_node;

    for (i = 0; i < pipeline_outputs.length; i++) {
        this_output = pipeline_outputs[i];

        // identify source Method
        source = canvasState.shapes[method_node_offset + this_output.source_step - 1];

        // find the correct out-magnet
        for (k = 0; k < source.out_magnets.length; k++) {
            magnet = source.out_magnets[k];
            if (magnet.label === this_output.source_dataset_name) {
                connector = new Connector(magnet);
                output_node = new OutputNode(
                    this_output.x * canvasState.canvas.width / canvasState.scale,
                    this_output.y * canvasState.canvas.height / canvasState.scale,
                    this_output.output_name,
                    this_output.id
                 );

                canvasState.addShape(output_node);
                
                connector.x = this_output.x * canvasState.canvas.width / canvasState.scale;
                connector.y = this_output.y * canvasState.canvas.height / canvasState.scale;

                connector.dest = output_node.in_magnets[0];
                connector.dest.connected = [ connector ];  // bind cable to output node
                connector.source = magnet;

                magnet.connected.push(connector);  // bind cable to source Method
                canvasState.connectors.push(connector);
                break;
            }
        }
    }
}

function update_status(canvasState, run, look_for_md5) {
    var pipeline_steps = run.step_progress,
        outputs = run.output_progress,
        i, shape, method_pk, output_pk;

    // Set all the inputs as complete
    for(i = 0; i < canvasState.shapes.length; i++){
        shape = canvasState.shapes[i];

        // Raw nodes and CDt nodes are by definition inputs
        if (shape instanceof RawNode || shape instanceof CDtNode) {
            shape.status = 'CLEAR'; // TODO: Replace with proper status
        }
    }

    // Update each pipeline step
    for (method_pk in pipeline_steps) if (pipeline_steps.propertyIsEnumerable(method_pk)) {
        shape = canvasState.findMethodNode(method_pk);
        if (shape instanceof MethodNode) {
            shape.status = pipeline_steps[method_pk].status;
            shape.log_id = pipeline_steps[method_pk].log_id;
            shape.rtp_id = run.rtp_id;
        }
    }

    // Update all outputs
    for (output_pk in outputs) if (outputs.propertyIsEnumerable(output_pk)) {
        shape = canvasState.findOutputNode(output_pk);
        if (shape instanceof OutputNode) {
            shape.status = outputs[output_pk].status;
            shape.md5 = outputs[output_pk].md5;
            shape.dataset_id = outputs[output_pk].dataset_id;
            shape.rtp_id = run.rtp_id;

            if (shape.md5 === look_for_md5) {
                shape.found_md5 = true;
            }
        }
    }

    // Invalidate to force a redraw
    canvasState.valid = false;
}
