/**
 * Convert between API calls and CanvasState object.
 */
var pipeline = (function(exports){
    "use strict";

    var my = {};

    // Pipeline constructor
    my.Pipeline = function(canvasState){
        this.pipeline = null;
        this.canvasState = canvasState;
    };

    my.Pipeline.prototype.load = function(pipeline) {
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
        this.build_inputs();
        this.build_steps();
        this.build_outputs();
    };

    my.Pipeline.prototype.serialize = function(form_data) {
        /**
         * This method serializes the pipeline into an object that can be
         * fed to the backend REST API.
         *
         * See: /api/pipelines/
         *
         * @param form_data: starting data that all of the pipeline details
         * will be added to. Object format matches the JSON structure of the
         * API.
         */

        var self = this,
            pipeline_outputs = [],
            pipeline_inputs = [],
            canvas_x_ratio = 1.0/self.canvasState.canvas.width,
            canvas_y_ratio = 1.0/self.canvasState.canvas.height,
            is_trivial = true; // This is a trivial modification until we hit a non trivial
            // modification

        // TODO: Move check graph integrity out of Pipeline and into CanvasState

        // Check graph integrity
        $.each(self.canvasState.shapes, function(_, shape){
            if (shape instanceof drydock_objects.MethodNode) {
                var num_connections = 0;

                $.each(shape.out_magnets, function(_, magnet){
                    num_connections += magnet.connected.length;
                });

                if (num_connections === 0) {
                    throw 'MethodNode with unused outputs';
                }
            }
            else if (shape instanceof drydock_objects.OutputNode) {
                pipeline_outputs.push(shape);

            }
            else if (shape instanceof drydock_objects.CdtNode || shape instanceof drydock_objects.RawNode) {
                var magnet = null;
                pipeline_inputs.push(shape);

                // all CDtNodes or RawNodes (inputs) should feed into a MethodNode and have only one magnet
                if (shape.out_magnets.length != 1)
                    throw 'Invalid amount of magnets for output node!';

                // is this magnet connected?
                if (shape.out_magnets[0].connected.length === 0)
                    throw 'Unconnected input node';
            }
            else {
                throw 'Unknown node type encountered!';
            }
        });


        // sort pipelines by their isometric position, left-to-right, top-to-bottom
        // (sort of like reading order if you tilt your screen 30° clockwise)
        pipeline_inputs.sort(Geometry.isometricSort);
        pipeline_outputs.sort(Geometry.isometricSort);

        // at least one Connector must terminate as pipeline output
        if (pipeline_outputs.length === 0) {
            throw 'Pipeline has no output.';
        }

        // Now we're ready to start
        form_data = form_data || {};
        form_data.steps = [];
        form_data.inputs = [];
        form_data.outcables = [];

        // Construct the input updates
        $.each(pipeline_inputs, function(idx, input){
            var structure = null;

            // Setup the compound datatype
            if (input instanceof drydock_objects.CdtNode) {
                structure = {
                    compounddatatype: input.pk,
                    min_row: null,
                    max_row: null
                };
            }

            // Slap this input into the form data
            form_data.inputs[idx] = {
                structure: structure,
                dataset_name: input.label,
                dataset_idx: idx + 1,
                x: input.x * canvas_x_ratio,
                y: input.y * canvas_y_ratio,
            };
        });

        // MethodNodes are now sorted live, prior to pipeline submission —JN
        var sorted_elements = self.canvasState.getSteps();

        // Add arguments for input cabling
        $.each(sorted_elements, function(idx, step) {

            // TODO: Make this work for nested pipelines

            // Put the method in the form data
            form_data.steps[idx] = {
                transformation: step.pk,  // to retrieve Method
                transformation_type: "Method",
                step_num: idx + 1,  // 1-index (pipeline inputs are index 0)
                x: step.x * canvas_x_ratio,
                y: step.y * canvas_y_ratio,
                name: step.label,
                fill_colour: step.fill,
                cables_in: [],
                new_code_resource_revision_id: (
                        step.new_code_resource_revision ?
                                step.new_code_resource_revision.id :
                                    null),
                new_outputs_to_delete_names: step.outputs_to_delete
            };

            if (step.new_dependencies && step.new_dependencies.length) {
                var new_dependency_ids = [];
                for (var i = 0; i < step.new_dependencies.length; i++) {
                    new_dependency_ids.push(step.new_dependencies[i].id);
                }
                form_data.steps[idx].new_dependency_ids = new_dependency_ids;
            }

            // retrieve Connectors
            $.each(step.in_magnets, function(cable_idx, magnet) {
                if (magnet.connected.length === 0) {
                    return true; // continue;
                }

                var connector = magnet.connected[0],
                    source = magnet.connected[0].source.parent;

                form_data.steps[idx].cables_in[cable_idx] = {
                    source_dataset_name: connector.source.label,
                    dest_dataset_name: connector.dest.label,
                    source_step: source instanceof drydock_objects.MethodNode ? sorted_elements.indexOf(source)+1 : 0,
                    keep_output: false, // in the future this can be more flexible
                    custom_wires: [] // no wires for a raw cable
                };
            });
        });

        // Construct outputs
        $.each(pipeline_outputs, function(idx, output) {
            var connector = output.in_magnets[0].connected[0],
                source_step = connector.source.parent,
                structure = null;

            form_data.outcables[idx] = {
                output_name: output.label,
                output_idx: idx + 1,
                output_cdt: connector.source.cdt,
                source: source_step.pk,
                source_step: sorted_elements.indexOf(source_step) + 1, // 1-index
                source_dataset_name: connector.source.label, // magnet label
                x: output.x * canvas_x_ratio,
                y: output.y * canvas_y_ratio,
                custom_wires: [] // in the future we might have this
            };
        });

        // this code written on Signal Hill, St. John's, Newfoundland
        // May 2, 2014 - afyp

        // this code modified at my desk
        // June 18, 2014 -- RL

        // I code at my desk too.
        // July 30, 2014 - JN

        // How did I even computer?
        // April 28, 2048 - Cat

        return form_data;
    };

    my.Pipeline.prototype.update = function(runstat, look_for_md5, run_id)  {
        /**
         * Updates the progress of this pipeline with the status
         *
         * See: /api/runs/
         *
         * @param runstat: A dictionary containing the status
         *  components of a run.
         * @param look_for_md5: Marks a dataset if its md5 sum matches
         *  this input
         * @param run_id: Keeps track of the run id
         */
        var self = this;

        // Mark all the inputs as complete
        $.each(self.canvasState.shapes, function(_, shape){
            if ( shape instanceof drydock_objects.RawNode ||
                 shape instanceof drydock_objects.CdtNode) {

                shape.status = 'CLEAR';
            }
        });

        // Update each pipeline step
        $.each(runstat.step_progress, function(method_pk, step){
            var shape = self.canvasState.findMethodNode(parseInt(method_pk));

            if (shape instanceof drydock_objects.MethodNode) {
                shape.status = step.status;
                shape.log_id = step.log_id;
                shape.run_id = run_id;
            }
        });

        // Update the outputs
        $.each(runstat.output_progress, function(output_pk, output) {
            var shape = self.canvasState.findOutputNode(parseInt(output_pk));

            if (shape instanceof drydock_objects.OutputNode) {
                shape.status = output.status;
                shape.dataset_id = output.dataset_id;
                shape.md5 = output.md5;

                shape.run_id = run_id;
                shape.found_md5 = (shape.md5 === look_for_md5);
            }
        });

        // Update the inputs
        $.each(runstat.inputs, function(input_pk, output) {
            var shape = self.canvasState.findInputNode(parseInt(input_pk));

            if (shape instanceof drydock_objects.RawNode ||
                shape instanceof drydock_objects.CdtNode) {
                shape.dataset_id = output.dataset_id;
                shape.md5 = output.md5;

                shape.run_id = run_id;
                shape.found_md5 = (shape.md5 === look_for_md5);
            }
        });

        // Invalidate and force redraw
        self.canvasState.valid = false;
        self.canvasState.draw();
    };

    my.Pipeline.prototype.draw = function() {
        /**
         * Forces a redraw of this pipeline on its associated canvas
         */

        this.canvasState.testExecutionOrder();
        for (var i = 0; i < this.canvasState.shapes.length; i++) {
            this.canvasState.detectCollisions(this.canvasState.shapes[i], 0.5);
        }
        this.canvasState.draw();
    };


    // Private members
    my.Pipeline.prototype.build_inputs = function() {
        /**
         * Sets up the canvas state with the inputs for a pipeline
         */
        var self = this,
            canvas_x_ratio = this.canvasState.canvas.width / this.canvasState.scale,
            canvas_y_ratio = this.canvasState.canvas.height / this.canvasState.scale;

        if (self.pipeline === null) { throw "build_inputs() called with no pipeline?"; }

        // Over each input for the pipeline
        $.each(self.pipeline.inputs, function(_, node) {

            // Node has no structure => no CDT, so it's raw
            if (node.structure === null) {
                self.canvasState.addShape(new drydock_objects.RawNode(
                    node.x * canvas_x_ratio,
                    node.y * canvas_y_ratio,
                    node.dataset_name,
                    node.dataset_idx
                ));
            }
            else {
                self.canvasState.addShape(new drydock_objects.CdtNode(
                    node.structure.compounddatatype,
                    node.x * canvas_x_ratio,
                    node.y * canvas_y_ratio,
                    node.dataset_name,
                    node.dataset_idx
                ));
            }

            // TODO: Not sure why this is set to true? I'm sure it's important,
            // but I'd like an explaination
            self.canvasState.dragging = true;
        });
    };

    my.Pipeline.prototype.build_steps = function() {
        /**
         * Private method that sets up canvas state to draw methods
         */
        var self = this,
            canvas_x_ratio = self.canvasState.canvas.width / self.canvasState.scale,
            canvas_y_ratio = self.canvasState.canvas.height / self.canvasState.scale;

        if (self.pipeline === null) { throw "build_steps() called with no pipeline?"; }
        var method_node_offset = self.pipeline.inputs.length;

        // Over each pipeline step
        $.each(self.pipeline.steps, function() {
            var node = this,
                method_node = new drydock_objects.MethodNode(
                    node.transformation,
                    node.transformation_family,
                    node.x * canvas_x_ratio,
                    node.y * canvas_y_ratio,
                    node.fill_colour, // fill
                    node.name,
                    node.inputs,
                    node.outputs,
                    undefined, // status
                    node.outputs_to_delete
            );

            // Add `n draw
            self.canvasState.addShape(method_node);

            // Connect method inputs
            $.each(node.cables_in, function() {
                var cable = this,
                    source = null,
                    connector = null,
                    magnet = null;

                // Find the destination for this
                $.each(method_node.in_magnets, function() {
                    if (this.label === cable.dest_dataset_name) {
                        magnet = this;
                        return false; // break
                    }
                });
                
                // Not found?
                if (magnet === null) {
                    console.error("Failed to redraw Pipeline: missing in_magnet");
                    return false; // Bail
                }

                // cable from pipeline input, identified by dataset_name
                if (cable.source_step === 0) {

                    // Find the source for this
                    $.each(self.canvasState.shapes, function() {
                        if (!(this instanceof drydock_objects.MethodNode) &&
                              this.label === cable.source_dataset_name) {
                            source = this;
                            return false; // break
                        }
                    });

                    // Not found?
                    if (source === null) {
                        console.error("Failed to redraw Pipeline: missing data node");
                        return false; // Bail
                    }
                    
                    // data nodes only have one out-magnet, so use 0-index
                    connector = new drydock_objects.Connector(source.out_magnets[0]);

                    // connect other end of cable to the MethodNode
                    connector.x = magnet.x;
                    connector.y = magnet.y;
                    connector.dest = magnet;

                    source.out_magnets[0].connected.push(connector);
                    magnet.connected.push(connector);
                    self.canvasState.connectors.push(connector);
                }
                else {
                    // cable from another MethodNode

                    // this requires that pipeline_steps in JSON is sorted by step_num
                    source = self.canvasState.shapes[method_node_offset + cable.source_step - 1];

                    // find the correct out-magnet
                    $.each(source.out_magnets, function(){

                        if (this.label === cable.source_dataset_name) {
                            connector = new drydock_objects.Connector(this);
                            connector.x = magnet.x;
                            connector.y = magnet.y;
                            connector.dest = magnet;

                            this.connected.push(connector);
                            magnet.connected.push(connector);
                            self.canvasState.connectors.push(connector);
                            return false; // break;
                        }
                    });
                }
            });
        });
    };

    my.Pipeline.prototype.build_outputs = function () {
        /**
         * Private method that sets up canvas state to draw outputs
         */
        var self = this,
            canvas_x_ratio = self.canvasState.canvas.width / self.canvasState.scale,
            canvas_y_ratio = self.canvasState.canvas.height / self.canvasState.scale;

        if (self.pipeline === null) { throw "build_outputs() called with no pipeline?"; }

        var method_node_offset = self.pipeline.inputs.length;

        $.each(self.pipeline.outcables, function(_, this_output) {

            // identify source Method
            var source = self.canvasState.shapes[method_node_offset + this_output.source_step - 1];

            // Over each out magnet for that source
            $.each(source.out_magnets, function(j, magnet) {
                if (magnet.label === this_output.source_dataset_name) {
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

    my.Pipeline.prototype.applyStepUpdates = function(updates) {
        var pipeline = this,
            steps = pipeline.canvasState.getSteps(),
            updated_step_nums = updates.map(function(update) { return update.step_num - 1; });
        
        for (var i = 0; i < steps.length; i++) {
            if (updated_step_nums.indexOf(i) < 0) {
                steps[i].updateSignal('no update available');
            }
        }
        $.each(updates, function() {
            var update = this,
                old_method = steps[update.step_num - 1],
                new_method,
                any_mismatch;
            if ( ! update.method) {
                new_method = old_method;
                any_mismatch = false;
            }
            else {
                new_method = new drydock_objects.MethodNode(
                        update.method.id,
                        update.method.family_id,
                        0,// x
                        0,// y
                        old_method.fill,
                        old_method.label,
                        update.method.inputs,
                        update.method.outputs);
                any_mismatch = pipeline.canvasState.replaceMethod(
                        old_method,
                        new_method);
                new_method.updateSignal(
                        any_mismatch ?
                                'updated with issues'
                                : 'updated');
            }
            if (update.code_resource_revision) {
                new_method.new_code_resource_revision = update.code_resource_revision;
                new_method.updateSignal(any_mismatch ? 'updated with issues' : 'updated');
            }
            if (update.dependencies) {
                new_method.new_dependencies = update.dependencies;
                new_method.updateSignal(any_mismatch ? 'updated with issues' : 'updated');
            }
        });
        
        pipeline.canvasState.valid = false;
    };

    my.Pipeline.prototype.isPublished = function() {
        /**
         * Returns whether or not the pipeline has been published
         */
        return this.pipeline.is_published_version;
    };

    my.Pipeline.prototype.publish = function(family_pk, callback) {
        /**
         * Sets this pipeline as published
         *
         * @param family_pk: primary key of the family
         * TODO: ^^ This should be in the serializer maybe?
         * @param callback: Function called on success
         */
        var self = this;

        $.ajax({
            type: "PATCH",
            url: "/api/pipelinefamilies/"  + family_pk,
            data: { published_version: self.pipeline.id },
            datatype: "json",
            success: function(result){
                self.pipeline.is_published_version = true;
                callback(result);
            }
        });
    };

    my.Pipeline.prototype.unpublish =  function(family_pk, callback) {
        /**
         * Sets this pipeline as unpublished
         *
         * @param family_pk: primary key of the family
         * TODO: ^^ This should be in the serializer maybe?
         * @param callback: Function called on success
         */
        var self = this;

        $.ajax({
            type: "PATCH",
            url: "/api/pipelinefamilies/"  + family_pk,
            data: { published_version: null },
            datatype: "json",
            success: function(result){
                self.pipeline.is_published_version = false;
                callback(result);
            }
        });
    };

    return my;
})();

window.Pipeline = pipeline.Pipeline;
