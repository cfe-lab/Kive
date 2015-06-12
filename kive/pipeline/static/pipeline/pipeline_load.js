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

        var self = this,
            method_nodes = [],
            pipeline_outputs = [],
            pipeline_inputs = [],
            canvas_x_ratio = 1./self.canvasState.canvas.width,
            canvas_y_ratio = 1./self.canvasState.canvas.height;


        // TODO: Move check graph integrity out of Pipeline and into CanvasState

        // Check graph integrity
        $.each(self.canvasState.shapes, function(_, shape){
            if (shape instanceof MethodNode) {
                var num_connections = 0;

                // Track all method nodes
                method_nodes.push(shape);
                $.each(shape.out_magnets, function(_, magnet){
                    num_connections += magnet.connected.length;
                });

                if (num_connections === 0)
                    throw 'MethodNode with unused outputs';
            }
            else if (shape instanceof OutputNode)
                pipeline_outputs.push(shape);

            else if (shape instanceof CDtNode || shape instanceof RawNode) {
                var magnet = null;
                pipeline_inputs.push(shape);

                // all CDtNodes or RawNodes (inputs) should feed into a MethodNode and have only one magnet
                if (shape.out_magnets.length != 0)
                    throw 'Invalid amount of magnets for output node!';

                // is this magnet connected?
                if (shape.out_magnets[0].connected.length === 0)
                    throw 'Unconnected input node';
            }
            else
                throw 'Unknown node type encountered!';
        });


        // sort pipelines by their isometric position, left-to-right, top-to-bottom
        // (sort of like reading order if you tilt your screen 30° clockwise)
        pipeline_inputs.sort(Geometry.isometricSort);
        pipeline_outputs.sort(Geometry.isometricSort);

        // at least one Connector must terminate as pipeline output
        if (pipeline_outputs.length === 0) {
            submitError('Pipeline has no output');
            return;
        }

        // TODO: This block of code needs to be pulled out of this function
        var is_revision = $('#id_pipeline_select').length > 0;

        // arguments to initialize new Pipeline Family
        var family_name = $('#id_family_name').val(),  // hidden input if revision
            family_desc = $('#id_family_desc').val(),
            family_pk = $('#id_family_pk').val(),
            revision_name = $('#id_revision_name').val(),
            revision_desc = $('#id_revision_desc').val(),
            users_allowed = $("#id_users_allowed").val(),
            groups_allowed = $("#id_groups_allowed").val();

        // Form validation
        if (!is_revision) {
            if (family_name === '') {
                // FIXME: is there a better way to do this trigger?
                $('li', 'ul#id_ctrl_nav')[0].click();
                $('#id_family_name').css({'background-color': '#ffc'}).focus();
                submitError('Pipeline family must be named');
                return;
            }
            $('#id_family_name, #id_family_desc').css('background-color', '#fff');
        }

        $('#id_revision_name, #id_revision_desc').css('background-color', '#fff');
        // TODO: END TODO


        // Now we're ready to start
        var form_data = {
            users_allowed: users_allowed,
            groups_allowed: groups_allowed,
            // There is no PipelineFamily yet; we're going to create one.
            family_pk: family_pk,
            family_name: family_name,
            family_desc: family_desc,
            // arguments to add first pipeline revision
            revision_name: revision_name,
            revision_desc: revision_desc,
            // Canvas information to store in the Pipeline object.
            canvas_width: canvas.width,
            canvas_height: canvas.height,
            revision_parent_pk: is_revision ? $('#id_pipeline_select').val() : null,
            // Arrays will be populated in the following code.
            pipeline_steps: [],
            pipeline_inputs: [],
            pipeline_outputs: []
        };

        // update form data with inputs
        $.each(pipeline_inputs, function(idx, input){
            var structure = null;

            // Setup the compound datatype
            if(input instanceof CDtNode)
                structure = {
                    compounddatatype: input.pk
                };

            // Slap this input into the form data
            form_data.pipeline_inputs[idx] = {
                structure: structure,
                dataset_name: input.label,
                dataset_idx: idx + 1,
                x: input.x * canvas_x_ratio,
                y: input.y * canvas_y_ratio,
                min_row: null, // in the future these can be more detailed
                max_row: null
            };
        });

        // MethodNodes are now sorted live, prior to pipeline submission —JN
        var sorted_elements = [];
        $.each(self.canvasState.exec_order, function(_, exe_order_element) {
            sorted_elements = sorted_elements.concat(exe_order_element);
        });

        // Add arguments for input cabling
        $.each(sorted_elements, function(idx, step){

            // TODO: Make this work for nested pipelines

            // Put the method in the form data
            form_data.pipeline_steps[idx] = {
                transf_pk: step.pk,  // to retrieve Method
                transf_type: "Method",
                step_num: idx + 1,  // 1-index (pipeline inputs are index 0)
                x: step.x * canvas_x_ratio,
                y: step.y * canvas_y_ratio,
                name: step.label,
                fill_colour: step.fill,
                cables_in: [],
                outputs_to_delete: [] // not yet implemented
            };

            // retrieve Connectors
            $.each(step.in_magnets, function(cable_idx, magnet){
                if (magnet.connected.length === 0)
                    return true; // continue;

                var connector = magnet.connected[0],
                    source = magnet.connected[0].source.parent;

                form_data.pipeline_steps[idx].cables_in[cable_idx] = {
                    source_dataset_name: connector.source.label,
                    dest_dataset_name: connector.dest.label,
                    source_step: source instanceof MethodNode ? sorted_elements.indexOf(source)+1 : 0,
                    keep_output: false, // in the future this can be more flexible
                    wires: [] // no wires for a raw cable
                };
            });
        });

        // Construct outputs
        $.each(pipeline_outputs, function(idx, output){
            var connector = output.in_magnets[0].connected[0],
                source_step = connector.source.parent,
                structure = null;

            form_data[idx] = {
                output_name: output.label,
                output_idx: idx + 1,
                output_CDT_pk: connector.source.cdt, // TODO: change this to structure
                source: source_step.pk,
                source_step: sorted_elements.indexOf(source_step) + 1, // 1-index
                source_dataset_name: connector.source.label, // magnet label
                x: output.x * canvas_x_ratio,
                y: output.y * canvas_y_ratio,
                wires: [] // in the future we might have this
            };
        });

        // this code written on Signal Hill, St. John's, Newfoundland
        // May 2, 2014 - afyp

        // this code modified at my desk
        // June 18, 2014 -- RL

        // I code at my desk too.
        // July 30, 2014 - JN

        // How did I even computer?
        // April 28, 2015 - Cat

        return form_data;
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

        // Invalidate and force redraw
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
            var method_node = new drydock_objects.MethodNode(
                    node.transformation,
                    node.transformation_family,
                    node.x * canvas_x_ratio,
                    node.y * canvas_y_ratio,
                    null, // fill
                    node.name,
                    node.inputs,
                    node.outputs
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
