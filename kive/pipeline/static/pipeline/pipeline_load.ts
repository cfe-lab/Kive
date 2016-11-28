"use strict";

import { Geometry } from "./geometry";
import { MethodNode, CdtNode, RawNode, OutputNode, Connector } from "./drydock_objects";
import { CanvasState } from "./drydock";
import 'jquery';

interface ApiPipelineData extends PipelineMetadata {
    steps?: ApiStepData[];
    inputs?: ApiInputData[];
    outcables?: ApiOutputData[];
}
interface PipelineMetadata {
    family?: string;
    family_desc?: string;
    revision_name?: string;
    revision_desc?: string;
    revision_parent?: number;
    published?: boolean;
    users_allowed?: string[];
    groups_allowed?: string[];
    canvas_width?: number;
    canvas_height?: number;
}
interface ApiCdtData {
    compounddatatype: number;
    min_row: null;
    max_row: null;
}
interface ApiInputData {
    structure: ApiCdtData;
    dataset_name: string;
    dataset_idx: number;
    x: number;
    y: number;
}
interface ApiOutputData {
    output_name: string;
    output_idx: number;
    output_cdt: number;
    source: number;
    source_step: number;
    source_dataset_name: string;
    x: number;
    y: number;
    custom_wires: any[]; // in the future we might have this
}
interface ApiStepData {
    transformation: number;  // to retrieve Method
    transformation_type: string;
    step_num: number;  // 1-index (pipeline inputs are index 0)
    x: number;
    y: number;
    name: string;
    fill_colour: string;
    cables_in: ApiCableData[];
    new_code_resource_revision_id: number;
    new_outputs_to_delete_names: string[];
    new_dependency_ids?: number[];
}
interface ApiCableData {
    source_dataset_name: string;
    dest_dataset_name: string;
    source_step: number;
    keep_output: boolean;
    custom_wires: any[]; // no wires for a raw cable
}

/**
 * Convert between API calls and CanvasState object.
 */
export class Pipeline {

    pipeline: any = null;
    private API_URL = "/api/pipelinefamilies/";

    // Pipeline constructor
    constructor(private canvasState: CanvasState) {
    }

    load(pipeline) {
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

        this.canvasState.has_unsaved_changes = false;
        for (let shape of this.canvasState.shapes) {
            shape.has_unsaved_changes = false;
        }
    }

    /**
     * This method serializes the pipeline into an object that can be
     * fed to the backend REST API.
     *
     * Will throw errors if there are any. Contain this method in a
     * try/catch block.
     *
     * See: /api/pipelines/
     *
     * @param form_data: starting data that all of the pipeline details
     * will be added to. Object format matches the JSON structure of the
     * API.
     *
     * form_data will be merged with existing this.metadata if setMetadata()
     * is used first.
     */
    serialize (metadata?: PipelineMetadata) {

        var pipeline_outputs = this.canvasState.getOutputNodes(),
            pipeline_inputs = this.canvasState.getInputNodes(),
            canvas_x_ratio = 1.0 / this.canvasState.width,
            canvas_y_ratio = 1.0 / this.canvasState.height,
            // This is a trivial modification until we hit a non trivial
            // @todo: This variable is not used. Why?
            is_trivial = true;

        // Check graph integrity
        // Warning: This will throw errors if pipeline is not complete.
        // serialize() should be wrapped in a try/catch block to account for this.
        this.canvasState.assertIntegrity();

        // Sort inputs and outputs by their isometric position, left-to-right, top-to-bottom
        // (sort of like reading order if you tilt your screen 30° clockwise).
        pipeline_inputs.sort(Geometry.isometricSort);
        pipeline_outputs.sort(Geometry.isometricSort);

        // Now we're ready to start
        let form_data: ApiPipelineData = metadata || {};
        form_data.steps = [];
        form_data.inputs = [];
        form_data.outcables = [];

        // Construct the input updates
        for (let idx = 0; idx < pipeline_inputs.length; idx++) {
            let input = pipeline_inputs[idx];
            let structure = null;

            // Setup the compound datatype
            if (CanvasState.isCdtNode(input)) {
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
        }

        // MethodNodes are now sorted live, prior to pipeline submission —JN
        var sorted_elements = this.canvasState.getSteps();

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
                    source_step: CanvasState.isMethodNode(source) ? sorted_elements.indexOf(source) + 1 : 0,
                    keep_output: false, // in the future this can be more flexible
                    custom_wires: [] // no wires for a raw cable
                };
            });
        });

        // Construct outputs
        $.each(pipeline_outputs, function(idx, output) {
            var connector = output.in_magnets[0].connected[0],
                source_step = connector.source.parent;

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
    }

    update (runstat, look_for_md5, run_id)  {
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
            if ( CanvasState.isInputNode(shape) ) {
                shape.status = 'CLEAR';
            }
        });

        // Update each pipeline step
        $.each(runstat.step_progress, function(step_index){
            var shape = self.canvasState.methods[step_index];

            shape.status = this.status;
            shape.log_id = this.log_id;
            shape.run_id = run_id;
        });

        // Update the outputs
        $.each(runstat.output_progress, function(output_pk, output) {
            var shape = self.canvasState.findOutputNode(parseInt(output_pk));

            if (CanvasState.isOutputNode(shape)) {
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

            if ( CanvasState.isInputNode(shape) ) {
                shape.dataset_id = output.dataset_id;
                shape.md5 = output.md5;

                shape.run_id = run_id;
                shape.found_md5 = (shape.md5 === look_for_md5);
            }
        });

        // Invalidate and force redraw
        self.canvasState.valid = false;
        self.canvasState.draw();
    }

    draw() {
        /**
         * Forces a redraw of this pipeline on its associated canvas
         */

        this.canvasState.testExecutionOrder();
        this.canvasState.detectAllCollisions();
        this.canvasState.draw();
    }

    // Private members
    private build_inputs() {
        /**
         * Sets up the canvas state with the inputs for a pipeline
         */
        var self = this,
            canvas_x_ratio = this.canvasState.width / this.canvasState.scale,
            canvas_y_ratio = this.canvasState.height / this.canvasState.scale;

        if (self.pipeline === null) {
            throw "build_inputs() called with no pipeline?";
        }

        // Over each input for the pipeline
        $.each(self.pipeline.inputs, function(_, node) {

            // BaseNode has no structure => no CDT, so it's raw
            if (node.structure === null) {
                self.canvasState.addShape(new RawNode(
                    node.x * canvas_x_ratio,
                    node.y * canvas_y_ratio,
                    node.dataset_name,
                    node.dataset_idx
                ));
            }
            else {
                self.canvasState.addShape(new CdtNode(
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
    }

    private build_steps() {
        /**
         * Private method that sets up canvas state to draw methods
         */
        var self = this,
            canvas_x_ratio = self.canvasState.width  / self.canvasState.scale,
            canvas_y_ratio = self.canvasState.height / self.canvasState.scale;

        if (self.pipeline === null) { throw "build_steps() called with no pipeline?"; }
        var method_node_offset = self.pipeline.inputs.length;

        // Over each pipeline step
        $.each(self.pipeline.steps, function() {
            var node = this,
                method_node = new MethodNode(
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
                        if (!CanvasState.isMethodNode(this) &&
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
                    connector = new Connector(source.out_magnets[0]);

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
                            connector = new Connector(this);
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
    }

    private build_outputs() {
        /**
         * Private method that sets up canvas state to draw outputs
         */
        var self = this,
            canvas_x_ratio = self.canvasState.width / self.canvasState.scale,
            canvas_y_ratio = self.canvasState.height / self.canvasState.scale;

        if (self.pipeline === null) { throw "build_outputs() called with no pipeline?"; }

        var method_node_offset = self.pipeline.inputs.length;

        for (let outcable of self.pipeline.outcables) {
            // identify source Method
            var source = self.canvasState.shapes[method_node_offset + outcable.source_step - 1];

            // Over each out magnet for that source
            for (let magnet of source.out_magnets) {
                if (magnet.label === outcable.source_dataset_name) {
                    var output_idx = outcable.output_idx;
                    let output_in_list = self.pipeline.outputs.filter(
                        output => output.dataset_idx === output_idx
                    );
                    if (output_in_list.length !== 1) {
                        throw "There should be exactly 1 output with dataset_idx=" + output_idx;
                    }
                    let output = output_in_list[0];

                    let connector = new Connector(magnet),
                        output_node = new OutputNode(
                            output.x * canvas_x_ratio,
                            output.y * canvas_y_ratio,
                            outcable.output_name,
                            outcable.pk
                        );

                    self.canvasState.addShape(output_node);

                    connector.x = output.x * canvas_x_ratio;
                    connector.y = output.y * canvas_y_ratio;

                    connector.dest = output_node.in_magnets[0];
                    connector.dest.connected = [connector];  // bind cable to output node
                    connector.source = magnet;
                    connector.dest.cdt = connector.source.cdt;

                    magnet.connected.push(connector);  // bind cable to source Method
                    self.canvasState.connectors.push(connector);
                    break;
                }
            }
        }
    }

    findNewStepRevisions() {
        var pipeline = this,
            pipeline_id = pipeline.pipeline.id,
            steps = pipeline.canvasState.getSteps();

        for (let step of steps) {
            step.updateSignal("update in progress");
        }
        pipeline.canvasState.valid = false;

        $.getJSON("/api/pipelines/" + pipeline_id + "/step_updates/").done(
            updates => pipeline.applyStepRevisions(updates)
        ).fail(() => {
            for (let step of steps) {
                step.updateSignal("unavailable");
            }
            pipeline.canvasState.valid = false;
        });
    }

    applyStepRevisions(updates) {
        var pipeline = this,
            steps = pipeline.canvasState.getSteps(),
            updated_step_nums = updates.map(update => update.step_num - 1);

        for (let i = 0; i < steps.length; i++) {
            if (updated_step_nums.indexOf(i) < 0) {
                steps[i].updateSignal('no update available');
            }
        }
        for (let update of updates) {
            let old_method = steps[update.step_num - 1];
            let new_method;
            let any_mismatch;
            if ( ! update.method) {
                new_method = old_method;
                any_mismatch = false;
            } else {
                new_method = new MethodNode(
                    update.method.id,
                    update.method.family_id,
                    0, // x
                    0, // y
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
        }

        pipeline.canvasState.valid = false;
    }

    isPublished() {
        /**
         * Returns whether or not the pipeline has been published
         */
        return this.pipeline.is_published_version;
    }

    publish(family_pk, callback) {
        /**
         * Sets this pipeline as published
         *
         * @param family_pk: primary key of the family
         * TODO: ^^ This should be in the serializer maybe?
         * @param callback: Function called on success
         */
        $.ajax({
            type: "PATCH",
            url: this.API_URL + family_pk,
            data: { published_version: this.pipeline.id },
            dataType: "json",
            success: result => {
                this.pipeline.is_published_version = true;
                callback(result);
            }
        });
    }

    unpublish(family_pk, callback) {
        /**
         * Sets this pipeline as unpublished
         *
         * @param family_pk: primary key of the family
         * TODO: ^^ This should be in the serializer maybe?
         * @param callback: Function called on success
         */
        $.ajax({
            type: "PATCH",
            url: this.API_URL + family_pk,
            data: { published_version: null },
            dataType: "json",
            success: result => {
                this.pipeline.is_published_version = false;
                callback(result);
            }
        });
    }
}

