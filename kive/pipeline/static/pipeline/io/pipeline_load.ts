"use strict";

import { RestApi } from "../rest_api.service";
import { MethodNode, CdtNode, RawNode, OutputNode, Connector } from "../canvas/drydock_objects";
import { CanvasState } from "../canvas/drydock";
import { PipelineFromApi } from "./PipelineApi";

/**
 * Convert between API calls and CanvasState object.
 */
export class Pipeline {

    pipeline: PipelineFromApi = null;
    private FAMILY_API_URL = "/api/pipelinefamilies/";
    private REVISION_API_URL = "/api/pipelines/";
    private family_pk: number;

    // Pipeline constructor
    constructor(private canvasState: CanvasState) {}

    /**
     * This method loads a pipeline and sets up the canvas to
     * draw the pipeline.
     *
     * See: /api/pipelines/
     *
     * @param pipeline: serialized pipeline array
     */
    load(pipeline: PipelineFromApi) {
        this.pipeline = pipeline;
        this.canvasState.reset();

        this.buildInputs(pipeline);
        this.buildSteps(pipeline);
        this.buildOutputs(pipeline);

        this.canvasState.has_unsaved_changes = false;
        for (let shape of this.canvasState.shapes) {
            shape.has_unsaved_changes = false;
        }
    }

    loadFromString(pipeline_data: string) {
        let pipeline_json;
        try {
            pipeline_json = JSON.parse(pipeline_data);
        } catch (e) {
            throw "Pipeline could not be loaded: JSON parse error";
        }
        this.family_pk = pipeline_json.family_pk;
        this.load(pipeline_json);
    }

    /**
     * Updates the progress of this pipeline with the status
     *
     * See: /api/runs/
     *
     * @param run_status: A dictionary containing the status
     *  components of a run.
     * @param look_for_md5: Marks a dataset if its md5 sum matches
     *  this input
     * @param run_id: Keeps track of the run id
     */
    update (run_status, look_for_md5, run_id)  {

        // Update each pipeline step
        let methods = this.canvasState.getMethodNodes();
        for (let step_index in run_status.step_progress) {
            let step_status = run_status.step_progress[step_index];
            let method_node = methods[step_index];
            method_node.status = step_status.status;
            method_node.log_id = step_status.log_id;
            method_node.run_id = run_id;
        }

        // Update the outputs
        for (let output_pk in run_status.output_progress) {
            updateXput(
                run_status.output_progress[output_pk],
                this.canvasState.findOutputNode(parseInt(output_pk))
            );
        }

        // Update the inputs
        for (let input_pk in run_status.inputs) {
            updateXput(
                run_status.inputs[input_pk],
                this.canvasState.findInputNode(parseInt(input_pk))
            );
        }

        // Invalidate and force redraw
        this.canvasState.valid = false;
        this.canvasState.draw();

        function updateXput(status, node) {
            node.status = status.status || "CLEAR";
            node.dataset_id = status.dataset_id;
            node.md5 = status.md5;
            node.run_id = run_id;
            node.found_md5 = (node.md5 === look_for_md5);
        }
    }

    /**
     * Forces a redraw of this pipeline on its associated canvas
     */
    draw() {
        this.canvasState.testExecutionOrder();
        this.canvasState.detectAllCollisions();
        this.canvasState.draw();
    }

    /**
     * Sets up the canvas state with the inputs for a pipeline
     */
    private buildInputs(pipeline: PipelineFromApi) {
        let [ x_ratio, y_ratio ] = this.canvasState.getAspectRatio();

        // TODO: Not sure why this is set to true? I'm sure it's
        // important but I'd like an explanation
        this.canvasState.dragging = true;

        for (let node of pipeline.inputs) {

            let node_args = [
                node.x * x_ratio,
                node.y * y_ratio,
                node.dataset_name,
                node.dataset_idx
            ];

            // BaseNode has no structure => no CDT, so it's raw
            let InputCtor;
            if (node.structure === null) {
                InputCtor = RawNode;
            } else {
                node_args.unshift(node.structure.compounddatatype);
                InputCtor = CdtNode;
            }
            this.canvasState.addShape(new InputCtor(...node_args));
        }

    }

    /**
     * Private method that sets up canvas state to draw methods
     */
    private buildSteps(pipeline: PipelineFromApi) {
        let [ x_ratio, y_ratio ] = this.canvasState.getAspectRatio();

        if (pipeline === null) { throw "build_steps() called with no pipeline?"; }

        // required for cables to connect properly - API should return it already sorted but just in case
        pipeline.steps.sort((a, b) => a.step_num - b.step_num);

        // Over each pipeline step
        for (let node of pipeline.steps) {
            let method_node = new MethodNode(
                    node.transformation,
                    node.transformation_family,
                    node.x * x_ratio,
                    node.y * y_ratio,
                    node.fill_colour, // fill
                    node.name,
                    node.inputs,
                    node.outputs,
                    undefined, // status
                    node.outputs_to_delete
                );

            // Add `n draw
            this.canvasState.addShape(method_node);
            let methods = this.canvasState.getMethodNodes();

            // Connect method inputs
            for (let cable of node.cables_in) {
                let source = null;
                let connector = null;
                // Find the destination for this
                let in_magnet = method_node.in_magnets.filter(
                    magnet => magnet.label === cable.dest_dataset_name
                )[0];

                // Not found?
                if (in_magnet === undefined || in_magnet === null) {
                    throw "Failed to redraw Pipeline: missing in_magnet";
                }

                // cable from pipeline input, identified by dataset_name
                if (cable.source_step === 0) {

                    // Find the source for this
                    source = this.canvasState.shapes.filter(
                        shape =>
                            !CanvasState.isMethodNode(shape) &&
                            shape.label === cable.source_dataset_name
                    )[0];

                    // Not found?
                    if (source === undefined || source === null) {
                        throw "Failed to redraw Pipeline: missing data node";
                    }

                    // data nodes only have one out-magnet, so use 0-index
                    connector = new Connector(source.out_magnets[0]);

                    // connect other end of cable to the MethodNode
                    connector.x = in_magnet.x;
                    connector.y = in_magnet.y;
                    connector.dest = in_magnet;

                    source.out_magnets[0].connected.push(connector);
                    in_magnet.connected.push(connector);
                    this.canvasState.connectors.push(connector);
                }
                else {
                    // cable from another MethodNode
                    // @todo: this is rather fragile
                    // find the correct out-magnet
                    let out_magnet = methods[cable.source_step - 1].out_magnets.filter(
                        magnet => magnet.label === cable.source_dataset_name
                    )[0];

                    connector = new Connector(out_magnet);
                    connector.x = out_magnet.x;
                    connector.y = out_magnet.y;
                    connector.dest = in_magnet;

                    out_magnet.connected.push(connector);
                    in_magnet.connected.push(connector);
                    this.canvasState.connectors.push(connector);
                }
            }
        }
    }

    /**
     * Private method that sets up canvas state to draw outputs
     */
    private buildOutputs(pipeline: PipelineFromApi) {
        let [ x_ratio, y_ratio ] = this.canvasState.getAspectRatio();
        let method_node_offset = pipeline.inputs.length;

        for (let outcable of pipeline.outcables) {
            // identify source Method
            let source = this.canvasState.shapes[method_node_offset + outcable.source_step - 1];

            // Over each out magnet for that source
            for (let magnet of source.out_magnets) {
                if (magnet.label === outcable.source_dataset_name) {
                    let output_idx = outcable.output_idx;
                    let output = pipeline.outputs.filter(
                        output => output.dataset_idx === output_idx
                    )[0];
                    if (output === undefined || output === null) {
                        throw "There should be exactly 1 output with dataset index " + output_idx;
                    }

                    let connector = new Connector(magnet);
                    let output_node = new OutputNode(
                        output.x * x_ratio,
                        output.y * y_ratio,
                        outcable.output_name,
                        outcable.pk
                    );

                    this.canvasState.addShape(output_node);

                    connector.x = output.x * x_ratio;
                    connector.y = output.y * y_ratio;
                    connector.dest = output_node.in_magnets[0];
                    connector.dest.connected = [ connector ];  // bind cable to output node
                    connector.source = magnet;
                    connector.dest.cdt = connector.source.cdt;

                    magnet.connected.push(connector);  // bind cable to source Method
                    this.canvasState.connectors.push(connector);
                    break;
                }
            }
        }
    }

    /**
     * Calls updateSignal on all the MethodNods in an array with a fixed status string parameter.
     * @param steps The MethodNodes to update the signals of
     * @param status The status string to set
     */
    private static updateSignalsOf(steps: MethodNode[], status: string) {
        for (let step of steps) {
            step.updateSignal(status);
        }
    }

    /**
     * Strips the protocol, hostname, and port from a URL, thus making it relative instead of absolute.
     * @param url the absolute url
     * @returns the relative url
     */
    private static stripHostFrom(url: string): string {
        let host = window.location.protocol + "//" + window.location.host;
        let escaped_host = host.replace(/[\-\[\]\/\{\}\(\)\*\+\?\.\\\^\$\|]/g, "\\$&");
        let host_pattern = new RegExp("^" + escaped_host);
        return url.replace(host_pattern, "");
    }

    findNewStepRevisions() {
        let steps = this.canvasState.getSteps();
        Pipeline.updateSignalsOf(steps, "update in progress");
        this.canvasState.valid = false;
        RestApi.get(
            Pipeline.stripHostFrom(this.pipeline.step_updates),
            updates => this.applyStepRevisions(updates),
            () => {
                Pipeline.updateSignalsOf(steps, "unavailable");
                this.canvasState.valid = false;
            }
        );
    }

    private static checkForCrrAndDepUpdates(method: MethodNode, update): boolean {
        if (update.code_resource_revision) {
            method.new_code_resource_revision = update.code_resource_revision;
        }
        if (update.dependencies) {
            method.new_dependencies = update.dependencies;
        }
        return update.code_resource_revision || update.dependencies;
    }

    applyStepRevisions(updates) {
        let steps = this.canvasState.getSteps();

        Pipeline.updateSignalsOf(
            steps.filter( // only steps that did not get an update
                (_, i) => updates.map(e => e.step_num - 1).indexOf(i) === -1
            ),
            'no update available'
        );

        for (let update of updates) {
            let old_method = steps[update.step_num - 1];
            if ( ! update.method) {
                if (Pipeline.checkForCrrAndDepUpdates(old_method, update)) {
                    old_method.updateSignal('updated');
                }
            } else {
                let new_method = new MethodNode(
                    update.method.id,
                    update.method.family_id,
                    0, // x
                    0, // y
                    old_method.fill,
                    old_method.label,
                    update.method.inputs,
                    update.method.outputs
                );
                let any_mismatch = this.canvasState.replaceMethod(old_method, new_method);
                Pipeline.checkForCrrAndDepUpdates(new_method, update);
                new_method.updateSignal(any_mismatch ? 'updated with issues' : 'updated');
            }
        }

        this.canvasState.valid = false;
    }

    /**
     * Returns whether or not the pipeline has been published
     */
    isPublished() {
        return this.pipeline.published;
    }

    /**
     * Sets this pipeline as published
     * @todo test and utilize this!
     *
     * @param callback: Function called on success
     */
    publish(callback) {
        RestApi.patch(this.REVISION_API_URL + this.pipeline.id, { published: true }, callback);
    }

    /**
     * Sets this pipeline as unpublished
     * @todo test and utilize this!
     *
     * @param callback: Function called on success
     */
    unpublish(callback) {
        RestApi.patch(this.REVISION_API_URL + this.pipeline.id, { published: false }, callback);
    }

    setRevertCtrl(ctrl: string) {
        document.querySelector(ctrl).addEventListener('click', () => {
            let $canvas = $(this.canvasState.canvas);
            $canvas.fadeOut({
                complete: () => {
                    this.load(this.pipeline);
                    $canvas.fadeIn();
                }
            });
        });
    }

    setUpdateCtrl(ctrl: string) {
        document.querySelector(ctrl).addEventListener('click', () => this.findNewStepRevisions());
    }
}

