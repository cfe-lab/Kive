"use strict";

import {RestApi} from "../rest_api.service";
import {Connector, MethodNode, OutputNode, RawNode} from "../canvas/drydock_objects";
import {CanvasState} from "../canvas/drydock";
import {Container, PipelineData} from "./PipelineApi";

/**
 * Convert between API calls and CanvasState object.
 */
export class Pipeline {

    container: Container = null;
    pipeline: PipelineData = null;
    files: string[] = [];

    // Pipeline constructor
    constructor(private canvasState: CanvasState) {}

    /**
     * This method loads a pipeline and sets up the canvas to
     * draw the pipeline.
     *
     * See: /api/pipelines/
     *
     * @param container: serialized pipeline array
     */
    load(container: Container) {
        this.container = container;
        this.pipeline = container.pipeline;
        this.files = container.files;
        this.canvasState.reset();

        this.buildInputs(this.pipeline);
        this.buildSteps(this.pipeline);
        this.buildOutputs(this.pipeline);

        this.canvasState.has_unsaved_changes = false;
        for (let shape of this.canvasState.shapes) {
            shape.has_unsaved_changes = false;
        }
    }

    loadFromString(container_data: string) {
        let container_json;
        try {
            container_json = JSON.parse(container_data);
        } catch (e) {
            throw "Pipeline could not be loaded: JSON parse error";
        }
        this.load(container_json);
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
    private buildInputs(pipeline: PipelineData) {
        let [ x_ratio, y_ratio ] = this.canvasState.getAspectRatio();

        // Not sure why this was true, but I set it to false so the inputs
        // wouldn't get reordered until the user starts dragging them around.
        this.canvasState.dragging = false;

        for (let i in pipeline.inputs) {
            let node = pipeline.inputs[i];
            // BaseNode has no structure => no CDT, so it's raw
            this.canvasState.addShape(new RawNode(
                node.x * x_ratio,
                node.y * y_ratio,
                node.dataset_name,
                i));
        }

    }

    /**
     * Private method that sets up canvas state to draw methods
     */
    private buildSteps(pipeline: PipelineData) {
        let [ x_ratio, y_ratio ] = this.canvasState.getAspectRatio();

        if (pipeline === null) { throw "build_steps() called with no pipeline?"; }

        // Over each pipeline step
        for (let node of pipeline.steps) {
            let method_node = new MethodNode(
                    node.x * x_ratio,
                    node.y * y_ratio,
                    node.fill_colour, // fill
                    node.driver,
                    node.inputs,
                    node.outputs
                );

            // Add `n draw
            this.canvasState.addShape(method_node);
            let methods = this.canvasState.getMethodNodes();

            // Connect method inputs
            for (let cable of node.inputs) {
                let source = null;
                let connector = null;
                // Find the destination for this
                let in_magnet = method_node.in_magnets.filter(
                    magnet => magnet.label === cable.dataset_name
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
    private buildOutputs(pipeline: PipelineData) {
        let [ x_ratio, y_ratio ] = this.canvasState.getAspectRatio();
        let method_node_offset = pipeline.inputs.length;

        for (let output_idx in pipeline.outputs) {
            let outcable = pipeline.outputs[output_idx];
            // identify source Method
            let source = this.canvasState.shapes[method_node_offset + outcable.source_step - 1];

            // Over each out magnet for that source
            for (let magnet of source.out_magnets) {
                if (magnet.label === outcable.source_dataset_name) {
                    let connector = new Connector(magnet);
                    let output_node = new OutputNode(
                        outcable.x * x_ratio,
                        outcable.y * y_ratio,
                        outcable.dataset_name
                    );

                    this.canvasState.addShape(output_node);

                    connector.x = outcable.x * x_ratio;
                    connector.y = outcable.y * y_ratio;
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

    setRevertCtrl(ctrl: string) {
        document.querySelector(ctrl).addEventListener('click', () => {
            let $canvas = $(this.canvasState.canvas);
            $canvas.fadeOut({
                complete: () => {
                    this.load(this.container);
                    $canvas.fadeIn();
                }
            });
            this.canvasState.canvas.dispatchEvent(
                new CustomEvent('CanvasStateChange', { detail: { revert: true } })
            );
        });

    }
}

