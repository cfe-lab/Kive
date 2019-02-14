import { CanvasState } from "../canvas/drydock";
import { Geometry } from "../canvas/geometry";
import { RawNode, MethodNode, Magnet, OutputNode } from "../canvas/drydock_objects";
import {Container, DataSource, PipelineData, Step} from "./PipelineApi";

/**
 * This method serializes the pipeline into an object that can be
 * fed to the backend REST API.
 *
 * Will throw errors if there are any. Contain this method in a
 * try/catch block.
 *
 * See: /api/pipelines/
 *
 * @param canvasState: the state object of the pipeline canvas
 * @param metadata: starting data that all of the pipeline details
 * will be added to. Object format matches the JSON structure of the
 * API.
 *
 * form_data will be merged with existing this.metadata if setMetadata()
 * is used first.
 */
export function serializePipeline (canvasState: CanvasState, metadata?: Container) {

    let pipeline_outputs = canvasState.getOutputNodes();
    let pipeline_inputs = canvasState.getInputNodes();
    let pipeline_steps = canvasState.getSteps();
    let canvas_dimensions = canvasState.getAspectRatio();

    // Check graph integrity
    // Warning: This will throw errors if pipeline is not complete.
    // serialize() should be wrapped in a try/catch block to account for this.
    canvasState.assertIntegrity();

    // Sort inputs and outputs by their isometric position, left-to-right, top-to-bottom
    // (sort of like reading order if you tilt your screen 30° clockwise).
    pipeline_inputs.sort(Geometry.isometricSort);
    pipeline_outputs.sort(Geometry.isometricSort);

    let form_data = metadata || <Container> {};
    let pipeline_data = form_data.pipeline || <PipelineData> {};
    form_data.pipeline = pipeline_data;
    pipeline_data.steps = serializeSteps(pipeline_steps, canvas_dimensions);
    pipeline_data.inputs = serializeInputs(pipeline_inputs, canvas_dimensions);
    pipeline_data.outputs = serializeOutcables(pipeline_outputs, pipeline_steps, canvas_dimensions);

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

function serializeInputs(pipeline_inputs: RawNode[], canvas_dimensions: [ number, number ]): DataSource[] {
    let serialized_inputs = [];
    let [ x_ratio, y_ratio ] = canvas_dimensions;

    // Construct the input updates
    for (let i = 0; i < pipeline_inputs.length; i++) {
        let input = pipeline_inputs[i];

        // Slap this input into the form data
        serialized_inputs[i] = {
            dataset_name: input.label,
            x: input.x / x_ratio,
            y: input.y / y_ratio
        };
    }

    return serialized_inputs;
}

function serializeSteps(pipeline_steps: MethodNode[], canvas_dimensions: [ number, number ]): Step[] {
    let serialized_steps = [];
    let [ x_ratio, y_ratio ] = canvas_dimensions;

    // Add arguments for input cabling
    for (let i = 0; i < pipeline_steps.length; i++) {

        let step = pipeline_steps[i];

        // Put the method in the form data
        serialized_steps[i] = {
            x: step.x / x_ratio,
            y: step.y / y_ratio,
            driver: step.label,
            fill_colour: step.fill
        };

        if (step.new_dependencies && step.new_dependencies.length) {
            serialized_steps[i].new_dependency_ids =
                step.new_dependencies.map(dependency => dependency.id);
        }

        // retrieve Connectors
        serialized_steps[i].inputs = serializeInMagnets(step.in_magnets, pipeline_steps);
        serialized_steps[i].outputs = step.out_magnets.map(
            magnet => magnet.label);
    }

    return serialized_steps;
}

function serializeInMagnets(in_magnets: Magnet[], pipeline_steps: MethodNode[]): DataSource[] {
    let serialized_inputs = [];

    // retrieve Connectors
    for (let j = 0; j < in_magnets.length; j++) {
        let magnet = in_magnets[j];

        if (magnet.connected.length === 0) {
            serialized_inputs[j] = {
                source_dataset_name: null,
                source_step: null,
                dataset_name: magnet.label
            };
        }
        else {
            let connector = magnet.connected[0];
            let source = magnet.connected[0].source.parent;

            serialized_inputs[j] = {
                source_dataset_name: connector.source.label,
                dataset_name: connector.dest.label,
                source_step: CanvasState.isMethodNode(source)
                    ? pipeline_steps.indexOf(source) + 1
                    : 0
            };
        }
    }

    return serialized_inputs;
}

function serializeOutcables(
    pipeline_outputs: OutputNode[],
    pipeline_steps: MethodNode[],
    canvas_dimensions: [ number, number ]
): DataSource[] {
    let serialized_outputs = [];
    let [ x_ratio, y_ratio ] = canvas_dimensions;

    // Construct outputs
    for (let i = 0; i < pipeline_outputs.length; i++) {
        let output = pipeline_outputs[i];
        let magnet = output.in_magnets[0];
        if (magnet.connected.length === 0) {
            serialized_outputs[i] = {
                source_dataset_name: null,
                source_step: null,
                dataset_name: output.label,
                x: output.x / x_ratio,
                y: output.y / y_ratio
            };
        }
        else {
            let connector = magnet.connected[0];
            let source_step = connector.source.parent;

            serialized_outputs[i] = {
                dataset_name: output.label,
                source_step: pipeline_steps.indexOf(source_step) + 1, // 1-index
                source_dataset_name: connector.source.label, // magnet label
                x: output.x / x_ratio,
                y: output.y / y_ratio
            };
        }
    }
    return serialized_outputs;
}