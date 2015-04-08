
// place in global namespace to access from other files
var submit_to_url = '/pipeline_add';

var rawNodeWidth = 20,
    rawNodeHeight = 25,
    rawNodeColour = "#8D8",
    rawNodeInset = 10,
    rawNodeOffset = 25;

var cdtNodeWidth = 45,
    cdtNodeHeight = 28,
    cdtNodeColour = '#88D',
    cdtNodeInset = 13,
    cdtNodeOffset = 15;

var mNodeWidth = 80,
    mNodeInset = 10,
    mNodeSpacing = 20,
    mNodeColour = '#999',
    mNodeOffset = 10;

// Draw pipeline inputs on the canvas.
function draw_inputs(canvasState, pipeline) {
    var pipeline_inputs = pipeline['pipeline_inputs'];  // Array[]
    for (i = 0; i < pipeline_inputs.length; i++) {
        node = pipeline_inputs[i];
        if (node.CDT_pk === null) {
            canvasState.addShape(new RawNode(node.x * canvasState.canvas.width / canvasState.scale, node.y * canvasState.canvas.height / canvasState.scale, rawNodeWidth, rawNodeHeight, null, null, null, node.dataset_name));
        } else {
            canvasState.addShape(new CDtNode(node.CDT_pk, node.x * canvasState.canvas.width / canvasState.scale, node.y * canvasState.canvas.height / canvasState.scale, cdtNodeWidth, cdtNodeHeight, null, null, null, node.dataset_name));
        }
        canvasState.dragging = true;
        //canvasState.selection.push(canvasState.shapes[canvasState.shapes.length-1]);
//        canvasState.doUp();
    }
    draw_steps(canvasState, pipeline, pipeline_inputs.length);
}

// Draw pipeline steps on the canvas.
function draw_steps(canvasState, pipeline, method_node_offset) {
    var pipeline_steps = pipeline['pipeline_steps'];
    for (i = 0; i < pipeline_steps.length; i++) {
        node = pipeline_steps[i];
        var inputs = pipeline_steps[i]["inputs"],
            outputs = pipeline_steps[i]["outputs"];
        
        var method_node = new MethodNode(node.transf_pk, node.family_pk, node.x * canvasState.canvas.width / canvasState.scale, node.y * canvasState.canvas.height / canvasState.scale, mNodeWidth,
                mNodeInset, mNodeSpacing, mNodeColour, node.name, mNodeOffset,
                inputs, outputs);

        canvasState.addShape(method_node);
        method_node.draw(canvasState.ctx);  // to update Magnet x and y

        // connect Method inputs
        cables = node['cables_in'];
        for (j = 0; j < cables.length; j++) {
            cable = cables[j];
            if (cable.source_step == 0) {
                // cable from pipeline input, identified by dataset_name
                source = null;
                for (k = 0; k < canvasState.shapes.length; k++) {
                    shape = canvasState.shapes[k];
                    if (shape.constructor !== MethodNode && shape.label === cable.source_dataset_name) {
                        source = shape;
                        break;
                    }
                }
                if (source === null) {
                    alert("Failed to redraw Pipeline: missing data node");
                    return;
                }

                // data nodes only have one out-magnet, so use 0-index
                connector = new Connector(null, null, source.out_magnets[0]);

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
                        connector = new Connector(null, null, magnet);
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
    draw_outputs(canvasState, pipeline, method_node_offset);
}

// Draw pipeline outputs on the canvas.
function draw_outputs(canvasState, pipeline, method_node_offset) {
    var pipeline_outputs = pipeline['pipeline_outputs'];
    for (i = 0; i < pipeline_outputs.length; i++) {
        this_output = pipeline_outputs[i];

        // identify source Method
        source = canvasState.shapes[method_node_offset + this_output.source_step - 1];

        // find the correct out-magnet
        for (k = 0; k < source.out_magnets.length; k++) {
            magnet = source.out_magnets[k];
            if (magnet.label === this_output.source_dataset_name) {
                connector = new Connector(null, null, magnet);
                output_node = new OutputNode(
                    this_output.x * canvasState.canvas.width / canvasState.scale,
                    this_output.y * canvasState.canvas.height / canvasState.scale,
                    null, null, null, null, null,
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

function update_status(canvasState, status, look_for_md5) {
    var pipeline_steps = status.runs.step_progress;
    var outputs = status.runs.output_progress;

    // Update all runsteps
    for(method_pk in pipeline_steps) {
        var shape = canvasState.findMethodNode(method_pk);
        if(shape != null)
            shape.status = pipeline_steps[method_pk];
    }

    // Update all outputs
    for(output_pk in outputs) {
        var shape = canvasState.findOutputNode(output_pk);
        if(shape != null) {
            shape.status = outputs[output_pk].status;
            shape.md5 = outputs[output_pk].md5
            shape.dataset_id = outputs[output_pk].dataset_id;

            if(shape.md5 == look_for_md5)
                shape.found_md5 = true;
        }
    }

    // Invalidate to force a redraw
    canvasState.valid = false;
}