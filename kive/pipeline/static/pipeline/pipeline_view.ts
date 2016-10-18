"use strict";

import { CanvasState, CanvasListeners, Pipeline, REDRAW_INTERVAL } from "./pipeline_all";
import 'jquery';
import '/static/portal/noxss.js';

declare var noXSS: any;
noXSS();

let pipeline_dict_raw = $("#pipeline_dict").text();
let pipeline_dict;

try {
    pipeline_dict = JSON.parse(pipeline_dict_raw);
} catch(e) {
    console.error("Could not load pipeline dict from memory.");
}

if (pipeline_dict) {
    // initialize animated canvas
    let canvas  = document.getElementById('pipeline_canvas') as HTMLCanvasElement;
    canvas.width  = $('#inner_wrap').width() * 0.7;
    canvas.height = Math.min(window.innerHeight, canvas.width * .75);
    
    let canvasState = new CanvasState(canvas, false, REDRAW_INTERVAL);
    canvasState.scale = 0.7;
    CanvasListeners.initMouseListeners(canvasState);
    
    let pipeline = new Pipeline(canvasState);
    pipeline.load(pipeline_dict);
    pipeline.draw();
}