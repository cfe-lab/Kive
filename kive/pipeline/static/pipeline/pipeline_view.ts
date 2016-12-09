"use strict";

import { CanvasState, CanvasListeners, Pipeline, REDRAW_INTERVAL } from "./pipeline_all";

const DICT_ID = 'pipeline_dict';
const CANVAS_ID = 'pipeline_canvas';
const WRAPPER_ID = 'inner_wrap';

let id = document.getElementById.bind(document);
let pipeline_dict;
try {
    pipeline_dict = JSON.parse(id(DICT_ID).innerHTML);
} catch (e) {
    console.error("Could not load pipeline dict from memory.");
}
if (pipeline_dict) {
    let canvas = id(CANVAS_ID) as HTMLCanvasElement;
    canvas.width  = id(WRAPPER_ID).clientWidth * 0.7;
    canvas.height = Math.min(window.innerHeight, canvas.width * .75);
    let cstate = new CanvasState(canvas, false, REDRAW_INTERVAL);
    cstate.setScale(0.7);
    CanvasListeners.initMouseListeners(cstate);
    let pipeline = new Pipeline(cstate);
    pipeline.load(pipeline_dict);
    pipeline.draw();
}