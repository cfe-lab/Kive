import { Pipeline } from "./pipeline_load";
import { canvasState } from "./pipeline_add";
import 'jquery';

let pipeline_dict_raw = $("#pipeline_dict").text();
let pipeline_dict;
try {
    pipeline_dict = JSON.parse(pipeline_dict_raw);
} catch(e) {
    console.error("Could not load pipeline dict from memory.");
    console.trace();
}
if (pipeline_dict) {
    this.pipeline = new Pipeline(canvasState);
    this.pipeline.load(pipeline_dict);
    this.pipeline.draw();
}