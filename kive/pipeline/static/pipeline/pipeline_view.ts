import { Pipeline } from "./pipeline_load";
import { canvasState } from "./pipeline_dashboard";
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
    /**
     * @todo: Investigate if this can be done with simply CanvasState rather than the more verbose instantiation canvasState.
     */
    this.pipeline = new Pipeline(canvasState);
    canvasState.old_width  = canvasState.width  = canvasState.canvas.width  = $('#inner_wrap').width();
    $(window).off('resize');
    this.pipeline.load(pipeline_dict);
    this.pipeline.draw();
}