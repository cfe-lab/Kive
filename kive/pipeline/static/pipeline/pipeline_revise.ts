import { Pipeline } from "./pipeline_load";
import { CanvasState } from "./drydock";
declare var $: any;
declare var noXSS: any;

export class PipelineReviser {
    pipelineRaw: any;
    pipelineRevision: Pipeline;
    submit_to_url: string;
    cs: CanvasState;

    constructor(data: string) {
        if (data === "") {
            throw "Pipeline could not be loaded: no data found";
        }
        try {
            this.pipelineRaw = JSON.parse(data);
        } catch(e) {
            console.error("Pipeline could not be loaded: JSON parse error");
        }
    }

    load(cs?: CanvasState) {
        if (cs) {
            this.cs = cs;
        }
        if (!this.cs) {
            throw "Pipeline could not be loaded: CanvasState not ready";
        }
        else {
            let $canvas = $(this.cs.canvas);
            this.pipelineRevision = new Pipeline(this.cs);
            this.submit_to_url = this.pipelineRaw.family_pk;

            $canvas.fadeOut({
                complete: () => {
                    this.pipelineRevision.load(this.pipelineRaw);
                    this.pipelineRevision.draw();
                    $canvas.fadeIn();
                }
            });
        }
    }

    setRevertCtrl(ctrl) {
        ctrl.click(() => this.load());
    }

    setUpdateCtrl(ctrl) {
        ctrl.click(() => this.pipelineRevision.findNewStepRevisions());
    }
}