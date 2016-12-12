import { CanvasState } from "../canvas/drydock";
import {Pipeline} from "pipeline_load";
import {PipelineSubmit} from "pipeline_submit";

/*
 * @todo list
 * - Make partial pipelines serializable
 * - Bypass checkForUnsavedChanges
 * - Write layout for pipeline thumb
 * - Include global pipeline thumb if browserStorage
 * - Make localStorage API
 */

export class LocallyStoredCanvasState {

    private STORAGE_KEY = "kive_pipeline";
    private storage: Storage;

    LocallyStoredCanvasState() {
        if (window.hasOwnProperty("localStorage")) {
            this.storage = window.localStorage;
        } else if (window.hasOwnProperty("sessionStorage")) {
            this.storage = window.sessionStorage;
        } else {
            throw "Browser environment does not support session or local storage!";
        }
    }

    public detect(): boolean {
        return this.storage.getItem(this.STORAGE_KEY) !== null;
    }

    public save(state: CanvasState): void {
        let pipeline = new Pipeline(state);

        // maybe a PipelineState object is in order?
        // contains a CanvasState + metadata.

        /*
        class PipelineState

        members:
        pipeline
        canvasState
        API_URL

        constructor(canvasState)
        serialize - calls canvasState.serialize
        load(pipeline: PipelineForApi)

        draw()
           canvasState.testExecutionOrder()
           canvasState.detectAllCollisions()
           canvasState.draw()



         */

        let serialized;
        // let serialized = pipeline.serialize({
        //     users_allowed:  PipelineSubmit.getPermissionsArray($user_permissions),
        //     groups_allowed: PipelineSubmit.getPermissionsArray($group_permissions),
        //
        //     // There is no PipelineFamily yet; we're going to create one.
        //     family,
        //     family_desc: $family_desc.val(),
        //
        //     // arguments to add first pipeline revision
        //     revision_name: $revision_name.val(),
        //     revision_desc: $revision_desc.val(),
        //     revision_parent: action === 'revise' ? parent_revision_id : null,
        //     published: $published.prop('checked'),
        //
        //     // Canvas information to store in the Pipeline object.
        //     canvas_width:  canvasState.width,
        //     canvas_height: canvasState.height
        // });
        try {
            this.storage.setItem(this.STORAGE_KEY, JSON.stringify(serialized));
        } catch (e) {
            throw "Ran out of available memory for browser's local storage";
        }
    }

    // public load(): CanvasState {
    //     // todo
    //     return new CanvasState()
    // }
}