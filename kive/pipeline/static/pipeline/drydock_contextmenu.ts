"use strict";

import { CanvasState } from "./drydock";
import { CNode, Connector } from "./drydock_objects";
import "jquery";

interface ContextMenuInterface {
    [action: string]: ContextMenuAction;
}
type ContextMenuAction = (sel: (Connector|CNode)|(Connector|CNode)[]) => void

export class CanvasContextMenu {
    $menu: JQuery;
    
    constructor(selector: string, cs: CanvasState) {
        var menu = this;
        this.$menu = $(selector);
        this.$menu.on({
            // necessary to stop document click handler from kicking in
            mousedown: (e: JQueryMouseEventObject) => e.stopPropagation(),
            keydown:   (e: JQueryKeyEventObject)   => {
                e.stopPropagation();
                if (e.which === 27) { // esc
                    menu.cancel();
                }
            },
            click: function (e: JQueryMouseEventObject) {
                // when a context menu option is clicked
                e.stopPropagation();
                menu.$menu.hide();
            
                var sel = cs.selection;
                var action = $(this).data('action');
            
                if (sel && sel.length) {
                    // 'delete' is the only action that allows >1 node
                    // if (action !== 'delete') {
                    //     sel = sel[0];
                    // }
                    if (menu.actions.hasOwnProperty(action)) {
                        menu.actions[action](
                            action == 'delete' ? sel[0] : sel
                        );
                    }
                }
            }
        }, 'li');
    
        $(document).click( () => this.cancel() );
    }
    
    registerAction(name: string, newAction: ContextMenuAction) {
        if (!this.actions.hasOwnProperty(name)) {
            this.actions[name] = newAction;
        }
    }
    
    cancel() {
        this.$menu.hide();
    }
    
    actions: ContextMenuInterface = { };
    
}