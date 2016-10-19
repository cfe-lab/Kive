"use strict";

import { CanvasState } from "./drydock";
import { CNode, Connector } from "./drydock_objects";
import "jquery";

interface ContextMenuInterface {
    [action: string]: ContextMenuAction;
}
type ContextMenuAction = (sel: (Connector|CNode)|(Connector|CNode)[], e: JQueryMouseEventObject) => void

export class CanvasContextMenu {
    $menu: JQuery;
    private actions: ContextMenuInterface = {};
    private visible = false;
    
    constructor(selector: string, private cs: CanvasState) {
        var menu = this;
        this.$menu = $(selector);
        this.$menu.on({
            // necessary to stop document click handler from kicking in
            mousedown: (e: JQueryMouseEventObject) => e.stopPropagation(),
            keydown:   (e: JQueryKeyEventObject)   => {
                e.stopPropagation();
                if (e.which === 27) { // esc
                    this.visible && menu.cancel();
                }
            },
            click: function (e: JQueryMouseEventObject) {
                // when a context menu option is clicked
                e.stopPropagation();
                menu.$menu.hide();
            
                var sel = cs.selection;
                var action = $(this).data('action');
            
                if (sel) {
                    if (menu.actions.hasOwnProperty(action)) {
                        menu.actions[action](
                            action == 'delete' ? sel : sel[0],
                            e
                        );
                    }
                }
            }
        }, 'li');
    
        $(document).click( () => { this.visible && this.cancel(); } );
    }
    
    registerAction(name: string, newAction: ContextMenuAction) {
        if (!this.actions.hasOwnProperty(name)) {
            this.actions[name] = newAction;
        }
    }
    
    cancel() {
        this.$menu.hide();
        this.visible = false;
    }
    
    hide() {
        this.cancel();
    }
    
    show(e) {
        this.$menu.show().css({ top: e.pageY, left: e.pageX });
        $('li', this.$menu).show();
        this.visible = true;
    }
    
    open(e) {
        var sel = this.cs.selection;
        e.preventDefault();
        
        this.visible && this.cancel();
        
        // Edit mode can popup the context menu to delete and edit nodes
        if (this.cs.can_edit) {
            if (CanvasState.isNode(sel[0])) {
                this.show(e);
                $('.cm-add', this.$menu).hide();
                if (sel.length > 1 || CanvasState.isInputNode(sel[0])) {
                    $('.edit', this.$menu).hide();
                }
            } else {
                this.show(e);
                $('.edit, .delete', this.$menu).hide();
            }
        } else if (sel.length == 1) {
            // Otherwise, we're read only, so only popup the context menu for outputs with datasets
            let sel0 = sel[0];
            if (CanvasState.isDataNode(sel0) && sel0.dataset_id) {
                // Context menu for pipeline outputs
                this.show(e);
                $('.step_node', this.$menu).hide();
            } else if (CanvasState.isMethodNode(sel0) && sel0.log_id) {
                // Context menu for pipeline steps
                this.show(e);
                $('.dataset_node', this.$menu).hide();
            }
        }
        this.cs.doUp();
    }
    
    
}