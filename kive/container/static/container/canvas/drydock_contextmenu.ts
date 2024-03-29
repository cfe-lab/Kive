"use strict";

import { CanvasState } from "./drydock";
import { CNode, Connector } from "./drydock_objects";
import * as $ from "jquery";

interface ContextMenuInterface {
    [action: string]: ContextMenuAction;
}
type Selectable = (Connector|CNode);
type ContextMenuAction = (sel: Selectable|Selectable[], e: JQueryMouseEventObject) => void;
type CriteriaFn = (multi: boolean, sel: Selectable|Selectable[]) => boolean;

export class CanvasContextMenu {
    $menu: JQuery;
    private $ul: JQuery;
    private actions: ContextMenuInterface = {};
    private criteria: { [id: string]: Function } = {};
    private visible = false;

    constructor(selector: string, private cs: CanvasState) {
        var menu = this;
        this.$menu = $(selector);
        this.$ul = $('<ul>').appendTo(this.$menu);
        this.initEvents();
    }

    /**
     * Initialize event listeners
     */
    private initEvents() {
        this.$menu.on({
            // necessary to stop document click handler from kicking in
            mousedown: (e: JQueryMouseEventObject) => e.stopPropagation(),
            keydown:   (e: JQueryKeyEventObject)   => {
                e.stopPropagation();
                if (e.key === "Escape") {
                    this.visible && this.cancel();
                }
            },
            click: this.clickMenuHandlerFactory()
        }, 'li');
        $(document).click( () => { this.visible && this.cancel(); } );
    }

    /**
     *
     * @returns an event handler object ready to assign to a listener.
     */
    private clickMenuHandlerFactory(): (e: JQueryMouseEventObject) => void {
        let menu = this;
        return function(e) {
            // when a context menu option is clicked
            e.stopPropagation();
            menu.$menu.hide();

            var sel = menu.cs.selection;
            var action = $(this).data('action');

            if (sel && menu.actions.hasOwnProperty(action)) {
                menu.actions[action](
                    action === 'delete' ? sel : sel[0],
                    e
                );
            }
        };
    }

    /**
     * Creates a context menu option
     * @param name Human-readable name - internal ID will be generated based on this
     * @param criteriaFn Function for checking if this action should be available
     *      Params are (1) boolean for whether or not multi-node selections are allowed,
     *      and (2) the CanvasState's current selection.
     * @param newAction
     *      The action to run. Params are (1) the CanvasState's current selection and
     *      (2) the event object.
     */
    registerAction(name: string, criteriaFn: CriteriaFn, newAction: ContextMenuAction) {
        let id_name = name.toLowerCase().replace(/[^A-Za-z0-9]/g, '_');
        if (!this.actions.hasOwnProperty(id_name)) {
            let $li = $('<li>').addClass(id_name).data('action', id_name).text(name);
            this.$ul.append($li);
            this.actions[id_name] = newAction;
            this.criteria[id_name] = criteriaFn;
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
        e.preventDefault();

        let sel = this.cs.selection.length > 1 ? this.cs.selection : this.cs.selection[0];
        let sel_multi = this.cs.selection.length > 1;

        this.visible && this.cancel();

        this.show(e);
        this.$ul.find('li').hide();
        for (let action_id in this.actions) {
            if (this.criteria[action_id](sel_multi, sel)) {
                this.$ul.find('.' + action_id).show();
            }
        }
        if (this.$ul.find('li:visible').length === 0) {
            this.hide();
        }

        this.cs.doUp();
    }
}