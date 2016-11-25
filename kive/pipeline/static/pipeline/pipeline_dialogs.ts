
import { RawNode, CdtNode, MethodNode, OutputNode } from "./drydock_objects";
import { CanvasState } from "./drydock";
import 'jquery';

/**
 * Mini jQuery plugin to make dialogs draggable.
 */
$.fn.extend({
    draggable: function(opt) {
        opt = $.extend({ handle: '', cursor: 'normal' }, opt);
        var $el = opt.handle === '' ? this : this.find(opt.handle);
        
        $el.find('input, select, textarea').on('mousedown', function(e) {
            e.stopPropagation();
        });
        
        $el.css('cursor', opt.cursor).on("mousedown", function(e) {
            var $drag = $(this);
            if (opt.handle === '') {
                $drag.addClass('draggable');
            } else {
                $drag.addClass('active-handle').parent().addClass('draggable');
            }
            
            if (typeof opt.start === 'function') {
                opt.start(this);
            }
            
            $drag.data('z', $drag.data('z') || $drag.css('z-index'));
            
            var z = $drag.data('z'),
                pos = $drag.offset(),
                pos_y = pos.top - e.pageY,
                pos_x = pos.left - e.pageX;
            
            $drag.css('z-index', 1000).parents().off('mousemove mouseup').on("mousemove", function(e) {
                $('.draggable').offset({
                    top:  e.pageY + pos_y,
                    left: e.pageX + pos_x
                });
            }).on("mouseup", function() {
                $(this).removeClass('draggable').css('z-index', z);
            });
            
            e.preventDefault(); // disable selection
        }).on("mouseup", function() {
            if (opt.handle === "") {
                $(this).removeClass('draggable');
            } else {
                $(this).removeClass('active-handle').parent().removeClass('draggable');
            }
            if (typeof opt.stop === 'function') {
                opt.stop(this);
            }
        });
        
        return $el;
    }
});


/**
 * Base class for UI dialogs on the pipeline assembly canvas.
 * UI is used to set pipeline metadata, add new nodes, and access other controls.
 */
export class Dialog {
    
    private visible = false;
    
    /**
     * @param jqueryRef
     *      The root element of the dialog as a jQuery object.
     * @param activator
     *      The primary UI control for activating the dialog.
     */
    constructor(public jqueryRef, public activator) {
        activator.click( e => {
            // open this one
            this.show();
            // do not bubble up (which would hit document.click again)
            e.stopPropagation();
        });
        // capture mouse/key events
        jqueryRef.on('click mousedown keydown', e => e.stopPropagation() );
        // esc closes the dialog
        jqueryRef.on('keydown', e => {
            if (e.which === 27) { // esc
                this.cancel();
            }
        });
        // hide this menu if it's visible
        $(document).click( () => { this.visible && this.cancel(); } );
    }
    
    /**
     * Opens the dialog
     */
    show() {
        // close all other menus
        $(document).click();
        this.activator.addClass('clicked');
        this.jqueryRef.show().css('left', this.activator.offset().left);
        this.focusFirstEmptyInput();
        this.visible = true;
    }
    
    /**
     * Closes the dialog
     */
    hide() {
        this.activator.removeClass('clicked');
        this.jqueryRef.hide();
        this.visible = false;
    }
    
    /**
     * Focuses the first unfilled input field
     */
    focusFirstEmptyInput() {
        this.jqueryRef.find('input, select').each(function() {
            if (this.value === '') {
                $(this).focus();
                return false; // break;
            }
        });
    }
    
    /**
     * Clears all inputs
     * Child classes should extend this functionality.
     */
    reset() {
        this.jqueryRef.find('input[type="text"], textarea, select').val('');
    }
    
    /**
     * Hides by default - child classes may choose to have this reset the dialog as well.
     */
    cancel() {
        this.hide();
    }
    
    validateInitialization() {
        for (let propertyName in this) {
            if (propertyName[0] === "$" && this[propertyName].constructor === $) {
                if (this[propertyName].length === 0) {
                    throw "Error in dialog: could not find " + this[propertyName].selector + " in template";
                }
            }
        }
    }
}

/**
 * Currently no functionality added on top of normal dialog. This may change in the future.
 */
// class PipelineFamilyDialog extends Dialog {
//
// }

/**
 * Currently no functionality added on top of normal dialog. This may change in the future.
 */
// class PipelineDialog extends Dialog {
//
// }

/**
 * Middle-base class for dialogs incorporating a Node preview canvas.
 */
abstract class NodePreviewDialog extends Dialog {
    protected preview_canvas: HTMLCanvasElement;
    protected is_modal = true;
    
    /**
     * NodePreviewDialogs have a <canvas> element and are draggable.
     * @param jqueryRef
     *      The root element of the dialog as a jQuery object.
     * @param activator
     *      The primary UI control for activating the dialog.
     */
    constructor(jqueryRef, activator) {
        super(jqueryRef, activator);
        if (jqueryRef.draggable) {
            jqueryRef.draggable();
        }
        this.preview_canvas = <HTMLCanvasElement> $('canvas', jqueryRef)[0];
        this.preview_canvas.width = jqueryRef.width();
        this.preview_canvas.height = 60;
    }
    
    /**
     * Converts the coords of the preview canvas to the coords of another CanvasState.
     * @param otherCanvasState
     *      the other canvas to translate the node's coords to.
     * @returns {{left: number, top: number}}
     *      the coords that should give an identical page position on otherCanvasState.
     */
    protected translateToOtherCanvas(otherCanvasState: CanvasState) {
        let pos: {left: number, top: number} = $(this.preview_canvas).offset();
        if (this.preview_canvas && pos) {
            pos.left += this.preview_canvas.width  / 2 - otherCanvasState.canvas.offsetLeft;
            pos.top  += this.preview_canvas.height / 2 - otherCanvasState.canvas.offsetTop;
        } else {
            pos.left = 100;
            pos.top  = 200 + Math.round(50 * Math.random());
        }
        return pos;
    }
    
    /**
     * Sync the preview canvas with dialog inputs.
     * Child classes must implement.
     */
    protected abstract triggerPreviewRefresh(): void;
    
    /**
     * Show the dialog.
     * NodePreviewDialogs are modal, so they are not positioned relative to their activator.
     */
    show() {
        super.show();
        this.jqueryRef.css({
            top: 300,
            left: 300
        });
        this.triggerPreviewRefresh();
    }
    
    /**
     * Reset the preview canvas and clear all inputs (via super)
     */
    reset() {
        super.reset();
        // this has the side-effect of clearing the canvas.
        this.clearPreview();
    }
    
    /**
     * Hide and reset all in one.
     */
    cancel() {
        this.hide();
        this.reset();
    }
    
    /**
     * Clear canvas
     */
    protected clearPreview() {
        this.preview_canvas.width = this.preview_canvas.width;
    }
}

/**
 * Dialog for adding new InputNodes to canvas.
 * Includes name and compound datatype.
 */
export class InputDialog extends NodePreviewDialog {
    /*
     * Shorthand HTML Template pasted here for convenience.
     * It is not guaranteed to be current.
     * Indentation takes the place of closing tags.

    <div #id_input_ctrl .ctrl_menu>
        <canvas>
        <h3>Inputs</h3>
        <form>
            <input #id_datatype_name type="text">
            <select #id_select_cdt .select-label>
                <!-- compound data type <option>s -->
            <input #id_cdt_button type="submit" value="Add Input">
            <div #id_dt_error .errortext>

    */
    private $datatype_name;
    private $select_cdt;
    private $error;
    
    /**
     * In addition to the NodePreviewDialog functionality,
     * InputDialog will wire up all the necessary template elements.
     * @param jqueryRef
     *      The root element of the dialog as a jQuery object.
     * @param activator
     *      The primary UI control for activating the dialog.
     */
    constructor(jqueryRef, activator) {
        super(jqueryRef, activator);
        let dialog = this;
        this.$datatype_name = $('#id_datatype_name');
        this.$error = $('#id_dt_error');
        this.$select_cdt = $('#id_select_cdt');
        this.$select_cdt.change(function(e) {
            e.stopPropagation();
            dialog.triggerPreviewRefresh();
        });
    }
    
    /**
     * Update the preview canvas.
     */
    triggerPreviewRefresh() {
        this.drawPreviewCanvas();
    }
    
    /**
     * Align the dialog to a given coord. Anchor point is center of the dialog.
     * @param x
     *      The x-coordinate.
     * @param y
     *      The y-coordinate.
     */
    align(x: number, y: number): void {
        this.jqueryRef.css({
            left: x - this.jqueryRef.innerWidth()  / 2,
            top:  y - parseInt(this.jqueryRef.css('padding-top'), 10)
        });
    }
    
    /**
     * Creates a node object based on the dialog state.
     * Coords default to 0, 0 and label is an empty string.
     * @returns {RawNode|CdtNode}
     */
    generateNode(label: string = ''): RawNode|CdtNode {
        let pk = parseInt(this.$select_cdt.val(), 10); // primary key
        return isNaN(pk) ? new RawNode(0, 0, label) : new CdtNode(pk, 0, 0, label);
    }
    
    /**
     * Draws the node on the preview canvas.
     */
    drawPreviewCanvas (): void {
        let ctx = this.preview_canvas.getContext('2d');
        let w = this.preview_canvas.width;
        let h = this.preview_canvas.height;
        ctx.clearRect(0, 0, w, h);
        let node = this.generateNode();
        node.x = w / 2;
        node.y = h / 2;
        node.draw(ctx);
    }
    
    /**
     * Adds a new node to canvasState based on the InputDialog state. Calculates the corresponding coordinate position,
     * checks for name uniqueness, and detects shape collisions. If successful the dialog is reset and closed.
     * @param canvasState
     */
    submit(canvasState: CanvasState) {
        let pos = this.translateToOtherCanvas(canvasState);
        
        // check for empty and duplicate names
        let node_label = this.$datatype_name.val();
        if (node_label === '') {
            // required field
            this.$error.text("Label is required.");
        } else if (!CanvasState.isUniqueName(canvasState.getInputNodes(), node_label)) {
            this.$error.text('That name has already been used.');
        } else {
            let shape = this.generateNode(node_label);
            shape.x = pos.left;
            shape.y = pos.top;
            
            canvasState.addShape(shape);
            // Second arg: Upon collision, move new shape 0% and move existing objects 100%
            canvasState.detectCollisions(shape, 0);

            this.reset(); // reset text field
            this.hide();
        }
    }
    
    /**
     * Clears the dialog state.
     */
    reset() {
        super.reset();
        this.$error.text('');
    }
}

/**
 * Singleton UI for picking a colour.
 */
var colourPickerFactory = (function() {
    /**
     * Template pasted here for convenience.
     
     <input #id_select_colour type="hidden">
     <div #colour_picker_menu>
     <div .colour_picker_colour style="background-color: #999;">
     <!-- ... more colours ... -->
     <div #colour_picker_pick .colour_picker_colour style="background-color: #999;">
     
     */
    // Private members
    var $hidden_input = $('#id_select_colour');
    var $pick = $('#colour_picker_pick').click( () => picker.show() );
    var $menu = $('#colour_picker_menu')
        .on('click', 'div', function() {
            picker.pick($(this).css('background-color'));
        });
    var callback: Function = () => {};

    // Exposed methods
    var picker = {
    
        /**
         * colourPicker.show
         * Displays the available choices.
         */
        show: function() {
            var pos = $pick.position();
            $menu.css({ top: pos.top + 20, left: pos.left }).show();
        },
    
        /**
         * colourPicker.pick
         * Sets the current colour choice.
         * @param colour
         *      the colour to choose as a hexadecimal string.
         */
        pick: function(colour) {
            $pick.css('background-color', colour);
            $hidden_input.val(colour);
            $menu.hide();
            callback(colour);
        },
    
        /**
         * colourPicker.val
         * @return the currently picked colour
         */
        val: () => $hidden_input.val(),
    
        /**
         * colourPicker.setCallback
         * set a function to execute when a colour is picked.
         */
        setCallback: (cb: Function) => { callback = cb; }
    };
    return picker;
});

export class MethodDialog extends NodePreviewDialog {
    /*
     * Shorthand HTML Template pasted here for convenience.
     * It is not guaranteed to be current.
     * Indentation takes the place of closing tags.

    <div #id_method_ctrl .ctrl_menu>
        <canvas>
        <h3>Methods</h3>
        <form>
            <input #id_select_colour type="hidden">
            <div #colour_picker_menu>
                <div .colour_picker_colour style="background-color: #999;">
                <!-- ... more colours ... -->
            <div .colour_picker_colour #colour_picker_pick style="background-color: #999;">

            <input #id_method_name type="text" placeholder="Label">

            <select #id_select_method_family>
                <!-- ... method families ... -->
            <div #id_method_revision_field>
                <select #id_select_method>
                    <!-- populated by ajax transaction -->

            <div #id_method_delete_outputs_field>
                <input #id_method_delete_outputs type="checkbox" checked>
                <label for="id_method_delete_outputs">Save intermediate outputs of this step</label>
                <div .expand_outputs_ctrl>▸ List outputs</div>
                <fieldset #id_method_delete_outputs_details>
                    <!-- ... <input type="checkbox"> ... -->

            <input #id_method_button type="submit" value="Add Method">
            <div #id_method_error .errortext>
    */

    private $delete_outputs;
    private $delete_outputs_details;
    private $submit_button;
    private $select_method;
    private $select_method_family;
    private $input_name;
    private $error;
    private $expand_outputs_ctrl;
    private colour_picker;
    private add_or_revise: string = "add";
    private editing_node: MethodNode;
    private methodInputs: any[];
    private methodOutputs: any[];
    private cached_api_result: any;
    
    /**
     * In addition to the NodePreviewDialog functionality, MethodDialog will wire up all the necessary template
     * elements. It also sets event watchers on UI which is internal to the dialog. Finally, it initializes the method
     * revisions menu which is an asynchronous event.
     * @param jqueryRef
     *      The root element of the dialog as a jQuery object.
     * @param activator
     *      The primary UI control for activating the dialog.
     */
    constructor(jqueryRef, activator) {
        super(jqueryRef, activator);
        
        this.colour_picker = colourPickerFactory(); // not a class-based object - note no "new"
        this.$delete_outputs = $('#id_method_delete_outputs');
        this.$delete_outputs_details = $('#id_method_delete_outputs_details');
        this.$submit_button = $('#id_method_button');
        this.$select_method = $("#id_select_method");
        this.$select_method_family = $('#id_select_method_family');
        this.$input_name = $('#id_method_name');
        this.$error = $('#id_method_error');
        this.$expand_outputs_ctrl = $('.expand_outputs_ctrl', this.jqueryRef);

        this.$select_method.change(
            () => this.triggerPreviewRefresh()
        );
        this.colour_picker.setCallback(
            () => this.triggerPreviewRefresh()
        );
        let dialog = this;
        this.$select_method_family.change( function() {
            dialog.updateMethodRevisionsMenu(this.value);
        });
        this.$delete_outputs.change(
            () => {
                this.linkChildCheckboxes();
                this.refreshPreviewCanvasMagnets();
            }
        );
        this.$delete_outputs_details.on('change', '.method_delete_outputs',
            () => {
                this.linkParentCheckbox();
                this.refreshPreviewCanvasMagnets();
            }
        );
        this.$expand_outputs_ctrl.click(
            () => this.showHideChildCheckboxes()
        );

        this.updateMethodRevisionsMenu(this.$select_method_family.val());
        this.linkChildCheckboxes();
    }
    
    /**
     * Update the preview canvas based on the dialog state.
     */
    protected triggerPreviewRefresh() {
        let value = this.$select_method.val();
        if (value) {
            // Update preview picture of node to show the appropriate MethodNode
            // use AJAX to retrieve Revision inputs and outputs
            return $.getJSON("/api/methods/" + value + "/").done(result => {
                if (!result) {
                    console.error("Couldn't find PK", result);
                }
                this.methodInputs = result.inputs;
                this.methodOutputs = result.outputs;
                this.cached_api_result = result;
                this.setOutputsFieldsetList(result.outputs);
                if (this.editing_node) {
                    this.setOutputsToDeleteFromEditingNode();
                }
                this.drawPreviewCanvas(result, this.colour_picker.val());
            });
        }
        return $.Deferred().fail();
    }
    
    private refreshPreviewCanvasMagnets() {
        this.drawPreviewCanvas(this.cached_api_result, this.colour_picker.val());
    }
    
    /**
     * Align the dialog to a given coord. Anchor point is top center.
     * @param x
     *      The x-coordinate.
     * @param y
     *      The y-coordinate.
     */
    align(x: number, y: number): void {
        this.jqueryRef.css({
            left: x - this.preview_canvas.width / 2,
            top: y
        });
    }
    
    /**
     * Loads a MethodNode into the MethodDialog so that the user may edit.
     * @param node
     *      The MethodNode to revise.
     */
    load(node: MethodNode): void {
        this.reset();
        this.$select_method_family.val(node.family);
        this.colour_picker.pick(node.fill);
        this.setToRevise();
        this.editing_node = node;

        let request = this.updateMethodRevisionsMenu(node.family); // trigger ajax

        // disable forms while ajax is loading
        this.jqueryRef.find('input').prop('disabled', true);

        request.done(() => {
            /**
             * @todo
             * Move this logic somewhere else (and find out what it does)
             */
            if (node.new_code_resource_revision || (node.new_dependencies && node.new_dependencies.length > 0)) {
                let msgs = [];
                if (node.new_code_resource_revision) {
                    msgs.push('driver updated (' +
                        node.new_code_resource_revision.revision_name +
                        ')');
                }
                if (node.new_dependencies && node.new_dependencies.length > 0) {
                    msgs.push('dependencies updated (' +
                        node.new_dependencies.map(el => el.revision_name).join(', ') +
                        ')');
                }

                let opt = $('<option>')
                    .val(node.pk)
                    .text('new: ' + '[' + msgs.join('; ') + ']');

                this.$select_method.prepend(opt);
            }

            this.jqueryRef.find('input').prop('disabled', false);

            // wait for AJAX to populate drop-down before selecting option
            this.$select_method.val(node.pk);
            this.$input_name.val(node.label).select();
        });
    }
    
    setOutputsToDeleteFromEditingNode() {
        let editing_node = this.editing_node;
        this.$delete_outputs_details.find('input').each(function() {
            $(this).prop('checked', -1 === editing_node.outputs_to_delete.indexOf(this.value) );
        });
        this.linkParentCheckbox();
        // this.refreshPreviewCanvasMagnets();
    }
    
    /**
     * Update the "save/delete outputs" checkbox to reflect the more granular child checkboxes' state.
     * If all are unchecked, parent will be unchecked; if all checked, parent will be checked; in all other cases,
     * parent will be set to "indeterminate".
     */
    private linkParentCheckbox() {
        var siblings = this.$delete_outputs_details.find('input'),
            checked_inputs = siblings.filter(':checked').length,
            prop_obj: {indeterminate: boolean, checked?: boolean} = { indeterminate: false };
        
        if (checked_inputs < siblings.length && checked_inputs > 0) {
            prop_obj.indeterminate = true;
        } else {
            prop_obj.checked = (checked_inputs !== 0);
        }
        this.$delete_outputs.prop(prop_obj);
    }
    
    /**
     * Update the granular "save/delete a specified output" checkboxes to reflect the more general parent checkbox.
     * Child checkboxes take on the parent's value.
     */
    private linkChildCheckboxes() {
        this.$delete_outputs_details.find('input')
            .prop('checked', this.$delete_outputs.is(':checked'));
    }
    
    /**
     * Toggle the visibility of the $delete_outputs_details group.
     */
    private showHideChildCheckboxes() {
        if (this.$delete_outputs_details.is(':visible')) {
            this.hideChildCheckboxes();
        } else {
            this.$delete_outputs_details.show();
            this.$expand_outputs_ctrl.text('▾ Hide list');
        }
    }
    
    /**
     * Hide the $delete_outputs_details group.
     */
    private hideChildCheckboxes() {
        this.$delete_outputs_details.hide();
        this.$expand_outputs_ctrl.text('▸ List outputs');
    }
    
    /**
     * Update the revisions menu.
     * @param mf_id
     *      The method family ID
     * @returns
     *      A jQuery Deferred object
     */
    private updateMethodRevisionsMenu(mf_id): JQueryPromise<void> {
        if (mf_id !== '') {
            // this.$revision_field.show().focus();
            let request = $.getJSON("/api/methodfamilies/" + mf_id + "/methods/");
            return request.done(result => {
                let option_elements = result.map(revision =>
                    $("<option>", {
                        value: revision.id,
                        title: revision.revision_desc
                    }).text(
                        revision.revision_number + ': ' + revision.revision_name
                    )
                );
                this.$select_method.show().empty()
                    .append(option_elements);
                this.triggerPreviewRefresh();
            });
        }
        // this.$revision_field.hide();
        return $.ajax({}).fail(); // No method family chosen, never loads.
    }
    
    /**
     * Sets the outputs fieldset list.
     * @param outputs
     *      An array of the method's outputs. <exact type unknown>
     *      Each includes object properties dataset_idx and dataset_name.
     */
    private setOutputsFieldsetList (outputs: any[]): void {
        this.$delete_outputs.prop('checked', true);
        this.$delete_outputs_details.empty();
        let elements = [];
        for (let output of outputs) {
            elements.push(
                $('<input>', {
                    type: 'checkbox',
                    name: 'dont_delete_outputs',
                    'class': 'method_delete_outputs',
                    id: 'dont_delete_outputs_' + output.dataset_idx,
                    value: output.dataset_name,
                    checked: 'checked'
                }),
                $('<label>')
                    .attr('for', 'dont_delete_outputs_' + output.dataset_idx)
                    .text(output.dataset_name),
                $('<br>')
            );
        }
        this.$delete_outputs_details.append(elements);
    }
    
    private static setOutputsToDelete(method: MethodNode, outputs: string[]): MethodNode {
        method.outputs_to_delete = outputs;
        for (let magnet of method.out_magnets) {
            magnet.toDelete = method.outputs_to_delete.indexOf(magnet.label) > -1;
        }
        return method;
    }
    
    /**
     * Given data from the REST API, draw a MethodNode on the preview canvas.
     * @param api_method_result
     * @param colour
     */
    private drawPreviewCanvas (api_method_result, colour?: string): void {
        let n_outputs = Object.keys(api_method_result.outputs).length * 8;
        let n_inputs  = Object.keys(api_method_result.inputs).length * 8 + 14;
    
        this.clearPreview();
    
        this.preview_canvas.height = (n_outputs + n_inputs) / 2 + 55;
        
        let method = MethodDialog.setOutputsToDelete(
            new MethodNode(
                api_method_result.pk,
                null, // family
                // Ensures node is centred perfectly on the preview canvas
                // For this calculation to be accurate, method node draw params cannot change.
                this.preview_canvas.width / 2 -
                (
                    Math.max(0, n_outputs - n_inputs + 48) -
                    Math.max(0, n_outputs - n_inputs - 42)
                ) * 0.4330127, // x
                n_inputs / 2 + 20, // y
                colour,
                null, // label
                api_method_result.inputs,
                api_method_result.outputs
            ),
            this.$delete_outputs_details.find('input').get()
                .filter( el => !$(el).prop('checked') )
                .map( el => el.value )
        );
        
        method.draw(this.preview_canvas.getContext('2d'));
    }
    
    /**
     * Wrapper for the MethodNode constructor that will also set MethodNode.outputs_to_delete based on the dialog state.
     * @todo See if MethodDialog.drawPreviewCanvas can use this function. (currently only MethodDialog.submit does)
     * @params See MethodNode documentation.
     * @returns MethodNode
     */
    private produceMethodNode(id, family, x, y, colour, label, inputs, outputs): MethodNode {
        return MethodDialog.setOutputsToDelete(
            new MethodNode(id, family, x, y, colour, label, inputs, outputs),
            this.$delete_outputs_details.find('input').get()
                .filter( el => !$(el).prop('checked') )
                .map( el => el.value )
        );
    }
    
    /**
     * Adds the MethodNode represented by the current state to the supplied CanvasState.
     * @param canvasState
     *      The CanvasState to add the MethodNode to.
     */
    submit(canvasState: CanvasState) {
        let node_label = this.$input_name.val(); // pk of method
        let method_id = this.$select_method.val();
        let family_id = this.$select_method_family.val();
        let pos = this.translateToOtherCanvas(canvasState);
        
        if (method_id && family_id && node_label) {
            // user selected valid Method Revision
            var method = this.produceMethodNode(
                method_id,
                this.$select_method_family.val(),
                pos.left,
                pos.top,
                this.colour_picker.val(),
                node_label,
                this.methodInputs,
                this.methodOutputs
            );
            if (this.add_or_revise === 'add') {
                // create new MethodNode
                canvasState.addShape(method);
            } else {
                // replace the selected MethodNode
                // draw new node over old node
                canvasState.replaceMethod(this.editing_node, method);
                canvasState.selection = [ method ];
            }
            this.hide();
            this.reset();
        } else if (!node_label) {
            // required field
            this.$error.text("Label is required");
            this.$input_name.focus();
        } else {
            this.$error.text("Select a method");
            if (family_id) {
                this.$select_method.focus();
            } else {
                this.$select_method_family.focus();
            }
        }
    }
    
    /**
     * Clears all fields and private variables for future use.
     */
    reset() {
        super.reset();
        this.$error.text('');
        this.hideChildCheckboxes();
        this.$select_method.empty();
        this.setToAdd();
        this.editing_node = null;
        this.methodInputs = null;
        this.methodOutputs = null;
        this.cached_api_result = null;
    }
    
    /**
     * Sets the dialog to add a new method on submit.
     */
    private setToAdd() {
        this.$submit_button.val('Add Method');
        this.add_or_revise = "add";
    }
    
    /**
     * Sets the dialog to replace a method on submit rather than create a new one.
     */
    private setToRevise() {
        this.$submit_button.val('Revise Method');
        this.add_or_revise = "revise";
    }
}

export class OutputDialog extends NodePreviewDialog {
    /*
     * Shorthand HTML Template pasted here for convenience.
     * It is not guaranteed to be current.
     * Indentation takes the place of closing tags.

    <div #id_output_ctrl .ctrl_menu>
        <canvas>
        <h3>Outputs</h3>
        <form>
            <input #id_output_name type="text">
            <input #id_output_button type="submit" value="OK">
            <div #id_output_error .errortext>
     */
    private $error;
    private $output_name;
    private paired_node: OutputNode;
    
    /**
     * In addition to the NodePreviewDialog functionality, OutputDialog will wire up all the necessary template
     * elements.
     * @param jqueryRef
     *      The root element of the dialog as a jQuery object.
     * @param activator
     *      The primary UI control for activating the dialog.
     */
    constructor(jqueryRef, activator) {
        super(jqueryRef, activator);
        this.$error = $("#id_output_error");
        this.$output_name = $('#id_output_name');
        this.drawPreviewCanvas();
    }
    
    /* The following is a hack to get around inconvenient document.click event timing. */
    cancel_: any;
    makeImmune() {
        this.cancel_ = this.cancel;
        this.cancel = function() {
            this.cancel = this.cancel_;
        };
    }
    
    /**
     * Draws an OutputNode on the preview canvas. No need to check the details, all OutputNodes look the same.
     */
    drawPreviewCanvas(): void {
        let ctx = this.preview_canvas.getContext('2d');
        let w = this.preview_canvas.width;
        let h = this.preview_canvas.height;
        let node = new OutputNode(w / 2, h / 2, '');
        ctx.clearRect(0, 0, w, h);
        node.draw(ctx);
    }

    /**
     * Implements abstract method
     */
    triggerPreviewRefresh(): void {
        // outputs all look the same: take no action
    }
    
    /**
     * Load an existing OutputNode so that we can rename it.
     * @param node
     *      The OutputNode to edit
     */
    load(node: OutputNode): void {
        this.reset();
        this.paired_node = node;
        this.$output_name.val(node.label).select(); // default value
    }
    
    /**
     * Align the dialog to a given coord. Anchor point is center of the dialog.
     * @param x
     *      The x-coordinate.
     * @param y
     *      The y-coordinate.
     */
    align(x: number, y: number): void {
        this.jqueryRef.css({
            left: x - this.jqueryRef.innerWidth()  / 2,
            top:  y - parseInt(this.jqueryRef.css('padding-top'), 10)
        });
    }
    
    /**
     * Renames the OutputNode.
     * @param canvasState
     *      The CanvasState to operate on (currently only used to trigger a redraw)
     */
    submit(canvasState: CanvasState) {
        var label = this.$output_name.val();
        if (this.paired_node) {
            if (this.paired_node.label === label) {
                /* No change */
                this.hide();
                this.reset();
            } else if (CanvasState.isUniqueName(canvasState.getOutputNodes(), label)) {
                /* Name is changed and valid */
                this.paired_node.setLabel(label);
                canvasState.valid = false;
                this.hide();
                this.reset();
            } else {
                /* Non-unique name entered */
                this.$error.html('<img src="/static/pipeline/warning_icon.png"> That name has already been used.');
            }
        } else {
            let pos = this.translateToOtherCanvas(canvasState);
    
            // check for empty and duplicate names
            let node_label = this.$output_name.val();
            if (node_label === '') {
                // required field
                this.$error.text("Label is required.");
            } else if (!CanvasState.isUniqueName(canvasState.getOutputNodes(), node_label)) {
                this.$error.html('<img src="/static/pipeline/warning_icon.png"> That name has already been used.');
            } else {
                let shape = new OutputNode(pos.left, pos.top, node_label);
                canvasState.addShape(shape);
                // Second arg: Upon collision, move new shape 0% and move existing objects 100%
                canvasState.detectCollisions(shape, 0);
        
                this.reset(); // reset text field
                this.hide();
            }
        }
    }
    
    /**
     * Clears all inputs and private members for future use.
     */
    reset() {
        this.$output_name.val('');
        this.$error.empty();
        this.paired_node = null;
    }
    
    /**
     * Closes the dialog and removes the working from the CanvasState.
     * (Assumes the user got here by dragging an Connector into the canvasState's OutputZone.)
     * @param canvasState
     */
    cancel(canvasState?: CanvasState) {
        super.cancel();
        if (this.paired_node && canvasState) {
            canvasState.connectors.pop();
            canvasState.valid = false;
        }
    }
}

/**
 * Dialog controlling how to arrange and display Nodes on the canvasState.
 */
export class ViewDialog extends Dialog {
    
    private static execOrderDisplayOptions = { always: true, never: false, ambiguous: undefined };
    
    /**
     * Change whether canvasState shows order numbers on MethodNodes.
     * @param canvasState
     * @param value One of 3 configuration options for canvasState.force_show_exec_order.
     */
    static changeExecOrderDisplayOption (canvasState: CanvasState, value: "always"|"never"|"ambiguous") {
        if (ViewDialog.execOrderDisplayOptions.hasOwnProperty(value)) {
            canvasState.force_show_exec_order = ViewDialog.execOrderDisplayOptions[value];
            canvasState.valid = false;
        }
    }
    
    /**
     * Align nodes along an axis.
     * @param canvasState
     * @param axis A string from "x"|"y"|"iso_x"|"iso_y"
     */
    static alignCanvasSelection (canvasState: CanvasState, axis: "x"|"y"|"iso_x"|"iso_y") {
        canvasState.alignSelection(axis);
    }
    
}