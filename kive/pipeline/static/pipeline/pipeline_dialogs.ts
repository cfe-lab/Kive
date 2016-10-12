
import { RawNode, CdtNode, MethodNode, OutputNode } from "./drydock_objects";
import { CanvasState } from "./drydock";
declare var $: any;

export class Dialog {
    constructor(public jqueryRef, public activator) {
        activator.click( () => this.show() );
    }
    
    show() {
        this.activator.addClass('clicked');
        this.jqueryRef.show().css('left', this.activator.offset().left);
        this.focusFirstEmptyInput();
    }
    hide() {
        this.activator.removeClass('clicked');
        this.jqueryRef.hide();
    }
    focusFirstEmptyInput() {
        this.jqueryRef.find('input, select').each(function() {
            if (this.value === '') {
                $(this).focus();
                return false; // break;
            }
        });
    }
    reset() {}
}
class PipelineFamilyDialog extends Dialog {
    
}
class PipelineDialog extends Dialog {
    
}
class NodePreviewDialog extends Dialog {
    protected preview_canvas: HTMLCanvasElement;
    protected is_modal = true;

    constructor(jqueryRef, activator) {
        super(jqueryRef, activator);
        if (jqueryRef.draggable) {
            jqueryRef.draggable();
        }
        this.preview_canvas = $('canvas', jqueryRef)[0];
        this.preview_canvas.width = jqueryRef.innerWidth();
        this.preview_canvas.height = 60;
    }

    translateToOtherCanvas(otherCanvasState) {
        let pos = $(this.preview_canvas).offset();
        if (this.preview_canvas && pos) {
            pos.left += this.preview_canvas.width  / 2 - otherCanvasState.canvas.offsetLeft;
            pos.top  += this.preview_canvas.height / 2 - otherCanvasState.canvas.offsetTop;
        } else {
            pos.left = 100;
            pos.top  = 200 + Math.round(50 * Math.random());
        }
        return pos;
    }

    triggerPreviewRefresh(): void { }
    
    show() {
        this.activator.addClass('clicked');
        this.jqueryRef.show().css( {
            'top': 300,
            'left': 300
        });
        this.focusFirstEmptyInput();
        this.triggerPreviewRefresh();
    }
}
export class InputDialog extends NodePreviewDialog {
    private $datatype_name;
    private $select_cdt;
    private $error;

    constructor(jqueryRef, activator) {
        super(jqueryRef, activator);
        let dialog = this;
        this.$datatype_name = $('#id_datatype_name');
        this.$error = $('#id_dt_error');
        this.$select_cdt = $('#id_select_cdt');
        this.$select_cdt.change(function(e) {
            e.stopPropagation();
            dialog.drawPreviewCanvas(this.value);
        });
    }

    triggerPreviewRefresh() {
        this.$select_cdt.change();
    }
    
    drawPreviewCanvas (node_pk?: string): void {
        node_pk = node_pk || this.$select_cdt.val();
        let ctx = this.preview_canvas.getContext('2d');
        let w = this.preview_canvas.width;
        let h = this.preview_canvas.height;
        let node = node_pk === '' ?
            new RawNode(w / 2, h / 2) :
            new CdtNode(node_pk, w / 2, h / 2);
        ctx.clearRect(0, 0, w, h);
        node.draw(ctx);
    }
    
    submit(e, canvasState) {
        e.preventDefault(); // stop default form submission behaviour
        
        let pos = this.translateToOtherCanvas(canvasState);
        
        // check for empty and duplicate names
        let node_label = this.$datatype_name.val();
        if (node_label === '') {
            // required field
            this.$error.text("Label is required.");
        } else if (!CanvasState.isUniqueName(canvasState.getInputNodes(), node_label)) {
            this.$error.text('That name has already been used.');
        } else {
            let pk = parseInt(this.$select_cdt.val(), 10); // primary key
            let shape = isNaN(pk) ?
                new RawNode(pos.left, pos.top, node_label) :
                new CdtNode(pk, pos.left, pos.top, node_label);

            canvasState.addShape(shape);
            // Second arg: Upon collision, move new shape 0% and move existing objects 100%
            canvasState.detectCollisions(shape, 0);

            this.reset(); // reset text field
            this.hide();
        }
    }

    reset() {
        super.reset();
        this.$error.text('');
    }
}

class ColourPicker {

    private $hidden_input;
    private $menu;
    private $pick;
        
    constructor() {
        var picker = this;
        this.$hidden_input = $('#id_select_colour');
        this.$menu = $('#colour_picker_menu');
        this.$pick = $('#colour_picker_pick');

        this.$pick.click( () => this.show() );
        this.$menu.on('click', 'div', function() {
            picker.pick($(this).css('background-color'));
        });
    }

    show() {
        var pos = this.$pick.position();
        this.$menu.css({ top: pos.top + 20, left: pos.left }).show();
    }
    pick(colour) {
        this.$pick.css('background-color', colour);
        this.$hidden_input.val(colour);
        this.$menu.hide();
    }
    val() {
        return this.$hidden_input.val();
    }
}

export class MethodDialog extends NodePreviewDialog {

    private $delete_outputs;
    private $delete_outputs_details;
    private $submit_button;
    private $revision_field;
    private $select_method;
    private $select_method_family;
    private $input_name;
    private $error;
    private $expand_outputs_ctrl;
    private colour_picker;
    public add_or_revise: string = "add";
    private editing_node: MethodNode;
    
    constructor(jqueryRef, activator) {
        super(jqueryRef, activator);
        
        this.colour_picker = new ColourPicker();
        this.$delete_outputs = $('#id_method_delete_outputs');
        this.$delete_outputs_details = $('#id_method_delete_outputs_details');
        this.$submit_button = $('#id_method_button');
        this.$revision_field = $('#id_method_revision_field');
        this.$select_method = $("#id_select_method");
        this.$select_method_family = $('#id_select_method_family');
        this.$input_name = $('#id_method_name');
        this.$error = $('#id_method_error');
        this.$expand_outputs_ctrl = $('.expand_outputs_ctrl', this.jqueryRef)

        let dialog = this;
        this.$select_method.change( function() {
            dialog.methodSelectMenuHook(this.value);
        });
        this.$select_method_family.change( function() {
            dialog.updateMethodRevisionsMenu(this.value);
        });
        this.$delete_outputs.change( () => dialog.linkChildCheckboxes() )
        this.$expand_outputs_ctrl.click( () => dialog.childCheckboxVisibilityCtrl() )
        this.$delete_outputs_details.on('change',  '.method_delete_outputs', () => dialog.linkParentCheckbox() );

        this.updateMethodRevisionsMenu(this.$select_method_family.val());
        this.linkChildCheckboxes();

        this.preview_canvas.width = jqueryRef.innerWidth();
    }

    triggerPreviewRefresh() {
        this.$select_method.change();
    }

    align(x, y) {
        this.jqueryRef.css({
            left: x - this.preview_canvas.width/2,
            top: y
        })
    }

    load(node: MethodNode) {
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
            this.$revision_field.find('select').val(node.pk);
            this.$input_name.val(node.label).select();
            this.$delete_outputs_details.find('input').each(function() {
                $(this).prop('checked', -1 === node.outputs_to_delete.indexOf(this.value) );
            });
            this.linkParentCheckbox();
        })
    }

    show() {
        super.show();
    }
    linkParentCheckbox() {
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
    
    linkChildCheckboxes() {
        this.$delete_outputs_details.find('input')
            .prop('checked', this.$delete_outputs.is(':checked'));
    }
    
    childCheckboxVisibilityCtrl() {
        if (this.$delete_outputs_details.is(':visible')) {
            this.hideChildCheckboxes();
        } else {
            this.$delete_outputs_details.show();
            this.$expand_outputs_ctrl.text('▾ Hide list');
        }
    }
    hideChildCheckboxes() {
        this.$delete_outputs_details.hide();
        this.$expand_outputs_ctrl.text('▸ List outputs');
    }
    
    updateMethodRevisionsMenu(mf_id) {
        if (mf_id !== '') {
            this.$revision_field.show().focus();
            return $.getJSON(
                "/api/methodfamilies/" + mf_id + "/methods/",
                result => {
                    var option_elements = result.map(revision =>
                        $("<option>", {
                            value: revision.id,
                            title: revision.revision_desc
                        }).text(
                            revision.revision_number + ': ' + revision.revision_name
                        )
                    );
                    this.$select_method
                        .show()
                        .empty()
                        .append(option_elements)
                        .change();
                }
            );
        }
        this.$revision_field.hide();
        return $.Deferred().reject(); // No method family chosen, never loads.
    }
    
    updateOutputsFieldsetList (outputs: any[], fieldset_element, checkbox_element): void {
        /*
         * Update outputs fieldset list
         */
        fieldset_element.empty();
        checkbox_element.prop('checked', true);
        for (let output of outputs) {
            fieldset_element.append(
                $('<input>', {
                    type: 'checkbox',
                    name: 'dont_delete_outputs',
                    'class': 'method_delete_outputs',
                    id: 'dont_delete_outputs_'+ output.dataset_idx,
                    value: output.dataset_name,
                    checked: 'checked'
                }),
                $('<label>')
                    .attr('for', 'dont_delete_outputs_'+ output.dataset_idx)
                    .text(output.dataset_name),
                $('<br>')
            );
        }
    }
    
    methodSelectMenuHook(value) {
        if (value) {
            // Update preview picture of node to show the appropriate MethodNode
            // use AJAX to retrieve Revision inputs and outputs
            $.getJSON("/api/methods/" + value + "/").done(result => {
                if (!result) {
                    console.error("Couldn't find PK", result);
                }
                this.drawPreviewCanvas(
                    result,
                    this.colour_picker.val()
                );
                this.updateOutputsFieldsetList(
                    result.outputs,
                    this.$delete_outputs_details,
                    this.$delete_outputs
                );
            });
        }
    }
    
    drawPreviewCanvas (api_method_result?, colour?: string): void {
        let ctx = this.preview_canvas.getContext('2d');
        ctx.clearRect(0, 0, this.preview_canvas.width, this.preview_canvas.height);
        var n_outputs = Object.keys(api_method_result.outputs).length * 8,
            n_inputs  = Object.keys(api_method_result.inputs).length * 8 + 14;
    
        this.preview_canvas.height = (n_outputs + n_inputs) / 2 + 55;
        (new MethodNode(
            api_method_result.pk,
            null,// family
            // Ensures node is centred perfectly on the preview canvas
            // For this calculation to be accurate, method node draw params cannot change.
            this.preview_canvas.width / 2 -
            (
                Math.max(0, n_outputs - n_inputs + 48) -
                Math.max(0, n_outputs - n_inputs - 42)
            ) * 0.4330127,// x
            n_inputs / 2 + 20,// y
            colour,
            null,// label
            api_method_result.inputs,
            api_method_result.outputs
        )).draw(ctx);
    }


    produceMethodNode(id, family, x, y, colour, label, inputs, outputs) {
        let method = new MethodNode(
            id, family, x, y, colour, label, inputs, outputs
        );
        
        method.outputs_to_delete = this.$delete_outputs_details.find('input').get()
            .filter( el => !$(el).prop('checked') )
               .map( el => el.value );

        for (let magnet of method.out_magnets) {
            if (method.outputs_to_delete.indexOf(magnet.label) > -1) {
                magnet.toDelete = true;
            }
        }
        return method;
    }
    
    submit(e, canvasState) {
        e.preventDefault(); // stop default form submission behaviour
    
        // this.$revision_field = $('#id_method_revision_field');
        
        let select_method_family = this.$select_method_family;
        let select_method = this.$select_method;
        let node_label = this.$input_name.val(); // pk of method
        let method_id = select_method.val();
        let pos = this.translateToOtherCanvas(canvasState);
        
        // locally-defined function has access to all variables in parent function scope
        let createOrReplaceMethodNode = result => {
            var method = this.produceMethodNode(
                    method_id,
                    select_method_family.val(),
                    pos.left,
                    pos.top,
                    this.colour_picker.val(),
                    node_label,
                    result.inputs,
                    result.outputs
            );
            if (this.add_or_revise == 'add') {
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
        };
        
        if (method_id !== undefined && select_method_family.val() !== '') {
            // user selected valid Method Revision
            if (node_label !== '') {
                // use AJAX to retrieve Revision inputs and outputs
                $.getJSON("/api/methods/" + method_id + "/")
                    .done(createOrReplaceMethodNode);
            } else {
                // required field
                this.$error.text("Label is required");
                this.$input_name.focus();
            }
        } else {
            this.$error.text("Select a Method");
            if (select_method.is(':visible')) {
                select_method.focus();
            } else {
                select_method_family.focus();
            }
        }
    }
    
    reset() {
        let ctx = this.preview_canvas.getContext('2d');
        ctx.clearRect(0, 0, this.preview_canvas.width, this.preview_canvas.height);
        this.$error.text('');
        this.$input_name.val('');
        this.hideChildCheckboxes();
        this.$select_method_family.val(
            this.$select_method_family.children('option').eq(0).val()
        ).change();
        this.setToAdd();
        this.editing_node = null;
    }

    setToAdd() {
        this.$submit_button.val('Add Method');
        this.add_or_revise = "add";
    }

    setToRevise() {
        this.$submit_button.val('Revise Method');
        this.add_or_revise = "revise";
    }
}
export class OutputDialog extends NodePreviewDialog {
    private $error;
    private $output_name;
    private paired_node: OutputNode;
    
    constructor(jqueryRef, activator) {
        super(jqueryRef, activator);
        this.$error = $("#id_output_error");
        this.$output_name = $('#id_output_name');
        this.drawPreviewCanvas();
    }

    drawPreviewCanvas(): void {
        let ctx = this.preview_canvas.getContext('2d');
        let w = this.preview_canvas.width;
        let h = this.preview_canvas.height;
        let node = new OutputNode(w / 2, h / 2, '');
        ctx.clearRect(0, 0, w, h);
        node.draw(ctx);
    }

    load(node: OutputNode): void {
        this.reset();
        this.paired_node = node;
        this.$output_name.val(node.label).select(); // default value
    }

    align(x, y): void {
        this.jqueryRef.css({
            left: x - this.jqueryRef.width()  / 2,
            top:  y - this.jqueryRef.height() / 2
        });
    }
    
    submit(e, canvasState: CanvasState) {
        // override ENTER key, click Create output button on form
        e.preventDefault();
        var label = this.$output_name.val();
        if (this.paired_node.label == label) {
            this.hide();
            this.reset();
        } else if (CanvasState.isUniqueName(canvasState.getOutputNodes(), label)) {
            this.paired_node.label = label;
            canvasState.selection = [ this.paired_node ];
            canvasState.valid = false;
            this.hide();
            this.reset();
        } else {
            this.$error.html('<img src="/static/pipeline/warning_icon.png"> That name has already been used.');
        }
    }

    reset() {
        this.$output_name.val('');
        this.$error.empty();
    }
    
    cancel(canvasState) {
        this.hide();
        this.$error.hide();
        canvasState.connectors.pop();
        canvasState.valid = false;
    }
}
export class ViewDialog extends Dialog {
    changeExecOrderDisplayOption (canvasState) {
        var $this = $(this),
            val = $this.val(),
            val_map = { always: true, never: false, ambiguous: undefined };
        
        if ($this.is(':checked') && val_map.hasOwnProperty(val)) {
            canvasState.force_show_exec_order = val_map[val];
            canvasState.valid = false;
        }
    };
}