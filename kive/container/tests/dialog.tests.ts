import {MethodDialog, Dialog, InputDialog, OutputDialog} from "@container/pipeline_dialogs";
import {MethodNode, CdtNode, OutputNode} from "@container/canvas/drydock_objects";
import { REDRAW_INTERVAL, CanvasState } from "@container/canvas/drydock";
import * as imagediff from 'imagediff';
import {RawNode} from "@container/canvas/drydock_objects";
import {Container} from "@container/io/PipelineApi";

jasmine.getFixtures().fixturesPath = '/templates/container';
jasmine.getStyleFixtures().fixturesPath = '/static/container';
// noinspection TypeScriptValidateJSTypes
jasmine.getFixtures().preload(
    'content_view_dialog.tpl.html',
    'content_method_dialog.tpl.html',
    'content_input_dialog.tpl.html',
    'content_output_dialog.tpl.html'
);
// noinspection TypeScriptValidateJSTypes
jasmine.getStyleFixtures().preload('drydock.css');

describe("Dialog fixture", function() {
    let dlg;

    beforeEach(function(){
        appendLoadFixtures('content_view_dialog.tpl.html');
        appendLoadStyleFixtures('drydock.css');
        appendSetFixtures("<a id='activator'>Activator</a>");
        dlg = new Dialog(
            $('.ctrl_menu').attr('id', '#id_view_ctrl'),
            $('#activator')
        );
    });

    it('should initialize properly', function() {
        expect(dlg.jqueryRef).toBeInDOM();
        expect(dlg.activator).toBeInDOM();
        try {
            dlg.validateInitialization();
        } catch (e) {
            fail(e);
        }
    });

    it('should show', function() {
        dlg.jqueryRef.hide();
        dlg.show();
        expect(dlg.jqueryRef).toBeVisible();
    });

    it('should hide', function() {
        dlg.jqueryRef.show();
        dlg.hide();
        expect(dlg.jqueryRef).toBeHidden();
    });

    it('should appear after the activator is clicked', function() {
        expect(dlg.jqueryRef).toBeHidden();
        dlg.activator.click();
        expect(dlg.jqueryRef).toBeVisible();
    });

    it('should capture key and mouse events', function() {
        spyOnEvent('body', 'click');
        spyOnEvent('body', 'mousedown');
        spyOnEvent('body', 'keydown');

        dlg.jqueryRef[0].dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
        dlg.jqueryRef[0].dispatchEvent(new KeyboardEvent('keydown', { key: "Enter", bubbles: true, cancelable: true }));

        expect('click').not.toHaveBeenTriggeredOn('body');
        expect('mousedown').not.toHaveBeenTriggeredOn('body');
        expect('keydown').not.toHaveBeenTriggeredOn('body');
    });

});

describe("Container MethodDialog fixture", function() {

    let dlg;
    let $cp_hidden_input;
    let $cp_pick;
    let $cp_menu;
    let $submit_button;
    let $select_method;
    let $input_names;
    let $output_names;
    let $error;
    let $expand_outputs_ctrl;
    let canvas;
    let expected_canvas;
    let expected_ctx;
    let expected_method;

    let initial_container: Container = {
        "files": [
            "prelim_map.py",
            "remap.py",
            "helper.py"
        ],
        "pipeline": {
            "kive_version": "0.14",
            "default_config": {
                "parent_family": "sample",
                "parent_tag": "basic",
                "parent_md5": "8dab0b3c7b7d812f0ba4819664be8acb",
                "memory": 100,
                "threads": 1
            },
            "inputs": [{
                "dataset_name": "input2",
                "x": 0.15,
                "y": 0.15,
            }, {
                "dataset_name": "input1",
                "x": 0.05,
                "y": 0.3,
            }],
            "outputs": [{
                "dataset_name": "unmapped2_fastq",
                "source_step": 2,
                "source_dataset_name": "unmapped2_fastq",
                "x": 0.637772562280456,
                "y": 0.633208895290869,
            }, {
                "dataset_name": "unmapped1_fastq",
                "source_step": 2,
                "source_dataset_name": "unmapped1_fastq",
                "x": 0.637772562280456,
                "y": 0.633208895290869,
            }, {
                "dataset_name": "remap_conseq",
                "source_step": 2,
                "source_dataset_name": "remap_conseq",
                "x": 0.637772562280456,
                "y": 0.633208895290869,
            }, {
                "dataset_name": "remap",
                "source_step": 2,
                "source_dataset_name": "remap",
                "x": 0.637772562280456,
                "y": 0.633208895290869,
            }, {
                "dataset_name": "remap_counts",
                "source_step": 2,
                "source_dataset_name": "remap_counts",
                "x": 0.637772562280456,
                "y": 0.633208895290869,
            }],
            "steps": [{
                "x": 0.344662650584514,
                "y": 0.5,
                "driver": "prelim_map.py",

                "inputs": [{
                    "dataset_name": "fastq1",
                    "source_step": 0,
                    "source_dataset_name": "input1",
                }, {
                    "dataset_name": "fastq2",
                    "source_step": 0,
                    "source_dataset_name": "input2",
                }],
                "outputs": ["prelim"],
            }, {
                "x": 0.450583501602465,
                "y": 0.257130788000083,
                "driver": "remap.py",
                "inputs": [{
                    "dataset_name": "fastq1",
                    "source_step": 0,
                    "source_dataset_name": "input1",
                }, {
                    "dataset_name": "fastq2",
                    "source_step": 0,
                    "source_dataset_name": "input2",
                }, {
                    "dataset_name": "prelim",
                    "source_step": 1,
                    "source_dataset_name": "prelim",
                }],
                "outputs": [
                    "remap",
                    "remap_counts",
                    "remap_conseq",
                    "unmapped1_fastq",
                    "unmapped2_fastq"
                ]
            }]
        }
    };

    beforeAll(function() {
        jasmine.addMatchers(imagediff.jasmine);
    });

    beforeEach(function(){
        appendLoadFixtures('content_method_dialog.tpl.html');
        appendLoadStyleFixtures('drydock.css');
        appendSetFixtures("<a id='activator'>Activator</a>");
        dlg = new MethodDialog(
            $('.ctrl_menu').attr('id', '#id_method_ctrl'),
            $('#activator'),
            initial_container);

        $cp_hidden_input = $('#id_select_colour');
        $cp_pick = $('#colour_picker_pick');
        $cp_menu = $('#colour_picker_menu');
        $submit_button = $('#id_method_button');
        $select_method = $("#id_select_method");
        $input_names = $('#id_input_names');
        $output_names = $('#id_output_names');
        $error = $('#id_method_error');
        $expand_outputs_ctrl = $('.ctrl_menu .expand_outputs_ctrl');
        $input_names.val('in1');
        $output_names.val('out1 out2 out3');
        canvas = <HTMLCanvasElement> $('canvas')[0];
        expected_canvas = imagediff.createCanvas(canvas.width, 78);
        expected_ctx = expected_canvas.getContext('2d');
        expected_method = new MethodNode(
            103.349365,  // x
            31,  // y (for 3 outputs)
            "#999",  // fill
            null,  // label
            [
                { dataset_name: 'in1', source_step: 0, source_dataset_name: 'in1'}
            ],  // inputs
            ['out1', 'out2', 'out3']);  // outputs
    });

    it('should initialize properly', function() {
        expect(dlg.jqueryRef).toBeInDOM();
        expect(dlg.activator).toBeInDOM();
        try {
            dlg.validateInitialization();
        } catch (e) {
            fail(e);
        }
    });

    it('should appear after the activator is clicked', function() {
        expect(dlg.jqueryRef).toBeHidden();
        dlg.activator.click();
        expect(dlg.jqueryRef).toBeVisible();
    });

    it('should reset itself', function() {
        $input_names.val('foo bar');
        $output_names.val('baz');
        $error.text('error');
        $expand_outputs_ctrl.text('▾ Hide list');
        $submit_button.val('Revise Method');
        dlg.reset();

        expect($input_names.val()).toBe('');
        expect($output_names.val()).toBe('');
        expect($submit_button.val().match(/Revise/i)).toBeFalsy();
    });

    it('should refresh preview when input names change', function() {
        expected_method = new MethodNode(
            106.8134666,  // x
            35,  // y (for 2 outputs)
            "#999",  // fill
            null,  // label
            [
                { dataset_name: 'in1', source_step: 0, source_dataset_name: 'in1'},
                { dataset_name: 'in2', source_step: 0, source_dataset_name: 'in2'}
            ],  // inputs
            ['out1', 'out2', 'out3']);  // outputs
        expected_canvas.height = 82; // based on number of inputs and outputs.
        expected_method.draw(expected_ctx);

        $input_names.val('in1 in2');
        dlg.activator.click();

        (expect(canvas) as any).toImageDiffEqual(expected_canvas);
    });

    it('should open the colour picker', function() {
        dlg.activator.click();
        $cp_pick.click();
        expect($cp_menu).toBeVisible();
    });

    it('should pick a colour and close the menu', function() {
        dlg.activator.click();

        $cp_pick.click();
        let $color1 = $cp_menu.find('.colour_picker_colour').eq(3);
        let bgcol = $color1.css('background-color');
        $color1.click();
        expect($cp_pick.css('background-color')).toBe(bgcol);
        expect($cp_hidden_input.val()).toBe(bgcol);
        expect($cp_menu).toBeHidden();

        expected_method.fill = bgcol;
        expected_method.draw(expected_ctx);
        (expect(canvas) as any).toImageDiffEqual(expected_canvas);
    });

    it('should move to a specified coordinate', function() {
        dlg.activator.click();
        dlg.align(100, 100);

        // gets position relative to the document
        let dlg_pos = dlg.jqueryRef.offset();

        expect(dlg_pos.left).toBe(100 - canvas.width / 2 );
        expect(dlg_pos.top).toBe(100);
    });

    it('should load a MethodNode object', function() {
        dlg.show();
        let mock_method_node = new MethodNode(
            50 /* x */, 50 /* y */,
            "#0d8",
            "prelim_map.py",
            [
                { dataset_name: 'remap', source_step: 0, source_dataset_name: 'remap'}
            ],
            ["aligned", "conseq_ins", "failed_read"]
        );

        expected_method.fill = "#0d8";
        expected_method.draw(expected_ctx);

        dlg.load(mock_method_node);

        expect($select_method.val()).toBe('prelim_map.py');
        expect($cp_hidden_input.val()).toBe('#0d8');

        (expect(canvas) as any).toImageDiffEqual(expected_canvas);
    });

    it('should not submit when required fields are missing', function() {
        dlg.activator.click();

        let canvas = imagediff.createCanvas(300, 150);
        let canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);
        spyOn(canvasState, 'addShape');

        $error.text('');
        $select_method.val('');
        dlg.submit(canvasState);
        expect($select_method).toBeFocused();
        expect($error).not.toBeEmpty();

        expect(canvasState.addShape).not.toHaveBeenCalled();
    });

    it('should not submit when there\'s already a node by that name', function() {
        dlg.activator.click();

        let canvas = imagediff.createCanvas(300, 150);
        let canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);
        canvasState.addShape(new MethodNode(
            23,
            50,
            'red',
            'my_script.py',
            [{dataset_name: "in1", source_step: 0, source_dataset_name: "in1"}],
            ["out1"]));

        spyOn(canvasState, 'addShape');

        $select_method.val('my_script.py');
        dlg.submit(canvasState);
        expect($error).not.toBeEmpty();
        expect(canvasState.addShape).not.toHaveBeenCalled();
    });

    it('should apply its state to a CanvasState', function(done) {
        dlg.activator.click();

        let canvas = imagediff.createCanvas(300, 150);
        let canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);

        canvas.addEventListener('CanvasStateChange', function(event: CustomEvent) {
            expect(event.detail).toHaveAttr('added');
            expect(Array.isArray(event.detail.added)).toBeTruthy();

            let method = event.detail.added[0];
            expect(CanvasState.isMethodNode(method)).toBeTruthy();
            done();
        });

        spyOn(canvasState, 'addShape').and.callThrough();
        spyOn(dlg, 'reset').and.callThrough();

        $select_method.val('remap.py');
        dlg.submit(canvasState);
        expect(canvasState.addShape).toHaveBeenCalled();
        expect(dlg.reset).toHaveBeenCalled();

        let methods = canvasState.getMethodNodes();
        expect(methods.length).toEqual(1);
        expect(methods[0].label).toBe('custom_name');
        expect(methods[0].n_outputs).toEqual(3);
        expect(methods[0].n_inputs).toEqual(1);
    });
});

describe("InputDialog fixture", function() {
    let dlg;
    let $datatype_name;
    let $select_cdt;
    let $error;
    let canvas;
    let expected_canvas;
    let expected_ctx;
    let expected_input;
    let expected_raw_input;

    beforeAll(function() {
        jasmine.addMatchers(imagediff.jasmine);
    });

    beforeEach(function(){
        appendLoadFixtures('content_input_dialog.tpl.html');
        appendLoadStyleFixtures('drydock.css');
        appendSetFixtures("<a id='activator'>Activator</a>");
        dlg = new InputDialog(
            $('.ctrl_menu').attr('id', '#id_view_ctrl'),
            $('#activator')
        );

        $datatype_name = $('#id_datatype_name');
        $error = $('#id_dt_error');
        $select_cdt = $('#id_select_cdt');
        canvas = <HTMLCanvasElement> $('canvas')[0];
        expected_canvas = imagediff.createCanvas(canvas.width, canvas.height);
        expected_ctx = expected_canvas.getContext('2d');
        expected_input = new CdtNode(99, 125, 30, '');
        expected_raw_input = new RawNode(125, 30, '');
    });

    it('should initialize properly', function() {
        expect(dlg.jqueryRef).toBeInDOM();
        expect(dlg.activator).toBeInDOM();
        try {
            dlg.validateInitialization();
        } catch (e) {
            fail(e);
        }
    });

    it('should move to a specified coordinate', function() {
        dlg.activator.click();

        // anchor point is top center of the inner div (inside of css padding)
        dlg.align(100, 100);

        // gets position relative to the document
        let dlg_pos = dlg.jqueryRef.offset();

        expect(dlg_pos.left).toBeCloseTo(-33, 1);
        expect(dlg_pos.top).toBeCloseTo(92, 1);
    });

    it('should show a CDTNode preview when a CDT is selected', function() {
        expected_input.draw(expected_ctx);

        dlg.activator.click();
        $select_cdt.append($('<option>').val(23));
        $select_cdt.val(23).change();

        (expect(canvas) as any).toImageDiffEqual(expected_canvas);
    });

    it('should show a RawNode preview when no CDT is selected', function() {
        expected_raw_input.draw(expected_ctx);

        dlg.activator.click();
        $select_cdt.append($('<option>').val('foo'));
        $select_cdt.val('foo').change();

        (expect(canvas) as any).toImageDiffEqual(expected_canvas);
    });

    it('should not submit when required fields are empty', function() {
        dlg.activator.click();
        let canvas = imagediff.createCanvas(300, 150);
        let canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);
        $datatype_name.val('');
        spyOn(canvasState, 'addShape');
        dlg.submit(canvasState);
        expect($error).not.toBeEmpty();
        expect(canvasState.addShape).not.toHaveBeenCalled();
    });

    it('should not submit when there\'s already a node by that name', function() {
        dlg.activator.click();
        $select_cdt.append($('<option>').val(23));
        $select_cdt.val(23);
        let canvas = imagediff.createCanvas(300, 150);
        let canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);
        canvasState.addShape(new CdtNode(23, 50, 50, 'foo'));
        $datatype_name.val('foo');

        spyOn(canvasState, 'addShape');
        dlg.submit(canvasState);
        expect($error).not.toBeEmpty();
        expect(canvasState.addShape).not.toHaveBeenCalled();
    });

    it('should apply its state to a CanvasState', function() {
        dlg.activator.click();
        let canvas = imagediff.createCanvas(300, 150);
        let canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);

        $select_cdt.append($('<option>').val(23));
        $select_cdt.val(23);
        $datatype_name.val('foo');

        spyOn(canvasState, 'addShape').and.callThrough();
        dlg.submit(canvasState);
        expect(canvasState.addShape).toHaveBeenCalled();
        let inputs = canvasState.getInputNodes();
        expect(inputs.length).toEqual(1);
        expect(inputs[0].label).toBe('foo');
    });

});

describe("Container OutputDialog fixture", function() {
    let dlg;
    let $output_name;
    let $error;
    let canvas;
    let expected_canvas;
    let expected_ctx;

    beforeAll(function() {
        jasmine.addMatchers(imagediff.jasmine);
    });

    beforeEach(function(){
        for (let i = 0; i < 2; i++) {
            appendLoadFixtures('content_output_dialog.tpl.html');
            appendLoadStyleFixtures('drydock.css');
            appendSetFixtures("<a id='activator'>Activator</a>");
            dlg = new OutputDialog(
                $('.ctrl_menu').attr('id', '#id_output_ctrl'),
                $('#activator')
            );
            $output_name = $('#id_output_name');
            $error = $('#id_output_error');
            canvas = <HTMLCanvasElement> $('canvas')[0];
            expected_canvas = imagediff.createCanvas(canvas.width, canvas.height);
            expected_ctx = expected_canvas.getContext('2d');
        }
    });

    it('should initialize properly', function() {
        expect(dlg.jqueryRef).toBeInDOM();
        expect(dlg.activator).toBeInDOM();
        try {
            dlg.validateInitialization();
        } catch (e) {
            fail(e);
        }
    });

    it('should move to a specified coordinate', function() {
        dlg.activator.click();

        // anchor point is top center of the inner div (inside of css padding)
        dlg.align(100, 100);

        // gets position relative to the document
        let dlg_pos = dlg.jqueryRef.offset();

        expect(dlg_pos.left).toBeCloseTo(-33, 1);
        expect(dlg_pos.top).toBeCloseTo(92, 1);
    });

    it('should show an OutputNode preview', function() {
        let expectedNode = new OutputNode(125, 30, '');
        expectedNode.draw(expected_ctx);

        dlg.activator.click();

        (expect(canvas) as any).toImageDiffEqual(expected_canvas);
    });

    it('should not submit when required fields are empty', function() {
        dlg.activator.click();
        let canvas = imagediff.createCanvas(300, 150);
        let canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);
        $output_name.val('');
        spyOn(canvasState, 'addShape');
        dlg.submit(canvasState);
        expect($error).not.toBeEmpty();
        expect(canvasState.addShape).not.toHaveBeenCalled();
    });

    it('should not submit when there\'s already a node by that name', function() {
        dlg.activator.click();
        let canvas = imagediff.createCanvas(300, 150);
        let canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);
        canvasState.addShape(new OutputNode(50, 50, 'foo'));
        $output_name.val('foo');

        spyOn(canvasState, 'addShape');
        dlg.submit(canvasState);
        expect($error).not.toBeEmpty();
        expect(canvasState.addShape).not.toHaveBeenCalled();
    });

    it('should load an OutputNode', function() {
        let output_node = new OutputNode(50, 50, 'foo');
        dlg.activator.click();
        dlg.load(output_node);

        expect($output_name).toHaveValue('foo');
    });

    it('should apply its state to a CanvasState', function() {
        dlg.activator.click();
        let canvas = imagediff.createCanvas(300, 150);
        let canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);
        $output_name.val('foo');

        spyOn(canvasState, 'addShape').and.callThrough();
        dlg.submit(canvasState);
        expect(canvasState.addShape).toHaveBeenCalled();
        let outputs = canvasState.getOutputNodes();
        expect(outputs.length).toEqual(1);
        expect(outputs[0].label).toBe('foo');
    });

    it('should change the name of a loaded OutputNode', function() {
        let output_node = new OutputNode(50, 50, 'foo');
        let canvas = imagediff.createCanvas(300, 150);
        let canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);
        canvasState.addShape(output_node);
        dlg.activator.click();
        dlg.load(output_node);

        $output_name.val('foo2');
        dlg.submit(canvasState);

        let outputs = canvasState.getOutputNodes();
        expect(outputs.length).toEqual(1);
        expect(outputs[0].label).toBe('foo2');
    });

});