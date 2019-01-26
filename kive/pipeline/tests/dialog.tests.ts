import { MethodDialog, Dialog, InputDialog, OutputDialog } from "@pipeline/pipeline_dialogs";
import {MethodNode, CdtNode, OutputNode} from "@canvas/drydock_objects";
import { REDRAW_INTERVAL, CanvasState } from "@canvas/drydock";
import * as imagediff from 'imagediff';
import {RawNode} from "@canvas/drydock_objects";

jasmine.getFixtures().fixturesPath = '/templates/pipeline';
jasmine.getStyleFixtures().fixturesPath = '/static/pipeline';
jasmine.getFixtures().preload(
    'pipeline_view_dialog.tpl.html',
    'pipeline_method_dialog.tpl.html',
    'pipeline_input_dialog.tpl.html',
    'pipeline_output_dialog.tpl.html'
);
jasmine.getStyleFixtures().preload('drydock.css');

describe("Dialog fixture", function() {
    let dlg;

    beforeEach(function(){
        appendLoadFixtures('pipeline_view_dialog.tpl.html');
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

describe("MethodDialog fixture", function() {

    let dlg;
    let $cp_hidden_input;
    let $cp_pick;
    let $cp_menu;
    let $delete_outputs;
    let $delete_outputs_details;
    let $submit_button;
    let $select_method;
    let $select_method_family;
    let $input_name;
    let $error;
    let $expand_outputs_ctrl;
    let canvas;
    let expected_canvas;
    let expected_ctx;
    let expected_method;

    let mockData1 = {
        "status": 200,
        "responseText": `[
            {
                "revision_name": "sam2aln",
                "display_name": "1: sam2aln",
                "revision_number": 1,
                "revision_desc": "Conversion of SAM data into aligned format.",
                "revision_DateTime": "2014-08-11T21:34:09.900000Z",
                "revision_parent": null,
                "user": "kive",
                "users_allowed": [],
                "groups_allowed": [
                "Everyone"
            ],
                "id": 6,
                "url": "http://localhost:8000/api/methods/6/",
                "absolute_url": "/method_revise/6/",
                "view_url": "/method_view/6/",
                "removal_plan": "http://localhost:8000/api/methods/6/removal_plan/",
                "family_id": 5,
                "family": "sam2aln",
                "driver": 8,
                "reusable": 1,
                "threads": 1,
                "dependencies": [
                {
                    "requirement": 3,
                    "path": "./",
                    "filename": ""
                }
            ],
                "inputs": [
                {
                    "dataset_name": "remap",
                    "dataset_idx": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 8,
                        "min_row": null,
                        "max_row": null
                    }
                }
            ],
                "outputs": [
                {
                    "dataset_name": "aligned",
                    "dataset_idx": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 11,
                        "min_row": null,
                        "max_row": null
                    }
                },
                {
                    "dataset_name": "conseq_ins",
                    "dataset_idx": 2,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 12,
                        "min_row": null,
                        "max_row": null
                    }
                },
                {
                    "dataset_name": "failed_read",
                    "dataset_idx": 3,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 13,
                        "min_row": null,
                        "max_row": null
                    }
                }
            ]
            }
        ]`
    };
    let mockData2 = {
        "status": 200,
        "responseText": `{
            "revision_name": "sam2aln",
            "display_name": "1: sam2aln",
            "revision_number": 1,
            "revision_desc": "Conversion of SAM data into aligned format.",
            "revision_DateTime": "2014-08-11T21:34:09.900000Z",
            "revision_parent": null,
            "user": "kive",
            "users_allowed": [],
            "groups_allowed": [
                "Everyone"
            ],
            "id": 6,
            "url": "http://localhost:8000/api/methods/6/",
            "absolute_url": "/method_revise/6/",
            "view_url": "/method_view/6/",
            "removal_plan": "http://localhost:8000/api/methods/6/removal_plan/",
            "family_id": 5,
            "family": "sam2aln",
            "driver": 8,
            "reusable": 1,
            "threads": 1,
            "dependencies": [
                {
                    "requirement": 3,
                    "path": "./",
                    "filename": ""
                }
            ],
            "inputs": [
                {
                    "dataset_name": "remap",
                    "dataset_idx": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 8,
                        "min_row": null,
                        "max_row": null
                    }
                }
            ],
            "outputs": [
                {
                    "dataset_name": "aligned",
                    "dataset_idx": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 11,
                        "min_row": null,
                        "max_row": null
                    }
                },
                {
                    "dataset_name": "conseq_ins",
                    "dataset_idx": 2,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 12,
                        "min_row": null,
                        "max_row": null
                    }
                },
                {
                    "dataset_name": "failed_read",
                    "dataset_idx": 3,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 13,
                        "min_row": null,
                        "max_row": null
                    }
                }
            ]
        }`
    };
    let mockData3 = {
        "status": 200,
        "responseText": `{
            "revision_name": "sam2aln",
            "display_name": "1: sam2aln",
            "revision_number": 1,
            "revision_desc": "Conversion of SAM data into aligned format.",
            "revision_DateTime": "2014-08-11T21:34:09.900000Z",
            "revision_parent": null,
            "user": "kive",
            "users_allowed": [],
            "groups_allowed": [
                "Everyone"
            ],
            "id": 6,
            "url": "http://localhost:8000/api/methods/6/",
            "absolute_url": "/method_revise/6/",
            "view_url": "/method_view/6/",
            "removal_plan": "http://localhost:8000/api/methods/6/removal_plan/",
            "family_id": 5,
            "family": "sam2aln",
            "driver": 8,
            "reusable": 1,
            "threads": 1,
            "dependencies": [
                {
                    "requirement": 3,
                    "path": "./",
                    "filename": ""
                }
            ],
            "inputs": [
                {
                    "dataset_name": "remap",
                    "dataset_idx": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 8,
                        "min_row": null,
                        "max_row": null
                    }
                },
                {
                    "dataset_name": "remap_duplicate",
                    "dataset_idx": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 8,
                        "min_row": null,
                        "max_row": null
                    }
                }
            ],
            "outputs": [
                {
                    "dataset_name": "aligned",
                    "dataset_idx": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 11,
                        "min_row": null,
                        "max_row": null
                    }
                },
                {
                    "dataset_name": "failed_read",
                    "dataset_idx": 3,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 13,
                        "min_row": null,
                        "max_row": null
                    }
                }
            ]
        }`
    };


    beforeAll(function() {
        jasmine.addMatchers(imagediff.jasmine);
    });

    beforeEach(function(){
        appendLoadFixtures('pipeline_method_dialog.tpl.html');
        appendLoadStyleFixtures('drydock.css');
        appendSetFixtures("<a id='activator'>Activator</a>");
        dlg = new MethodDialog(
            $('.ctrl_menu').attr('id', '#id_method_ctrl'),
            $('#activator')
        );

        $cp_hidden_input = $('#id_select_colour');
        $cp_pick = $('#colour_picker_pick');
        $cp_menu = $('#colour_picker_menu');
        $delete_outputs = $('#id_method_delete_outputs');
        $delete_outputs_details = $('#id_method_delete_outputs_details');
        $submit_button = $('#id_method_button');
        $select_method = $("#id_select_method");
        $select_method_family = $('#id_select_method_family');
        $input_name = $('#id_method_name');
        $error = $('#id_method_error');
        $expand_outputs_ctrl = $('.ctrl_menu .expand_outputs_ctrl');
        canvas = <HTMLCanvasElement> $('canvas')[0];
        expected_canvas = imagediff.createCanvas(canvas.width, 78);
        expected_ctx = expected_canvas.getContext('2d');
        expected_method = new MethodNode(
            99,  // pk
            null,  // family
            103.349365,  // x
            31,  // y (for 3 outputs)
            "#999",  // fill
            null,  // label
            [
              {"structure": {"compounddatatype": null}}
            ],  // inputs
            [
              {"structure": {"compounddatatype": null}},
              {"structure": {"compounddatatype": null}},
              {"structure": {"compounddatatype": null}}
            ]);  // outputs

        $select_method_family.find('option')
            .filter(function() { return $(this).val() !== ""; })
            .remove();

        $('<option>').val('5').appendTo($select_method_family);
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
        $input_name.val('foo');
        $error.text('error');
        $delete_outputs_details.show();
        $expand_outputs_ctrl.text('▾ Hide list');
        $submit_button.val('Revise Method');
        dlg.reset();

        expect($input_name.val()).toBe('');
        expect($delete_outputs_details).toBeHidden();
        expect($submit_button.val().match(/Revise/i)).toBeFalsy();
    });

    function loadMockMethod(callback: () => void) {
        jasmine.Ajax.withMock(function() {
            $select_method_family.val('5').change();

            // populate method revisions menu
            expect($select_method.find('option').length).toEqual(0);
            jasmine.Ajax.requests.mostRecent().respondWith(mockData1);
            expect($select_method.find('option').length).toEqual(1);

            // thumbnail refresh
            jasmine.Ajax.requests.mostRecent().respondWith(mockData2);

            callback();
        });
    }

    it('should update when the family menu changes', function(done) {
        expected_method.draw(expected_ctx);
        dlg.activator.click();

        loadMockMethod(function () {

            // No extra calls, just checking default drawing.

            (expect(canvas) as any).toImageDiffEqual(expected_canvas);
        });
        done();
    });

    it('should toggle visibility of the outputs list', function() {
        dlg.activator.click();
        $delete_outputs_details.show();
        $expand_outputs_ctrl.click();
        expect($delete_outputs_details).toBeHidden();
        $expand_outputs_ctrl.click();
        expect($delete_outputs_details).toBeVisible();
    });

    it('should sync child checkboxes of the outputs list with the parent check', function() {
        dlg.activator.click();
        loadMockMethod(function () {
            $delete_outputs.prop('checked', false).change();
            $delete_outputs_details.find('input').each(function() {
                expect($(this).prop('checked')).toBeFalsy();
            });

            $delete_outputs.prop('checked', true).change();
            $delete_outputs_details.find('input').each(function() {
                expect($(this).prop('checked')).toBeTruthy();
            });
        });
    });

    it('should sync the parent checkbox of the outputs list with the child checkboxes', function() {
        dlg.activator.click();

        loadMockMethod(function () {
            let outputs = $delete_outputs_details.find('input');
            outputs = [
                outputs.eq(0).prop('checked', false),
                outputs.eq(1).prop('checked', true),
                outputs.eq(2).prop('checked', false)
            ];

            outputs[1].change();
            expect($delete_outputs.prop('indeterminate')).toBeTruthy();

            outputs[1].prop('checked', false).change();
            expect($delete_outputs.prop('indeterminate')).toBeFalsy();
            expect($delete_outputs.prop('checked')).toBeFalsy();

            outputs[0].prop('checked', true).change();
            outputs[1].prop('checked', true).change();
            expect($delete_outputs.prop('indeterminate')).toBeTruthy();

            outputs[2].prop('checked', true).change();
            expect($delete_outputs.prop('indeterminate')).toBeFalsy();
            expect($delete_outputs.prop('checked')).toBeTruthy();
        });
    });

    it('should update the preview canvas when child outputs-to-delete change', function(done) {
        expected_method.out_magnets.forEach(function(magnet) {
            magnet.toDelete = true;
        });
        expected_method.draw(expected_ctx);

        dlg.activator.click();
        loadMockMethod(function () {
            let outputs = $delete_outputs_details.find('input');
            outputs = [
                outputs.eq(0).prop('checked', false),
                outputs.eq(1).prop('checked', false),
                outputs.eq(2).prop('checked', false)
            ];
            outputs[1].change();

            (expect(canvas) as any).toImageDiffEqual(expected_canvas);

        });
        done();
    });

    it('should update the preview canvas when parent outputs-to-delete change', function(done) {
        expected_method.out_magnets.forEach(function(magnet) {
            magnet.toDelete = true;
        });
        expected_method.draw(expected_ctx);

        dlg.activator.click();
        loadMockMethod(function () {
            $delete_outputs.prop('checked', false).change();

            (expect(canvas) as any).toImageDiffEqual(expected_canvas);
        });
        done();
    });

    it('should refresh preview when method revision changes', function(done) {
        expected_method = new MethodNode(
            99,  // pk
            null,  // family
            110.2775682,  // x
            35,  // y (for 2 outputs)
            "#999",  // fill
            null,  // label
            [
              {"structure": {"compounddatatype": null}},
              {"structure": {"compounddatatype": null}}
            ],  // inputs
            [
              {"structure": {"compounddatatype": null}},
              {"structure": {"compounddatatype": null}}
            ]);  // outputs
        expected_method.draw(expected_ctx);

        dlg.activator.click();
        loadMockMethod(function() {
            $select_method.append($('<option>').val(8));
            $select_method.val(8).change();
            jasmine.Ajax.requests.mostRecent().respondWith(mockData3);

            (expect(canvas) as any).toImageDiffEqual(expected_canvas);
        });
        done();
    });

    it('should open the colour picker', function() {
        dlg.activator.click();
        $cp_pick.click();
        expect($cp_menu).toBeVisible();
    });

    it('should pick a colour and close the menu', function(done) {
        dlg.activator.click();

        loadMockMethod(function() {
            $cp_pick.click();
            let $color1 = $cp_menu.find('.colour_picker_colour').eq(3);
            let bgcol = $color1.css('background-color');
            $color1.click();
            jasmine.Ajax.requests.mostRecent().respondWith(mockData2);
            expect($cp_pick.css('background-color')).toBe(bgcol);
            expect($cp_hidden_input.val()).toBe(bgcol);
            expect($cp_menu).toBeHidden();

            expected_method.fill = bgcol;
            expected_method.draw(expected_ctx);
            (expect(canvas) as any).toImageDiffEqual(expected_canvas);
        });
        done();
    });

    it('should move to a specified coordinate', function() {
        dlg.activator.click();
        dlg.align(100, 100);

        // gets position relative to the document
        let dlg_pos = dlg.jqueryRef.offset();

        expect(dlg_pos.left).toBe(100 - canvas.width / 2 );
        expect(dlg_pos.top).toBe(100);
    });

    it('should load a MethodNode object', function(done) {
        dlg.show();
        let mock_method_node = new MethodNode(
            6, 5, 50 /* x */, 50 /* y */,
            "#0d8",
            "custom_name",
            [
                {
                    "dataset_name": "remap",
                    "dataset_idx": 1,
                    "structure": {"compounddatatype": 8}
                }
            ],
            [
                {
                    "dataset_name": "aligned",
                    "dataset_idx": 1,
                    "structure": {"compounddatatype": 11}
                }, {
                "dataset_name": "conseq_ins",
                "dataset_idx": 2,
                "structure": {"compounddatatype": 12}
            }, {
                "dataset_name": "failed_read",
                "dataset_idx": 3,
                "structure": {"compounddatatype": 13}
            }
            ]
        );
        mock_method_node.outputs_to_delete = [ "conseq_ins" ];
        mock_method_node.out_magnets[1].toDelete = true;

        expected_method.fill = "#0d8";
        expected_method.out_magnets[1].toDelete = true;
        expected_method.draw(expected_ctx);

        jasmine.Ajax.withMock(function() {
            dlg.load(mock_method_node);
            jasmine.Ajax.requests.mostRecent().respondWith(mockData1);
            jasmine.Ajax.requests.mostRecent().respondWith(mockData2);

            expect(+$select_method_family.val()).toEqual(5);
            expect(+$select_method.val()).toBe(6);
            expect($input_name.val()).toBe('custom_name');
            expect($cp_hidden_input.val()).toBe('#0d8');
            expect($delete_outputs.prop('indeterminate')).toBeTruthy();

            let delete_checkboxes = $delete_outputs_details.find('input');
            expect(delete_checkboxes.length).toBe(3);
            expect(delete_checkboxes.eq(0).prop('checked')).toBeTruthy();
            expect(delete_checkboxes.eq(1).prop('checked')).toBeFalsy();
            expect(delete_checkboxes.eq(2).prop('checked')).toBeTruthy();

            (expect(canvas) as any).toImageDiffEqual(expected_canvas);
        });
        done();
    });

    it('should not submit when required fields are missing', function() {
        dlg.activator.click();

        let canvas = imagediff.createCanvas(300, 150);
        let canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);
        spyOn(canvasState, 'addShape');

        loadMockMethod(function() {
            $error.text('');
            $select_method.val('');
            $input_name.val('custom_name');
            dlg.submit(canvasState);
            expect($select_method).toBeFocused();
            expect($error).not.toBeEmpty();

            $error.text('');
            $select_method.val(6);
            $input_name.val('');
            dlg.submit(canvasState);
            expect($input_name).toBeFocused();
            expect($error).not.toBeEmpty();

            $error.text('');
            $select_method_family.val('');
            $select_method.val(6);
            $input_name.val('custom_name');
            dlg.submit(canvasState);
            expect($select_method_family).toBeFocused();
            expect($error).not.toBeEmpty();

            expect(canvasState.addShape).not.toHaveBeenCalled();
        });
    });

    it('should not submit when there\'s already a node by that name', function() {
        dlg.activator.click();

        let canvas = imagediff.createCanvas(300, 150);
        let canvasState = new CanvasState(canvas, true, REDRAW_INTERVAL);
        canvasState.addShape(new CdtNode(23, 50, 50, 'custom_name'));

        spyOn(canvasState, 'addShape');

        loadMockMethod(function() {
            $select_method.val(6);
            $input_name.val('custom_name');
            dlg.submit(canvasState);
            expect($error).not.toBeEmpty();
            expect(canvasState.addShape).not.toHaveBeenCalled();
        });
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

        loadMockMethod(function() {
            $select_method.val(6);
            $input_name.val('custom_name');
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
        appendLoadFixtures('pipeline_input_dialog.tpl.html');
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

describe("OutputDialog fixture", function() {
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
        appendLoadFixtures('pipeline_output_dialog.tpl.html');
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