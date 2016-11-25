"use strict";

import { MethodDialog } from "../static/pipeline/pipeline_dialogs";
import "jasmine";
import 'jasmine-html';
import 'jquery';
import 'jasmine-jquery';
import 'jasmine-ajax';
import 'imagediff';

describe("MethodDialog class", function() {

    var dlg;
    var $cp_hidden_input;
    var $cp_pick;
    var $cp_menu;
    var $delete_outputs;
    var $delete_outputs_details;
    var $submit_button;
    var $select_method;
    var $select_method_family;
    var $input_name;
    var $error;
    var $expand_outputs_ctrl;

    jasmine.getFixtures().fixturesPath = '/templates/pipeline';
    jasmine.getFixtures().preload('./pipeline_method_dialog.tpl.html');
    jasmine.getStyleFixtures().fixturesPath = '/static/pipeline';
    jasmine.getStyleFixtures().preload('./drydock.css');

    beforeAll(function() {
        jasmine.addMatchers(imagediff.jasmine);
    });

    beforeEach(function(){
        appendLoadFixtures('./pipeline_method_dialog.tpl.html');
        appendLoadStyleFixtures('./drydock.css');
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

        $select_method_family.find('option')
            .filter(function() { return $(this).val() !== ""; })
            .remove();

        $('<option>').val('6').appendTo($select_method_family);
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
            $select_method_family.val('6').change();

            // populate method revisions menu
            expect($select_method.find('option').length).toEqual(0);
            jasmine.Ajax.requests.mostRecent().respondWith({
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
            });
            expect($select_method.find('option').length).toEqual(1);

            // thumbnail refresh
            jasmine.Ajax.requests.mostRecent().respondWith({
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
            });

            callback();
        });
    }

    it('should update when the family menu changes', function(done) {
        let canvas = <HTMLCanvasElement> $('canvas')[0];
        dlg.activator.click();

        loadMockMethod(function () {
            var sam2aln = new Image();
            sam2aln.src = "/pipeline/test_assets/sam2aln_node.png";
            sam2aln.onload = function() {
                expect(canvas).toImageDiffEqual(sam2aln);
                done();
            };
        });
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

            // refreshPreviewCanvasMagnets()
        });
    });

    it('should sync the parent checkbox of the outputs list with the child checkboxes', function() {
        dlg.activator.click();

        loadMockMethod(function () {
            var outputs = $delete_outputs_details.find('input');
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

            // refreshPreviewCanvasMagnets()
        });
    });

    it('should open the colour picker', function() {
        dlg.activator.click();
        $cp_pick.click();
        expect($cp_menu).toBeVisible();
    });

    it('should pick a colour and close the menu', function() {
        dlg.activator.click();
        $cp_pick.click();
        var $color1 = $cp_menu.find('.colour_picker_colour').eq(0);
        var bgcol = $color1.css('background-color');
        $color1.click();
        expect($cp_pick.css('background-color')).toBe(bgcol);
        expect($cp_hidden_input.val()).toBe(bgcol);
        expect($cp_menu).toBeHidden();

        // canvas refresh
    });



    /*
        TODO:

        Methods:
        cancel() => reset() and hide() in one
        align(x, y) => dialog should move (Align the dialog to a given coord. Anchor point is top center.)
        load(MethodNode) =>
        submit(CanvasState) => node should go to a new CanvasState and be in exact same position from user's perspective

        Events:
        $select_method.change => canvas should draw

     */

});