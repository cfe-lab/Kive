"use strict";

import { MethodDialog } from "../static/pipeline/pipeline_dialogs";
import "jasmine";
import 'jasmine-html';
import 'jquery';
import 'jasmine-jquery';
import 'imagediff';

describe("MethodDialog class", function() {

    var dlg;
    jasmine.getFixtures().fixturesPath = '/templates/pipeline';
    jasmine.getFixtures().preload('./pipeline_method_dialog.tpl.html');
    jasmine.getStyleFixtures().fixturesPath = '/static/pipeline';
    jasmine.getStyleFixtures().preload('./drydock.css');

    /* Mock API server call responses by overloading jQuery's getJSON method */
    // @todo: Use Jasmine's AJAX library: http://jasmine.github.io/2.0/ajax.html
    $.getJSON = <any> function(url: string, ...restArgs: any[]) {
        var result = null;
        if (url.match(/\/api\/methods\/\d+/)) {
            result = {
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
            };
        } else if (url.match(/\/api\/methodfamilies\/\d+\/methods/)) {
            result = [
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
            ];
        }

        if (result) {
            return $.Deferred()
                .resolve(result)
                .done(function() {
                    console.log('Mock API request filled: ' + url);
                });
        } else {
            return $.Deferred().reject("404 error");
        }
    };

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

        let method_family_menu = $('#id_select_method_family');

        method_family_menu.find('option')
            .filter(function() { return $(this).val() !== ""; })
            .remove();

        $('<option>').val('6').appendTo(method_family_menu);
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
        var firstInput = $('#id_method_name');
        firstInput.val('foo');
        dlg.reset();
        expect(firstInput.val()).toBe('');
    });

    it('should update when the family menu changes', function(done) {
        let canvas = <HTMLCanvasElement> $('canvas')[0];

        dlg.activator.click();
        $('#id_select_method_family').val('6').change();
        expect($('#id_select_method').find('option').length).toBeGreaterThan(0);

        var sam2aln = new Image();
        sam2aln.src = "/pipeline/test_assets/sam2aln_node.png";
        sam2aln.onload = function() {
            expect(canvas).toImageDiffEqual(sam2aln);
            done();
        };
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
        $select_method_family.change => $select_method should change
        $delete_outputs.change => $delete_outputs_details should change
        $delete_outputs_details.change => $delete_outputs should change
        $expand_outputs_ctrl.click => $delete_outputs_details should toggle viz
        colour_picker.$pick.click => picker is visible
        colour_picker.$menu.div.click =>
                $pick changes colour,
                $hidden_input changes val,
                picker is hidden
     */

});