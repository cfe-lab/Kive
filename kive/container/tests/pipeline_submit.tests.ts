import { CanvasState } from "@container/canvas/drydock";
import { buildPipelineSubmit } from "@container/io/pipeline_submit";
import { RawNode, MethodNode, OutputNode, Connector } from "@container/canvas/drydock_objects";
import * as imagediff from 'imagediff';

(window as any).$ = (window as any).jQuery = $;
require("@portal/noxss.js");

interface BuildSubmitArgs extends Array<any> {
    0: CanvasState;
    1: string;
    2: JQuery;
    3: JQuery;
    4: number;
    5: JQuery;
    6: JQuery;
    7: number;
    8: JQuery;
    9: JQuery;
    10: JQuery;
    11: JQuery;
    12: () => any;
}

describe("Pipeline Submit class", function() {

    // static vars
    let canvasState;
    let args: BuildSubmitArgs,
        $error: JQuery,
        arg_names = [
            "canvasState", "action",
            "$family_name", "$family_desc",
            "family_pk",
            "$revision_name", "$revision_desc",
            "parent_revision_id",
            "$published",
            "$user_permissions", "$group_permissions",
            "$error",
            "familyNameError"
        ];

    let built_submit;

    function jqInput(value: string): JQuery {
        return $('<input>').val(value);
    }
    function jqOpt(value: string): JQuery {
        return $('<option>').val(value).text(value);
    }
    function jqPermissionsWidget(): JQuery {
        return $('<select multiple>')
            .append([ jqOpt("a"), jqOpt("b"), jqOpt("c") ])
            .val([ "b", "c" ]);
    }
    function mockPipeline() {
        let input = new RawNode(50, 50, 'raw_node');
        let method = new MethodNode(
            60, 60,
            '#999', 'method_node',
            [
                { dataset_name: 'input1', source_step: 0, source_dataset_name: 'input1'}],
            ['output1']
        );
        let output = new OutputNode(50, 50, 'output_node');


        let conn1 = new Connector(input.out_magnets[0]);
        let conn2 = new Connector(method.out_magnets[0]);

        conn1.x = method.in_magnets[0].x;
        conn1.y = method.in_magnets[0].y;
        conn2.x = output.in_magnets[0].x;
        conn2.y = output.in_magnets[0].y;

        input.out_magnets[0].connected.push(conn1);
        method.in_magnets[0].tryAcceptConnector(conn1);
        method.out_magnets[0].connected.push(conn2);
        output.in_magnets[0].tryAcceptConnector(conn2);

        canvasState.reset();
        canvasState.addShape(input);
        canvasState.addShape(method);
        canvasState.addShape(output);
        canvasState.connectors.push(conn1, conn2);
    }

    beforeAll(function() {
        jasmine.addMatchers(imagediff.jasmine);
        canvasState = new CanvasState(imagediff.createCanvas(300, 150), true);
    });

    beforeEach(function(){
        $error = $('<div>').appendTo('body').hide();

        args = [
            canvasState,
            'new', // action: new|add|revise
            jqInput('custom_family_name'),
            jqInput('custom_family_desc'),
            0, // family_pk
            jqInput('custom_revision_name'),
            jqInput('custom_revision_desc'),
            -1, // parent_revision_id
            $('<input>').attr('type', 'checkbox').prop('checked', false), // published
            jqPermissionsWidget(), // users
            jqPermissionsWidget(), // groups
            $error, // error outlet
            function() {} // special callback for family name errors
        ];

    });

    afterEach(function() {
        $error.remove();
    });

    it('should attach a CSRF token to each AJAX request', function() {
        expect($.ajaxSettings.beforeSend).not.toBeUndefined();
        expect($.ajaxSettings.beforeSend.toString()).toMatch(
            /\.setRequestHeader\s*\(\s*["']X-CSRFToken["']\s*,\s*[A-Za-z_\$]+\s*\)/
        );
    });

    describe('should throw an error when', function() {

        let $dummy = $('#nonexistent');
        let bad_args = [
            null, '', $dummy, $dummy,
            null, $dummy, $dummy, null,
            $dummy, $dummy, $dummy, $dummy,
            "invalid argument (not callable)"
        ];

        afterEach(function() {
            expect(function() {
                buildPipelineSubmit.apply(null, args);
            }).toThrow();
        });

        for (let i = 0; i < arg_names.length; i++) {
            if ([ 1, 4, 7 ].indexOf(i) > -1) continue;
            it(arg_names[i] + ' is not found', function () {
                args[i] = bad_args[i];
            });
        }

        it('trying to revise and parent_revision_id is missing', function () {
            args[1] = 'revise';
            args[7] = undefined;
        });

        it('trying to revise and family_pk is blank', function () {
            args[1] = 'revise';
            args[4] = parseInt(undefined, 10);
        });

        it('trying to add and family_pk is blank', function () {
            args[1] = 'add';
            args[4] = parseInt(undefined, 10);
        });

    });

    describe('should not throw an error when', function () {

        afterEach(function() {
            expect(function() {
                buildPipelineSubmit.apply(null, args);
            }).not.toThrow();
        });

        it('trying to add and parent_revision_id is missing', function () {
            args[1] = 'add';
            args[7] = undefined;
        });

        it('trying to create new family and parent_revision_id is missing', function () {
            args[1] = 'new';
            args[7] = undefined;
        });

        it('trying to create new family and $family_pk is missing', function () {
            args[1] = 'new';
            args[4] = parseInt(undefined, 10);
        });

    });

    describe("generated event handler function", function() {

        beforeAll(function() {
            mockPipeline();
        });

        beforeEach(function() {
            built_submit = buildPipelineSubmit.apply(null, args);
            jasmine.Ajax.install();
        });

        afterEach(function() {
            jasmine.Ajax.uninstall();
        });

        it('should be a function', function() {
            expect(built_submit).toEqual(jasmine.any(Function));
        });

        it('should prevent default', function() {
            let event = new Event('submit');
            spyOn(event, 'preventDefault');
            built_submit(event);
            expect(event.preventDefault).toHaveBeenCalled();
        });

        it('should generate a user error message when $family_name is empty', function () {
            args[2].val('');
            $error.empty();
            built_submit(new Event('submit'));
            expect($error).not.toBeEmpty();
        });

        it('should submit a new pipeline family and first revision', function() {
            built_submit(new Event('submit'));

            // @types for JasmineAjaxRequest seems to be a bit spotty.
            let request: any = jasmine.Ajax.requests.mostRecent();

            expect(request.url).toBe("/api/pipelinefamilies/");
            expect(request.method).toBe('POST');
            expect(request.data()).toEqual({
                users_allowed:  [ "b", "c" ],
                groups_allowed: [ "b", "c" ],
                name: 'custom_family_name',
                description: 'custom_family_desc',
            });

            request.respondWith({
                status: 200,
                statusText: 'HTTP/1.1 200 OK',
                contentType: 'application/json;charset=UTF-8',
                responseText: "{ \"id\": 6 }"
            });

            request = jasmine.Ajax.requests.mostRecent();
            expect(request.url).toBe("/api/pipelines/");
            expect(request.method).toBe('POST');

            let requestData = request.data();
            expect(requestData.users_allowed).toEqual([ "b", "c" ]);
            expect(requestData.groups_allowed).toEqual([ "b", "c" ]);
            expect(requestData.family).toEqual('custom_family_name');
            expect(requestData.family_desc).toEqual('custom_family_desc');
            expect(requestData.revision_name).toEqual('custom_revision_name');
            expect(requestData.revision_desc).toEqual('custom_revision_desc');
            expect(requestData.revision_parent).toEqual(null);
            expect(requestData.published).toEqual(false);
            expect(requestData.canvas_width).toEqual(300);
            expect(requestData.canvas_height).toEqual(150);
        });

        it('should handle API errors for pipeline family', function() {
            built_submit(new Event('submit'));

            jasmine.Ajax.requests.mostRecent().respondWith({
                status: 500,
                statusText: 'HTTP/1.1 500 Internal Server Error',
                contentType: 'application/json;charset=UTF-8',
                responseText: "{ \"non_field_errors\": [ \"custom error message\" ] }"
            });

            expect($error).toContainText("custom error message");
            expect($error).toBeVisible();
        });

        it('should handle API errors for pipeline revision', function() {
            built_submit(new Event('submit'));

            jasmine.Ajax.requests.mostRecent().respondWith({
                status: 200,
                statusText: 'HTTP/1.1 200 OK',
                contentType: 'application/json;charset=UTF-8',
                responseText: "{ \"id\": 6 }"
            });

            jasmine.Ajax.requests.mostRecent().respondWith({
                status: 500,
                statusText: 'HTTP/1.1 500 Internal Server Error',
                contentType: 'application/json;charset=UTF-8',
                responseText: "{ \"non_field_errors\": [ \"custom error message\" ] }"
            });

            expect($error).toContainText("custom error message");
            expect($error).toBeVisible();
        });

    });

    it('should submit a new pipeline revision to an empty pipeline family', function() {
        mockPipeline();
        args[1] = "add";
        built_submit = buildPipelineSubmit.apply(null, args);
        jasmine.Ajax.install();

        built_submit(new Event('submit'));

        // @types for JasmineAjaxRequest seems to be a bit spotty.
        let request: any = jasmine.Ajax.requests.mostRecent();
        expect(request.url).toBe("/api/pipelines/");
        expect(request.method).toBe('POST');

        let requestData = request.data();
        expect(requestData.users_allowed).toEqual([ "b", "c" ]);
        expect(requestData.groups_allowed).toEqual([ "b", "c" ]);
        expect(requestData.family).toEqual('custom_family_name');
        expect(requestData.family_desc).toEqual('custom_family_desc');
        expect(requestData.revision_name).toEqual('custom_revision_name');
        expect(requestData.revision_desc).toEqual('custom_revision_desc');
        expect(requestData.revision_parent).toEqual(null);
        expect(requestData.published).toEqual(false);
        expect(requestData.canvas_width).toEqual(300);
        expect(requestData.canvas_height).toEqual(150);

        jasmine.Ajax.uninstall();

    });

    it('should submit a new pipeline revision with a parent revision', function() {

        mockPipeline();
        args[1] = "revise";
        args[7] = 1;
        built_submit = buildPipelineSubmit.apply(null, args);
        jasmine.Ajax.install();

        built_submit(new Event('submit'));

        // @types for JasmineAjaxRequest seems to be a bit spotty.
        let request: any = jasmine.Ajax.requests.mostRecent();
        expect(request.url).toBe("/api/pipelines/");
        expect(request.method).toBe('POST');

        let requestData = request.data();
        expect(requestData.users_allowed).toEqual([ "b", "c" ]);
        expect(requestData.groups_allowed).toEqual([ "b", "c" ]);
        expect(requestData.family).toEqual('custom_family_name');
        expect(requestData.family_desc).toEqual('custom_family_desc');
        expect(requestData.revision_name).toEqual('custom_revision_name');
        expect(requestData.revision_desc).toEqual('custom_revision_desc');
        expect(requestData.revision_parent).toEqual(1);
        expect(requestData.published).toEqual(false);
        expect(requestData.canvas_width).toEqual(300);
        expect(requestData.canvas_height).toEqual(150);

        jasmine.Ajax.uninstall();

    });

});
