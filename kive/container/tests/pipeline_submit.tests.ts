import { CanvasState } from "@container/canvas/drydock";
import { buildPipelineSubmit } from "@container/io/pipeline_submit";
import { RawNode, MethodNode, OutputNode, Connector } from "@container/canvas/drydock_objects";
import * as imagediff from 'imagediff';

(window as any).$ = (window as any).jQuery = $;
require("@portal/noxss.js");

interface BuildSubmitArgs extends Array<any> {
    0: CanvasState;
    1: JQuery; // container_pk
    2: JQuery; // memory
    3: JQuery; // threads
    4: JQuery; // $error
}

describe("Container Pipeline Submit class", function() {

    // static vars
    let canvasState;
    let args: BuildSubmitArgs,
        $error: JQuery,
        $container_pk = $('<input type="text" value="0">'),
        $memory = $('<input type="text" value="400">'),
        $threads = $('<input type="text" value="2">'),
        arg_names = [
            "canvasState",
            "container_pk",
            "memory",
            "threads",
            "$error"
        ];

    let built_submit;

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
            $container_pk,
            $memory,
            $threads,
            $error // error outlet
        ];

    });

    afterEach(function() {
        $error.remove();
    });

    it('should attach a CSRF token to each AJAX request', function() {
        expect($.ajaxSettings.beforeSend).not.toBeUndefined();
        expect($.ajaxSettings.beforeSend.toString()).toMatch(
            /\.setRequestHeader\s*\(\s*["']X-CSRFToken["']\s*,\s*[A-Za-z_$]+\s*\)/
        );
    });

    describe('should throw an error when', function() {

        let $dummy = $('#nonexistent');
        let bad_args = [
            null, $dummy, $dummy, $dummy, $dummy
        ];

        afterEach(function() {
            expect(function() {
                buildPipelineSubmit.apply(null, args);
            }).toThrow();
        });

        for (let i = 0; i < arg_names.length; i++) {
            it(arg_names[i] + ' is not found', function () {
                args[i] = bad_args[i];
            });
        }

    });

    describe('should not throw an error when', function () {

        afterEach(function() {
            expect(function() {
                buildPipelineSubmit.apply(null, args);
            }).not.toThrow();
        });

        it('all fields are set', function () {
        });

    });

    describe("generated event handler function", function() {

        beforeAll(function() {
            mockPipeline();
        });

        beforeEach(function() {
            $container_pk.val('42');  // container_pk
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

        it('should submit an updated pipeline', function() {
            built_submit(new Event('submit'));

            // @types for JasmineAjaxRequest seems to be a bit spotty.
            let request: any = jasmine.Ajax.requests.mostRecent();

            expect(request.url).toBe("/api/containers/42/content/");
            expect(request.method).toBe('PUT');

            let requestData = request.data();
            expect(requestData.pipeline.inputs[0].dataset_name).toEqual("raw_node");
            expect(requestData.pipeline.steps[0].driver).toEqual("method_node");
            expect(requestData.pipeline.outputs[0].dataset_name).toEqual("output_node");
            expect(requestData.pipeline.default_config.memory).toEqual(400);
            expect(requestData.pipeline.default_config.threads).toEqual(2);
        });

        it('should handle API errors for pipeline revision', function() {
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

    });
});
