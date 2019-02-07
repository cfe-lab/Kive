import { CanvasWrapper } from "@container/canvas/drydock_objects";
import { CanvasState } from "@container/canvas/drydock";
import { Pipeline } from "@container/io/pipeline_load";
import { serializePipeline } from "@container/io/serializer";
import * as imagediff from 'imagediff';

describe("Container pipeline functions", function() {
    let $error: JQuery;

    beforeEach(function() {
        let width = 600,
            height = 300;

        jasmine.addMatchers(imagediff.jasmine);
        this.rawCanvas = imagediff.createCanvas(width, height);
        this.canvas = new CanvasWrapper(this.rawCanvas);
        this.canvasState = new CanvasState(this.rawCanvas, true);
        $error = $('<div>').appendTo('body').hide();

        this.api_pipeline = {
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
    });

    afterEach(function() {
        expect('Suppress SPEC HAS NO EXPECTATIONS').toBeDefined();
        $error.remove();
    });

    function loadApiPipeline(canvasState, pipeline) {
        let ppln = new Pipeline(canvasState);
        ppln.load(pipeline, $error);
        return ppln;
    }

    describe('Load', function(){
        it('should load pipeline from API', function() {
            loadApiPipeline(this.canvasState, this.api_pipeline);
        });

        it('should draw pipeline from API', function() {
            let pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
            pipeline.draw();
        });

        it('should autolayout from API', function() {
            let pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
            pipeline.draw();
            this.canvasState.autoLayout();
        });

        it('should find pipeline nodes from API', function() {
            loadApiPipeline(this.canvasState, this.api_pipeline);

            let input1  = this.canvasState.findNodeByLabel('input1'),
                input2  = this.canvasState.findNodeByLabel('input2'),
                prelim  = this.canvasState.findNodeByLabel('prelim_map.py'),
                remap   = this.canvasState.findNodeByLabel('remap.py'),
                remapc  = this.canvasState.findNodeByLabel('remap_counts'),
                remapcs = this.canvasState.findNodeByLabel('remap_conseq'),
                remapp  = this.canvasState.findNodeByLabel('remap'),
                umfasq1 = this.canvasState.findNodeByLabel('unmapped2_fastq'),
                umfasq2 = this.canvasState.findNodeByLabel('unmapped1_fastq'),
                jmguire = this.canvasState.findNodeByLabel('Jerry Maguire');

            expect(input1).toBeDefined();
            expect(input2).toBeDefined();
            expect(prelim).toBeDefined();
            expect(remap).toBeDefined();
            expect(remapc).toBeDefined();
            expect(remapcs).toBeDefined();
            expect(remapp).toBeDefined();
            expect(umfasq1).toBeDefined();
            expect(umfasq2).toBeDefined();
            expect(jmguire).toBeUndefined();
        });

        it('should load invalid pipeline and complain', function() {
            let bad_container = {
                files: ["foo.txt"],
                pipeline: "not at all what should be here"
            };

            loadApiPipeline(this.canvasState, bad_container);

            expect($error).toContainText("Could not load pipeline.");
            expect($error).toBeVisible();
        });

        it('should reset after invalid pipeline', function() {
            let bad_container = {
                files: ["foo.txt"],
                pipeline: {
                    inputs: [{
                        dataset_name: "in_csv"
                    }],
                    steps: "What is this mess?"
                }
            };

            loadApiPipeline(this.canvasState, bad_container);

            expect(this.canvasState.shapes.length).toBe(0);
        });
    });

    describe('Connections', function(){

        it('should connect inputs to methods API', function(){
            loadApiPipeline(this.canvasState, this.api_pipeline);

            let input1 = this.canvasState.findNodeByLabel('input1'),
                input2 = this.canvasState.findNodeByLabel('input2'),
                prelim = this.canvasState.findNodeByLabel('prelim_map.py'),
                remap  = this.canvasState.findNodeByLabel('remap.py');

            expect(input1.isConnectedTo(input2)).toBe(false);
            expect(input1.isConnectedTo(prelim)).toBe(true);
            expect(input2.isConnectedTo(prelim)).toBe(true);
            expect(input1.isConnectedTo(remap)).toBe(true);
            expect(input2.isConnectedTo(remap)).toBe(true);
        });

        it('should connect methods API', function(){
            loadApiPipeline(this.canvasState, this.api_pipeline);

            let prelim = this.canvasState.findNodeByLabel('prelim_map.py'),
                remap  = this.canvasState.findNodeByLabel('remap.py');

            expect(prelim.isConnectedTo(remap)).toBe(true);
        });

        it('should connect methods to outputs API', function(){
            loadApiPipeline(this.canvasState, this.api_pipeline);

            let remap   = this.canvasState.findNodeByLabel('remap.py'),
                remapc  = this.canvasState.findNodeByLabel('remap_counts'),
                remapcs = this.canvasState.findNodeByLabel('remap_conseq'),
                remapp  = this.canvasState.findNodeByLabel('remap'),
                umfasq1 = this.canvasState.findNodeByLabel('unmapped2_fastq'),
                umfasq2 = this.canvasState.findNodeByLabel('unmapped1_fastq');

            expect(remapc.isConnectedTo(remap)).toBe(true);
            expect(remapcs.isConnectedTo(remap)).toBe(true);
            expect(remapp.isConnectedTo(remap)).toBe(true);
            expect(umfasq1.isConnectedTo(remap)).toBe(true);
            expect(umfasq2.isConnectedTo(remap)).toBe(true);
            expect(remapc.isConnectedTo(umfasq2)).toBe(false);
        });
    });

    describe('Structure', function(){

        it('should have correct properties for inputs API', function(){
            let pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
            pipeline.draw();

            let input1 = this.canvasState.findNodeByLabel('prelim_map.py'),
                input2 = this.canvasState.findNodeByLabel('input2');

            let i1keys = Object.keys(input1),
                i2keys = Object.keys(input2);

            $.each(['x', 'y', 'dx', 'dy', 'fill', 'label'], function(_, key){
                expect(i1keys).toContain(key);
                expect(i2keys).toContain(key);
            });
        });

        it('should have correct properties for steps API', function(){
            let pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
            pipeline.draw();

            let prelim = this.canvasState.findNodeByLabel('prelim_map.py'),
                remap  = this.canvasState.findNodeByLabel('remap.py');

            let i1keys = Object.keys(prelim),
                i2keys = Object.keys(remap);

            $.each(['x', 'y', 'dx', 'dy', 'fill', 'label'], function(_, key){
                expect(i1keys).toContain(key);
                expect(i2keys).toContain(key);
            });

            expect(prelim.out_magnets[0].connected.length).toBe(1);
            expect(remap.out_magnets[0].connected.length).toBe(1);
        });

        it('should have correct properties for output API', function(){
            let pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
            pipeline.draw();

            let remapc  = this.canvasState.findNodeByLabel('remap_counts'),
                remapcs = this.canvasState.findNodeByLabel('remap_conseq');

            let i1keys = Object.keys(remapc),
                i2keys = Object.keys(remapcs);

            $.each(['x', 'y', 'dx', 'dy', 'fill', 'label'], function(_, key){
                expect(i1keys).toContain(key);
                expect(i2keys).toContain(key);
            });
        });
    });

    function loadAndSerialize(canvasState, api_pipeline, additional_args?) {
        let pipeline = loadApiPipeline(canvasState, api_pipeline);
        pipeline.draw();
        return serializePipeline(canvasState, additional_args);
    }

    describe('Serialize', function(){
        it('should serialize', function(){
            loadAndSerialize(this.canvasState, this.api_pipeline);
        });

        it('should check structure', function() {
            let serialized = loadAndSerialize(
                this.canvasState,
                this.api_pipeline
            );

            expect(serialized.pipeline.steps).toBeDefined();
            expect(serialized.pipeline.inputs).toBeDefined();
            expect(serialized.pipeline.outputs).toBeDefined();
        });

        it('should match original pipeline inputs', function(){
            let self = this,
                serialized = loadAndSerialize(
                    this.canvasState,
                    this.api_pipeline
                );

            $.each(serialized.pipeline.inputs, function(index, ser_input){
                let api_input = self.api_pipeline.pipeline.inputs[index];

                expect(ser_input.dataset_name).toBe(api_input.dataset_name);
                expect(ser_input.x).toBeCloseTo(api_input.x, 8);
                expect(ser_input.y).toBeCloseTo(api_input.y, 8);
            });
        });

        it('should match original pipeline steps', function() {
            let self = this,
                serialized = loadAndSerialize(
                    this.canvasState,
                    this.api_pipeline
                );

            expect(serialized.pipeline.steps.length).toBe(
                self.api_pipeline.pipeline.steps.length);
            $.each(serialized.pipeline.steps, function(index, ser_step){
                let api_step = self.api_pipeline.pipeline.steps[index];

                expect(ser_step.driver).toBe(api_step.driver);
                expect(ser_step.x).toBeCloseTo(api_step.x, 8);
                expect(ser_step.y).toBeCloseTo(api_step.y, 8);

            });
        });

        it('should match original pipeline steps (cables_in)', function() {
            let self = this,
                serialized = loadAndSerialize(
                    this.canvasState,
                    this.api_pipeline
                );

            $.each(serialized.pipeline.steps, function(index, ser_step){
                let api_step = self.api_pipeline.pipeline.steps[index];

                expect(ser_step.inputs.length).toBe(api_step.inputs.length);
                $.each(ser_step.inputs, function(cable_index, ser_cable){
                    let api_cable = api_step.inputs[cable_index];

                    expect(ser_cable.dataset_name).toBe(api_cable.dataset_name);
                    expect(ser_cable.source_step).toBe(api_cable.source_step);
                    expect(ser_cable.source_dataset_name).toBe(api_cable.source_dataset_name);
                });
            });
        });

        it('should match original pipeline steps (cables_out)', function() {
            let self = this,
                serialized = loadAndSerialize(
                    this.canvasState,
                    this.api_pipeline
                );

            $.each(serialized.pipeline.steps, function(index, ser_step){
                let api_step = self.api_pipeline.pipeline.steps[index];

                expect(ser_step.outputs).toEqual(api_step.outputs);
            });
        });

        it('should match original pipeline output', function() {
            let self = this,
                serialized = loadAndSerialize(
                    this.canvasState,
                    this.api_pipeline
                );

            $.each(serialized.pipeline.outputs, function(index, ser_output){
                let api_output = self.api_pipeline.pipeline.outputs[index];

                expect(ser_output.dataset_name).toBe(api_output.dataset_name);
                expect(ser_output.source_dataset_name).toBe(api_output.source_dataset_name);
                expect(ser_output.source_step).toBe(api_output.source_step);
                expect(ser_output.x).toBeCloseTo(api_output.x, 8);
                expect(ser_output.y).toBeCloseTo(api_output.y, 8);
            });
        });
    });
});