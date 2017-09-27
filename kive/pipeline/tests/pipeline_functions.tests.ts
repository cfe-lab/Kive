import { CanvasWrapper } from "../static/pipeline/canvas/drydock_objects";
import { CanvasState } from "../static/pipeline/canvas/drydock";
import { Pipeline } from "../static/pipeline/io/pipeline_load";
import { serializePipeline } from "../static/pipeline/io/serializer";
import * as imagediff from 'imagediff';

describe("Pipeline functions", function() {
    beforeEach(function() {
        var width = 600,
            height = 300;

        jasmine.addMatchers(imagediff.jasmine);
        this.rawCanvas = imagediff.createCanvas(width, height);
        this.canvas = new CanvasWrapper(this.rawCanvas);
        this.canvasState = new CanvasState(this.rawCanvas, true);

        this.api_pipeline = {
            "id": 25,
            "url": "http://127.0.0.1:8000/api/pipelines/25/",
            "family_pk": 2,
            "family": "Test",
            "revision_name": "",
            "revision_desc": "Carl Sagan's science slam-jam",
            "revision_number": 3,
            "revision_parent": 24,
            "revision_DateTime": "2015-06-10T19:36:31.570191Z",
            "user": "kive",
            "users_allowed": [],
            "groups_allowed": [],
            "inputs": [{
                "dataset_name": "input2",
                "dataset_idx": 1,
                "x": 0.15,
                "y": 0.15,
                "structure": {
                    "compounddatatype": 9,
                    "min_row": null,
                    "max_row": null
                }
            }, {
                "dataset_name": "input1",
                "dataset_idx": 2,
                "x": 0.05,
                "y": 0.3,
                "structure": null
            }],
            "outputs": [{
                "dataset_name": "unmapped2_fastq",
                "dataset_idx": 1,
                "x": 0.637772562280456,
                "y": 0.633208895290869,
                "structure": null
            }, {
                "dataset_name": "unmapped1_fastq",
                "dataset_idx": 2,
                "x": 0.637772562280456,
                "y": 0.633208895290869,
                "structure": null
            }, {
                "dataset_name": "remap_conseq",
                "dataset_idx": 3,
                "x": 0.637772562280456,
                "y": 0.633208895290869,
                "structure": {
                    "compounddatatype": 10,
                    "min_row": null,
                    "max_row": null
                }
            }, {
                "dataset_name": "remap",
                "dataset_idx": 4,
                "x": 0.637772562280456,
                "y": 0.633208895290869,
                "structure": {
                    "compounddatatype": 8,
                    "min_row": null,
                    "max_row": null
                }
            }, {
                "dataset_name": "remap_counts",
                "dataset_idx": 5,
                "x": 0.637772562280456,
                "y": 0.633208895290869,
                "structure": {
                    "compounddatatype": 9,
                    "min_row": null,
                    "max_row": null
                }
            }],
            "steps": [{
                "transformation": 4,
                "transformation_family": 3,
                "step_num": 1,
                "outputs_to_delete": [],
                "x": 0.344662650584514,
                "y": 0.5,
                "name": "prelim_map.py",
                "cables_in": [{
                    "source_step": 0,
                    "source": 143,
                    "source_dataset_name": "input1",
                    "dest": 7,
                    "dest_dataset_name": "fastq1",
                    "custom_wires": [],
                    "keep_output": false
                }, {
                    "source_step": 0,
                    "source": 142,
                    "source_dataset_name": "input2",
                    "dest": 8,
                    "dest_dataset_name": "fastq2",
                    "custom_wires": [],
                    "keep_output": false
                }],
                "outputs": [{
                    "dataset_name": "prelim",
                    "dataset_idx": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 7,
                        "min_row": null,
                        "max_row": null
                    }
                }],
                "inputs": [{
                    "dataset_name": "fastq1",
                    "dataset_idx": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": null
                }, {
                    "dataset_name": "fastq2",
                    "dataset_idx": 2,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": null
                }]
            }, {
                "transformation": 5,
                "transformation_family": 4,
                "step_num": 2,
                "outputs_to_delete": [],
                "x": 0.450583501602465,
                "y": 0.257130788000083,
                "name": "remap.py",
                "cables_in": [{
                    "source_step": 0,
                    "source": 143,
                    "source_dataset_name": "input1",
                    "dest": 10,
                    "dest_dataset_name": "fastq1",
                    "custom_wires": [],
                    "keep_output": false
                }, {
                    "source_step": 0,
                    "source": 142,
                    "source_dataset_name": "input2",
                    "dest": 11,
                    "dest_dataset_name": "fastq2",
                    "custom_wires": [],
                    "keep_output": false
                }, {
                    "source_step": 1,
                    "source": 9,
                    "source_dataset_name": "prelim",
                    "dest": 12,
                    "dest_dataset_name": "prelim",
                    "custom_wires": [],
                    "keep_output": false
                }],
                "outputs": [{
                    "dataset_name": "remap",
                    "dataset_idx": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 8,
                        "min_row": null,
                        "max_row": null
                    }
                }, {
                    "dataset_name": "remap_counts",
                    "dataset_idx": 2,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 9,
                        "min_row": null,
                        "max_row": null
                    }
                }, {
                    "dataset_name": "remap_conseq",
                    "dataset_idx": 3,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 10,
                        "min_row": null,
                        "max_row": null
                    }
                }, {
                    "dataset_name": "unmapped1_fastq",
                    "dataset_idx": 4,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": null
                }, {
                    "dataset_name": "unmapped2_fastq",
                    "dataset_idx": 5,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": null
                }],
                "inputs": [{
                    "dataset_name": "fastq1",
                    "dataset_idx": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": null
                }, {
                    "dataset_name": "fastq2",
                    "dataset_idx": 2,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": null
                }, {
                    "dataset_name": "prelim",
                    "dataset_idx": 3,
                    "x": 0.0,
                    "y": 0.0,
                    "structure": {
                        "compounddatatype": 7,
                        "min_row": null,
                        "max_row": null
                    }
                }]
            }],
            "outcables": [{
                "pk": 128,
                "output_idx": 1,
                "output_name": "unmapped2_fastq",
                "output_cdt": null,
                "source_step": 2,
                "source": 55,
                "source_dataset_name": "unmapped2_fastq",
                "custom_wires": []
            }, {
                "pk": 129,
                "output_idx": 2,
                "output_name": "unmapped1_fastq",
                "output_cdt": null,
                "source_step": 2,
                "source": 54,
                "source_dataset_name": "unmapped1_fastq",
                "custom_wires": []
            }, {
                "pk": 130,
                "output_idx": 3,
                "output_name": "remap_conseq",
                "output_cdt": 10,
                "source_step": 2,
                "source": 15,
                "source_dataset_name": "remap_conseq",
                "custom_wires": []
            }, {
                "pk": 131,
                "output_idx": 4,
                "output_name": "remap",
                "output_cdt": 8,
                "source_step": 2,
                "source": 13,
                "source_dataset_name": "remap",
                "custom_wires": []
            }, {
                "pk": 132,
                "output_idx": 5,
                "output_name": "remap_counts",
                "output_cdt": 9,
                "source_step": 2,
                "source": 14,
                "source_dataset_name": "remap_counts",
                "custom_wires": []
            }],
            "removal_plan": "http://127.0.0.1:8000/api/pipelines/25/removal_plan/"
        };
    });

    afterEach(function() {
        expect('Suppress SPEC HAS NO EXPECTATIONS').toBeDefined();
    });

    function loadApiPipeline(canvasState, pipeline) {
        var ppln = new Pipeline(canvasState);
        ppln.load(pipeline);
        return ppln;
    }

    describe('Load', function(){
        it('should load pipeline from API', function() {
            loadApiPipeline(this.canvasState, this.api_pipeline);
        });

        it('should draw pipeline from API', function() {
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
            pipeline.draw();
        });

        it('should autolayout from API', function() {
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
            pipeline.draw();
            this.canvasState.autoLayout();
        });

        it('should find pipeline nodes from API', function() {
            loadApiPipeline(this.canvasState, this.api_pipeline);

            var input1  = this.canvasState.findNodeByLabel('input1'),
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
    });

    describe('Connections', function(){

        it('should connect inputs to methods API', function(){
            loadApiPipeline(this.canvasState, this.api_pipeline);

            var input1 = this.canvasState.findNodeByLabel('input1'),
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

            var prelim = this.canvasState.findNodeByLabel('prelim_map.py'),
                remap  = this.canvasState.findNodeByLabel('remap.py');

            expect(prelim.isConnectedTo(remap)).toBe(true);
        });

        it('should connect methods to outputs API', function(){
            loadApiPipeline(this.canvasState, this.api_pipeline);

            var remap   = this.canvasState.findNodeByLabel('remap.py'),
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

        it('should ignore order of cables_in when wiring', function(){
            let pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
            pipeline.draw();

            // Now reverse the order of the cables_in
            let expectedCanvas = imagediff.createCanvas(600, 300);
            let expectedCanvasState = new CanvasState(expectedCanvas, true);
            this.api_pipeline.steps[1].cables_in.reverse();
            pipeline = loadApiPipeline(expectedCanvasState, this.api_pipeline);
            pipeline.draw();

            (expect(this.rawCanvas) as any).toImageDiffEqual(
                expectedCanvas);
        });
    });

    describe('Structure', function(){

        it('should have correct properties for inputs API', function(){
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
            pipeline.draw();

            var input1 = this.canvasState.findNodeByLabel('prelim_map.py'),
                input2 = this.canvasState.findNodeByLabel('input2');

            var i1keys = Object.keys(input1),
                i2keys = Object.keys(input2);

            $.each(['x', 'y', 'dx', 'dy', 'fill', 'label'], function(_, key){
                expect(i1keys).toContain(key);
                expect(i2keys).toContain(key);
            });
        });

        it('shoud have correct properties for inputs API', function(){
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
            pipeline.draw();

            var prelim = this.canvasState.findNodeByLabel('prelim_map.py'),
                remap  = this.canvasState.findNodeByLabel('remap.py');

            var i1keys = Object.keys(prelim),
                i2keys = Object.keys(remap);

            $.each(['x', 'y', 'dx', 'dy', 'fill', 'label', 'family'], function(_, key){
                expect(i1keys).toContain(key);
                expect(i2keys).toContain(key);
            });

            expect(prelim.family).toBe(3);
            expect(remap.family).toBe(4);
            expect(prelim.out_magnets[0].connected.length).toBe(1);
            expect(remap.out_magnets[0].connected.length).toBe(1);
        });

        it('should have correct properties for output API', function(){
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
            pipeline.draw();

            var remapc  = this.canvasState.findNodeByLabel('remap_counts'),
                remapcs = this.canvasState.findNodeByLabel('remap_conseq');

            var i1keys = Object.keys(remapc),
                i2keys = Object.keys(remapcs);

            $.each(['x', 'y', 'dx', 'dy', 'fill', 'label'], function(_, key){
                expect(i1keys).toContain(key);
                expect(i2keys).toContain(key);
            });
        });
    });

    describe('Update steps', function(){
        it('should apply method update', function() {
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline),
                new_method_id = 77,
                step_updates = [{
                    step_num: 1,
                    method: {
                        id: new_method_id,
                        family_id: 3,
                        inputs: [{
                            dataset_name: "fastq1",
                            dataset_idx: 1,
                            structure: null
                        },
                            {
                                dataset_name: "fastq2",
                                dataset_idx: 2,
                                structure: null
                            }],
                        outputs: [{
                            dataset_name: "prelim",
                            dataset_idx: 1,
                            structure: { compounddatatype: 7 }
                        }]
                    }
                }];

            pipeline.applyStepRevisions(step_updates);
            var new_method = this.canvasState.findNodeByLabel('prelim_map.py');
            expect(new_method.pk).toBe(new_method_id);
        });

        it('should notify the user of updates', function(){
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline),
                new_method_id = 77,
                step_updates = [{
                    step_num: 1,
                    method: {
                        id: new_method_id,
                        family_id: 3,
                        inputs: [{
                            dataset_name: "fastq1",
                            dataset_idx: 1,
                            structure: null
                        },
                            {
                                dataset_name: "fastq2",
                                dataset_idx: 2,
                                structure: null
                            }],
                        outputs: [{
                            dataset_name: "prelim",
                            dataset_idx: 1,
                            structure: { compounddatatype: 7 }
                        }]
                    }
                }];

            pipeline.applyStepRevisions(step_updates);
            var updated_method = this.canvasState.findNodeByLabel('prelim_map.py');
            var no_updates_found_method = this.canvasState.findNodeByLabel('remap.py');
            expect(updated_method.update_signal.status).toBe('updated');
            expect(no_updates_found_method.update_signal.status).toBe('no update available');
        });

        it('should notify the user if an updated method has changed inputs', function(){
            // changed input
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline),
                new_method_id = 78,
                step_updates = [{
                    step_num: 1,
                    method: {
                        id: new_method_id,
                        family_id: 3,
                        inputs: [{
                            dataset_name: "fastq1",
                            dataset_idx: 1,
                            structure: null
                        },
                            {
                                dataset_name: "new_cdt",
                                dataset_idx: 2,
                                structure: { compounddatatype: 19 }
                            }],
                        outputs: [{
                            dataset_name: "prelim",
                            dataset_idx: 1,
                            structure: { compounddatatype: 7 }
                        }]
                    }
                }];

            pipeline.applyStepRevisions(step_updates);
            var updated_method = this.canvasState.findNodeByLabel('prelim_map.py');
            expect(updated_method.update_signal.status).toBe('updated with issues');
        });

        it('should notify the user if an updated method has changed outputs', function(){
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline),
                new_method_id = 78,
                step_updates = [{
                    step_num: 1,
                    method: {
                        id: new_method_id,
                        family_id: 3,
                        inputs: [{
                            dataset_name: "fastq1",
                            dataset_idx: 1,
                            structure: null
                        },
                            {
                                dataset_name: "fastq2",
                                dataset_idx: 2,
                                structure: null
                            }],
                        outputs: [{
                            dataset_name: "changed_output",
                            dataset_idx: 1,
                            structure: { compounddatatype: 19 }
                        }]
                    }
                }];

            pipeline.applyStepRevisions(step_updates);
            var updated_method = this.canvasState.findNodeByLabel('prelim_map.py');
            expect(updated_method.update_signal.status).toBe('updated with issues');
        });

        it('should notify the user if an updated method has new inputs', function(){
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline),
                new_method_id = 78,
                step_updates = [{
                    step_num: 1,
                    method: {
                        id: new_method_id,
                        family_id: 3,
                        inputs: [{
                            dataset_name: "fastq1",
                            dataset_idx: 1,
                            structure: null
                        },
                            {
                                dataset_name: "fastq2",
                                dataset_idx: 2,
                                structure: null
                            }, {
                                dataset_name: "new_input",
                                dataset_idx: 3,
                                structure: null
                            }],
                        outputs: [{
                            dataset_name: "prelim",
                            dataset_idx: 1,
                            structure: { compounddatatype: 7 }
                        }]
                    }
                }];

            pipeline.applyStepRevisions(step_updates);
            var updated_method = this.canvasState.findNodeByLabel('prelim_map.py');
            expect(updated_method.update_signal.status).toBe('updated with issues');
        });

        it('should notify the user if an updated method has new outputs', function(){
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline),
                new_method_id = 78,
                step_updates = [{
                    step_num: 1,
                    method: {
                        id: new_method_id,
                        family_id: 3,
                        inputs: [{
                            dataset_name: "fastq1",
                            dataset_idx: 1,
                            structure: null
                        },
                            {
                                dataset_name: "fastq2",
                                dataset_idx: 2,
                                structure: null
                            }],
                        outputs: [{
                            dataset_name: "prelim",
                            dataset_idx: 1,
                            structure: { compounddatatype: 7 }
                        }, {
                            dataset_name: "new_output",
                            dataset_idx: 2,
                            structure: { compounddatatype: 19 }
                        }]
                    }
                }];

            pipeline.applyStepRevisions(step_updates);
            var updated_method = this.canvasState.findNodeByLabel('prelim_map.py');
            expect(updated_method.update_signal.status).toBe('updated with issues');
        });

        it('should apply code resource update', function() {
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline),
                step_updates = [{
                    step_num: 1,
                    code_resource_revision: {
                        id: 77,
                        revision_name: "new feature"
                    }
                }];
            pipeline.applyStepRevisions(step_updates);

            var new_method = this.canvasState.findNodeByLabel('prelim_map.py');

            expect(new_method.new_code_resource_revision).toBe(
                step_updates[0].code_resource_revision,
                "new_code_resource_revision");
        });

        it('should apply dependency update', function() {
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline),
                step_updates = [{
                    step_num: 1,
                    dependencies: [{
                        id: 77,
                        revision_name: "new feature"
                    }]
                }];
            pipeline.applyStepRevisions(step_updates);

            var new_method = this.canvasState.findNodeByLabel('prelim_map.py');

            expect(new_method.new_dependencies).toBe(
                step_updates[0].dependencies,
                "new_dependencies");
        });
    });

    function loadAndSerialize(canvasState, api_pipeline, additional_args?) {
        var pipeline = loadApiPipeline(canvasState, api_pipeline);
        pipeline.draw();
        return serializePipeline(canvasState, additional_args);
    }

    describe('Serialize', function(){
        it('should serialize', function(){
            var test = loadAndSerialize(this.canvasState, this.api_pipeline);
        });

        it('should check structure', function() {
            var serialized = loadAndSerialize(
                this.canvasState,
                this.api_pipeline,
                { family: "pass this argument thru" }
            );

            expect(serialized.family).toBe("pass this argument thru");
            expect(serialized.steps).toBeDefined();
            expect(serialized.inputs).toBeDefined();
            expect(serialized.outcables).toBeDefined();
        });

        it('should match original pipeline inputs', function(){
            var self = this,
                serialized = loadAndSerialize(
                    this.canvasState,
                    this.api_pipeline
                );

            // TODO: These (inputs) should really be sorted by dataset_idx
            $.each(serialized.inputs, function(index, ser_input){
                var api_input = self.api_pipeline.inputs[index];

                expect(ser_input.dataset_name).toBe(api_input.dataset_name);
                expect(ser_input.dataset_idx).toBe(api_input.dataset_idx);
                expect(ser_input.x).toBeCloseTo(api_input.x, 8);
                expect(ser_input.y).toBeCloseTo(api_input.y, 8);

                if (ser_input.structure === null) {
                    expect(ser_input.structure).toBe(api_input.structure);
                } else {
                    expect(ser_input.structure.compounddatatype).toBe(api_input.structure.compounddatatype);
                }
            });
        });

        it('should match original pipeline steps', function() {
            var self = this,
                serialized = loadAndSerialize(
                    this.canvasState,
                    this.api_pipeline
                );

            // TODO: These (inputs) should really be sorted by dataset_idx
            $.each(serialized.steps, function(index, ser_step){
                var api_step = self.api_pipeline.steps[index];

                expect(ser_step.name).toBe(api_step.name);
                expect(ser_step.step_num).toBe(api_step.step_num);
                expect(ser_step.transformation).toBe(api_step.transformation);
                expect(ser_step.x).toBeCloseTo(api_step.x, 8);
                expect(ser_step.y).toBeCloseTo(api_step.y, 8);

            });
        });

        it('should match original pipeline steps (cables_in)', function() {
            var self = this,
                serialized = loadAndSerialize(
                    this.canvasState,
                    this.api_pipeline
                );

            // TODO: These (inputs) should really be sorted by dataset_idx
            $.each(serialized.steps, function(index, ser_step){
                var api_step = self.api_pipeline.steps[index];

                $.each(ser_step.cables_in, function(cable_index, ser_cable){
                    var api_cable = api_step.cables_in[cable_index];

                    expect(ser_cable.dest_dataset_name).toBe(api_cable.dest_dataset_name);
                    expect(ser_cable.source_dataset_name).toBe(api_cable.source_dataset_name);
                });
            });
        });

        it('should match original pipeline output', function() {
            var self = this,
                serialized = loadAndSerialize(
                    this.canvasState,
                    this.api_pipeline
                );

            // TODO: These (inputs) should really be sorted by dataset_idx
            $.each(serialized.outcables, function(index, ser_output){
                var api_outcable = self.api_pipeline.outcables[index];
                var api_output = self.api_pipeline.outputs[index];

                expect(ser_output.output_cdt).toBe(api_outcable.output_cdt);
                expect(ser_output.output_idx).toBe(api_outcable.output_idx);
                expect(ser_output.output_name).toBe(api_outcable.output_name);
                expect(ser_output.source_dataset_name).toBe(api_outcable.source_dataset_name);
                expect(ser_output.source_step).toBe(api_outcable.source_step);
                expect(ser_output.x).toBeCloseTo(api_output.x, 8);
                expect(ser_output.y).toBeCloseTo(api_output.y, 8);
            });
        });

        it('should submit code resource update', function() {
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline),
                method = this.canvasState.findNodeByLabel("prelim_map.py"),
                new_code_resource_revision_id = 72;
            method.new_code_resource_revision = {
                id: new_code_resource_revision_id,
                name: "French"
            };
            pipeline.draw();
            var data = serializePipeline(this.canvasState),
                step = data.steps[0];

            expect(step.new_code_resource_revision_id).toBe(
                new_code_resource_revision_id,
                "step.new_code_resource_revision_id");
        });

        it('should submit dependency update', function() {
            var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline),
                method = this.canvasState.findNodeByLabel("prelim_map.py"),
                new_code_resource_revision_id = 72;
            method.new_dependencies = [{
                id: new_code_resource_revision_id,
                name: "French"
            }];
            pipeline.draw();
            var data = serializePipeline(this.canvasState),
                step = data.steps[0];

            expect(step.new_dependency_ids).toEqual(
                [new_code_resource_revision_id],
                "step.new_dependency_ids");
        });
    });
});