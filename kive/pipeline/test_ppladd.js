// TODO: Merge this with tests.js, once activity has stabilized

(function() {
    "use strict";
    
    describe("Pipeline functions", function() {
        beforeEach(function() {
            var width = 600,
                height = 300;

            jasmine.addMatchers(imagediff.jasmine);
            this.rawCanvas = imagediff.createCanvas(width, height);
            this.expectedRawCanvas = imagediff.createCanvas(width, height);
            this.canvas = new drydock_objects.CanvasWrapper(this.rawCanvas);
            this.canvasState = new CanvasState(this.rawCanvas);
            this.expectedCanvas = new drydock_objects.CanvasWrapper(this.expectedRawCanvas);
            this.ctx = this.canvas.ctx;
            this.expectedCanvas.ctx.fillStyle = "white";
            this.rgb_tolerance = 16; // max 255
            this.example_pipeline = {"revision_name": "", "groups_allowed": [], "pipeline_outputs": [{"source_dataset_name": "unmapped2_fastq", "output_CDT_pk": null, "x": 0.637772562280456, "y": 0.633208895290869, "output_idx": 1, "output_name": "unmapped2_fastq", "id": 128, "wires": [], "source_step": 2}, {"source_dataset_name": "unmapped1_fastq", "output_CDT_pk": null, "x": 0.637772562280456, "y": 0.633208895290869, "output_idx": 2, "output_name": "unmapped1_fastq", "id": 129, "wires": [], "source_step": 2}, {"source_dataset_name": "remap_conseq", "output_CDT_pk": 10, "x": 0.637772562280456, "y": 0.633208895290869, "output_idx": 3, "output_name": "remap_conseq", "id": 130, "wires": [], "source_step": 2}, {"source_dataset_name": "remap", "output_CDT_pk": 8, "x": 0.637772562280456, "y": 0.633208895290869, "output_idx": 4, "output_name": "remap", "id": 131, "wires": [], "source_step": 2}, {"source_dataset_name": "remap_counts", "output_CDT_pk": 9, "x": 0.637772562280456, "y": 0.633208895290869, "output_idx": 5, "output_name": "remap_counts", "id": 132, "wires": [], "source_step": 2}], "is_published_version": false, "user": 1, "revision_number": 3, "pipeline_steps": [{"inputs": {"1": {"cdt_label": "raw", "datasetname": "fastq1", "cdt_pk": null}, "2": {"cdt_label": "raw", "datasetname": "fastq2", "cdt_pk": null}}, "family_pk": 3, "transf_type": "Method", "cables_in": [{"dest_dataset_name": "fastq1", "keep_output": false, "source_dataset_name": "input1", "wires": [], "source_step": 0}, {"dest_dataset_name": "fastq2", "keep_output": false, "source_dataset_name": "input2", "wires": [], "source_step": 0}], "transf_pk": 4, "outputs_to_delete": [], "outputs": {"1": {"cdt_label": "(qname: string, flag: integer, rname: string, pos: integer, mapq: integer, cigar: string, rnext: string, pnext: integer, tlen: integer, seq: nucleotide sequence, qual: string)", "datasetname": "prelim", "cdt_pk": 7}}, "y": 0.224236051141488, "x": 0.344662650584514, "step_num": 1, "name": "prelim_map.py"}, {"inputs": {"1": {"cdt_label": "raw", "datasetname": "fastq1", "cdt_pk": null}, "2": {"cdt_label": "raw", "datasetname": "fastq2", "cdt_pk": null}, "3": {"cdt_label": "(qname: string, flag: integer, rname: string, pos: integer, mapq: integer, cigar: string, rnext: string, pnext: integer, tlen: integer, seq: nucleotide sequence, qual: string)", "datasetname": "prelim", "cdt_pk": 7}}, "family_pk": 4, "transf_type": "Method", "cables_in": [{"dest_dataset_name": "fastq1", "keep_output": false, "source_dataset_name": "input1", "wires": [], "source_step": 0}, {"dest_dataset_name": "fastq2", "keep_output": false, "source_dataset_name": "input2", "wires": [], "source_step": 0}, {"dest_dataset_name": "prelim", "keep_output": false, "source_dataset_name": "prelim", "wires": [], "source_step": 1}], "transf_pk": 5, "outputs_to_delete": [], "outputs": {"1": {"cdt_label": "(sample_name: string, qname: string, flag: integer, rname: string, pos: integer, mapq: integer, cigar: string, rnext: string, pnext: integer, tlen: integer, seq: nucleotide sequence, qual: string)", "datasetname": "remap", "cdt_pk": 8}, "2": {"cdt_label": "(sample_name: string, type: string, count: integer)", "datasetname": "remap_counts", "cdt_pk": 9}, "3": {"cdt_label": "(region: string, sequence: nucleotide sequence)", "datasetname": "remap_conseq", "cdt_pk": 10}, "4": {"cdt_label": "raw", "datasetname": "unmapped1_fastq", "cdt_pk": null}, "5": {"cdt_label": "raw", "datasetname": "unmapped2_fastq", "cdt_pk": null}}, "y": 0.257130788000083, "x": 0.450583501602465, "step_num": 2, "name": "remap.py"}], "pipeline_inputs": [{"dataset_idx": 1, "max_row": null, "CDT_pk": null, "y": 0.59464609800363, "x": 0.176757161239191, "min_row": null, "dataset_name": "input2"}, {"dataset_idx": 2, "max_row": null, "CDT_pk": null, "y": 0.695877476046049, "x": 0.197340202198511, "min_row": null, "dataset_name": "input1"}], "revision_parent_pk": 24, "family_name": "asdfasdf", "family_desc": "asdfasdf", "family_pk": 3, "revision_desc": "", "users_allowed": []};
            this.api_pipeline = {"id":25,"url":"http://127.0.0.1:8000/api/pipelines/25/","family":"asdfasdf","revision_name":"","revision_desc":"","revision_number":3,"revision_parent":24,"revision_DateTime":"2015-06-10T19:36:31.570191Z","user":"kive","users_allowed":[],"groups_allowed":[],"inputs":[{"dataset_name":"input2","dataset_idx":1,"x":0.176757161239191,"y":0.59464609800363,"structure":null},{"dataset_name":"input1","dataset_idx":2,"x":0.197340202198511,"y":0.695877476046049,"structure":null}],"outputs":[{"dataset_name":"unmapped2_fastq","dataset_idx":1,"x":0.637772562280456,"y":0.633208895290869,"structure":null},{"dataset_name":"unmapped1_fastq","dataset_idx":2,"x":0.637772562280456,"y":0.633208895290869,"structure":null},{"dataset_name":"remap_conseq","dataset_idx":3,"x":0.637772562280456,"y":0.633208895290869,"structure":{"compounddatatype":10,"min_row":null,"max_row":null}},{"dataset_name":"remap","dataset_idx":4,"x":0.637772562280456,"y":0.633208895290869,"structure":{"compounddatatype":8,"min_row":null,"max_row":null}},{"dataset_name":"remap_counts","dataset_idx":5,"x":0.637772562280456,"y":0.633208895290869,"structure":{"compounddatatype":9,"min_row":null,"max_row":null}}],"steps":[{"transformation":4,"transformation_family":3,"step_num":1,"outputs_to_delete":[],"x":0.344662650584514,"y":0.224236051141488,"name":"prelim_map.py","cables_in":[{"source_step":0,"source":143,"source_dataset_name":"input1","dest":7,"dest_dataset_name":"fastq1","custom_wires":[],"keep_output":false},{"source_step":0,"source":142,"source_dataset_name":"input2","dest":8,"dest_dataset_name":"fastq2","custom_wires":[],"keep_output":false}],"outputs":[{"dataset_name":"prelim","dataset_idx":1,"x":0.0,"y":0.0,"structure":{"compounddatatype":7,"min_row":null,"max_row":null}}],"inputs":[{"dataset_name":"fastq1","dataset_idx":1,"x":0.0,"y":0.0,"structure":null},{"dataset_name":"fastq2","dataset_idx":2,"x":0.0,"y":0.0,"structure":null}]},{"transformation":5,"transformation_family":4,"step_num":2,"outputs_to_delete":[],"x":0.450583501602465,"y":0.257130788000083,"name":"remap.py","cables_in":[{"source_step":0,"source":143,"source_dataset_name":"input1","dest":10,"dest_dataset_name":"fastq1","custom_wires":[],"keep_output":false},{"source_step":0,"source":142,"source_dataset_name":"input2","dest":11,"dest_dataset_name":"fastq2","custom_wires":[],"keep_output":false},{"source_step":1,"source":9,"source_dataset_name":"prelim","dest":12,"dest_dataset_name":"prelim","custom_wires":[],"keep_output":false}],"outputs":[{"dataset_name":"remap","dataset_idx":1,"x":0.0,"y":0.0,"structure":{"compounddatatype":8,"min_row":null,"max_row":null}},{"dataset_name":"remap_counts","dataset_idx":2,"x":0.0,"y":0.0,"structure":{"compounddatatype":9,"min_row":null,"max_row":null}},{"dataset_name":"remap_conseq","dataset_idx":3,"x":0.0,"y":0.0,"structure":{"compounddatatype":10,"min_row":null,"max_row":null}},{"dataset_name":"unmapped1_fastq","dataset_idx":4,"x":0.0,"y":0.0,"structure":null},{"dataset_name":"unmapped2_fastq","dataset_idx":5,"x":0.0,"y":0.0,"structure":null}],"inputs":[{"dataset_name":"fastq1","dataset_idx":1,"x":0.0,"y":0.0,"structure":null},{"dataset_name":"fastq2","dataset_idx":2,"x":0.0,"y":0.0,"structure":null},{"dataset_name":"prelim","dataset_idx":3,"x":0.0,"y":0.0,"structure":{"compounddatatype":7,"min_row":null,"max_row":null}}]}],"outcables":[{"pk":128,"output_idx":1,"output_name":"unmapped2_fastq","output_cdt":null,"source_step":2,"source":55,"source_dataset_name":"unmapped2_fastq","custom_wires":[]},{"pk":129,"output_idx":2,"output_name":"unmapped1_fastq","output_cdt":null,"source_step":2,"source":54,"source_dataset_name":"unmapped1_fastq","custom_wires":[]},{"pk":130,"output_idx":3,"output_name":"remap_conseq","output_cdt":10,"source_step":2,"source":15,"source_dataset_name":"remap_conseq","custom_wires":[]},{"pk":131,"output_idx":4,"output_name":"remap","output_cdt":8,"source_step":2,"source":13,"source_dataset_name":"remap","custom_wires":[]},{"pk":132,"output_idx":5,"output_name":"remap_counts","output_cdt":9,"source_step":2,"source":14,"source_dataset_name":"remap_counts","custom_wires":[]}],"removal_plan":"http://127.0.0.1:8000/api/pipelines/25/removal_plan/"};

            // Throw some more functions into the CanvasState object
            // Dirty hack for now, but it works
            CanvasState.prototype.isConnectedTo = function(node1, node2) {
                var connections = this.connectors;
                for(var i = 0; i < connections.length; i++)
                    if ((connections[i].source.parent == node1 && connections[i].dest.parent == node2) ||
                        (connections[i].source.parent == node2 && connections[i].dest.parent == node1))
                        return true;
                return false;
            };

            CanvasState.prototype.findNodeByLabel = function(label) {
                var shapes = this.shapes;
                for(var i = 0; i < shapes.length; i++)
                    if (shapes[i].label != null && shapes[i].label == label)
                        return shapes[i];
                return null;
            };
        });

        afterEach(function() {
            expect('Suppress SPEC HAS NO EXPECTATIONS').toBeDefined();
//            expect(this.rawCanvas).toImageDiffEqual(
//                    this.expectedRawCanvas,
//                    this.rgb_tolerance);
        });

        function loadPipeline(canvasState, pipeline){
            canvasState.reset();
            draw_pipeline(canvasState, pipeline);
            canvasState.testExecutionOrder();

            for (var i = 0; i < canvasState.shapes.length; i++) {
                canvasState.detectCollisions(canvasState.shapes[i], 0.5);
            }
            canvasState.draw();
        };

        describe('Load', function(){
            it('should load pipeline', function() {
                loadPipeline(this.canvasState, this.example_pipeline);
            });

            it('should find pipeline nodes', function() {
                loadPipeline(this.canvasState, this.example_pipeline);

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

                expect(input1).not.toBe(null);
                expect(input2).not.toBe(null);
                expect(prelim).not.toBe(null);
                expect(remap).not.toBe(null);
                expect(remapc).not.toBe(null);
                expect(remapp).not.toBe(null);
                expect(umfasq1).not.toBe(null);
                expect(umfasq2).not.toBe(null);
                expect(jmguire).toBe(null);
            });
        });

        describe('Connections', function(){
            it('should connect inputs to methods', function(){
                loadPipeline(this.canvasState, this.example_pipeline);

                var input1 = this.canvasState.findNodeByLabel('input1'),
                    input2 = this.canvasState.findNodeByLabel('input2'),
                    prelim = this.canvasState.findNodeByLabel('prelim_map.py'),
                    remap  = this.canvasState.findNodeByLabel('remap.py');

                expect(this.canvasState.isConnectedTo(input1, input2)).toBe(false);
                expect(this.canvasState.isConnectedTo(input1, prelim)).toBe(true);
                expect(this.canvasState.isConnectedTo(input2, prelim)).toBe(true);
                expect(this.canvasState.isConnectedTo(input1, remap)).toBe(true);
                expect(this.canvasState.isConnectedTo(input2, remap)).toBe(true);
            });

            it('should connect methods', function(){
                loadPipeline(this.canvasState, this.example_pipeline);

                var prelim = this.canvasState.findNodeByLabel('prelim_map.py'),
                    remap  = this.canvasState.findNodeByLabel('remap.py');

                expect(this.canvasState.isConnectedTo(prelim, remap)).toBe(true);
            });

            it('should connect methods to outputs', function(){
                loadPipeline(this.canvasState, this.example_pipeline);

                var remap   = this.canvasState.findNodeByLabel('remap.py'),
                    remapc  = this.canvasState.findNodeByLabel('remap_counts'),
                    remapcs = this.canvasState.findNodeByLabel('remap_conseq'),
                    remapp  = this.canvasState.findNodeByLabel('remap'),
                    umfasq1 = this.canvasState.findNodeByLabel('unmapped2_fastq'),
                    umfasq2 = this.canvasState.findNodeByLabel('unmapped1_fastq');

                expect(this.canvasState.isConnectedTo(remapc, remap)).toBe(true);
                expect(this.canvasState.isConnectedTo(remapcs, remap)).toBe(true);
                expect(this.canvasState.isConnectedTo(remapp, remap)).toBe(true);
                expect(this.canvasState.isConnectedTo(umfasq1, remap)).toBe(true);
                expect(this.canvasState.isConnectedTo(umfasq2, remap)).toBe(true);
                expect(this.canvasState.isConnectedTo(remapc, umfasq2)).toBe(false);
            });
        });
    });
})();
