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
            this.canvasState = new drydock.CanvasState(this.rawCanvas);
            this.expectedCanvas = new drydock_objects.CanvasWrapper(this.expectedRawCanvas);
            this.ctx = this.canvas.ctx;
            this.expectedCanvas.ctx.fillStyle = "white";
            this.rgb_tolerance = 16; // max 255

            this.api_pipeline = {"id":25,"url":"http://127.0.0.1:8000/api/pipelines/25/", "family_pk": 2, "family":"Test","revision_name":"","revision_desc":"Carl Sagan's science slam-jam","revision_number":3,"revision_parent":24,"revision_DateTime":"2015-06-10T19:36:31.570191Z","user":"kive","users_allowed":[],"groups_allowed":[],"inputs":[{"dataset_name":"input2","dataset_idx":1,"x":0.176757161239191,"y":0.59464609800363,"structure":{'compounddatatype':9, min_row:null, max_row:null }},{"dataset_name":"input1","dataset_idx":2,"x":0.197340202198511,"y":0.695877476046049,"structure":null}],"outputs":[{"dataset_name":"unmapped2_fastq","dataset_idx":1,"x":0.637772562280456,"y":0.633208895290869,"structure":null},{"dataset_name":"unmapped1_fastq","dataset_idx":2,"x":0.637772562280456,"y":0.633208895290869,"structure":null},{"dataset_name":"remap_conseq","dataset_idx":3,"x":0.637772562280456,"y":0.633208895290869,"structure":{"compounddatatype":10,"min_row":null,"max_row":null}},{"dataset_name":"remap","dataset_idx":4,"x":0.637772562280456,"y":0.633208895290869,"structure":{"compounddatatype":8,"min_row":null,"max_row":null}},{"dataset_name":"remap_counts","dataset_idx":5,"x":0.637772562280456,"y":0.633208895290869,"structure":{"compounddatatype":9,"min_row":null,"max_row":null}}],"steps":[{"transformation":4,"transformation_family":3,"step_num":1,"outputs_to_delete":[],"x":0.344662650584514,"y":0.224236051141488,"name":"prelim_map.py","cables_in":[{"source_step":0,"source":143,"source_dataset_name":"input1","dest":7,"dest_dataset_name":"fastq1","custom_wires":[],"keep_output":false},{"source_step":0,"source":142,"source_dataset_name":"input2","dest":8,"dest_dataset_name":"fastq2","custom_wires":[],"keep_output":false}],"outputs":[{"dataset_name":"prelim","dataset_idx":1,"x":0.0,"y":0.0,"structure":{"compounddatatype":7,"min_row":null,"max_row":null}}],"inputs":[{"dataset_name":"fastq1","dataset_idx":1,"x":0.0,"y":0.0,"structure":null},{"dataset_name":"fastq2","dataset_idx":2,"x":0.0,"y":0.0,"structure":null}]},{"transformation":5,"transformation_family":4,"step_num":2,"outputs_to_delete":[],"x":0.450583501602465,"y":0.257130788000083,"name":"remap.py","cables_in":[{"source_step":0,"source":143,"source_dataset_name":"input1","dest":10,"dest_dataset_name":"fastq1","custom_wires":[],"keep_output":false},{"source_step":0,"source":142,"source_dataset_name":"input2","dest":11,"dest_dataset_name":"fastq2","custom_wires":[],"keep_output":false},{"source_step":1,"source":9,"source_dataset_name":"prelim","dest":12,"dest_dataset_name":"prelim","custom_wires":[],"keep_output":false}],"outputs":[{"dataset_name":"remap","dataset_idx":1,"x":0.0,"y":0.0,"structure":{"compounddatatype":8,"min_row":null,"max_row":null}},{"dataset_name":"remap_counts","dataset_idx":2,"x":0.0,"y":0.0,"structure":{"compounddatatype":9,"min_row":null,"max_row":null}},{"dataset_name":"remap_conseq","dataset_idx":3,"x":0.0,"y":0.0,"structure":{"compounddatatype":10,"min_row":null,"max_row":null}},{"dataset_name":"unmapped1_fastq","dataset_idx":4,"x":0.0,"y":0.0,"structure":null},{"dataset_name":"unmapped2_fastq","dataset_idx":5,"x":0.0,"y":0.0,"structure":null}],"inputs":[{"dataset_name":"fastq1","dataset_idx":1,"x":0.0,"y":0.0,"structure":null},{"dataset_name":"fastq2","dataset_idx":2,"x":0.0,"y":0.0,"structure":null},{"dataset_name":"prelim","dataset_idx":3,"x":0.0,"y":0.0,"structure":{"compounddatatype":7,"min_row":null,"max_row":null}}]}],"outcables":[{"pk":128,"output_idx":1,"output_name":"unmapped2_fastq","output_cdt":null,"source_step":2,"source":55,"source_dataset_name":"unmapped2_fastq","custom_wires":[]},{"pk":129,"output_idx":2,"output_name":"unmapped1_fastq","output_cdt":null,"source_step":2,"source":54,"source_dataset_name":"unmapped1_fastq","custom_wires":[]},{"pk":130,"output_idx":3,"output_name":"remap_conseq","output_cdt":10,"source_step":2,"source":15,"source_dataset_name":"remap_conseq","custom_wires":[]},{"pk":131,"output_idx":4,"output_name":"remap","output_cdt":8,"source_step":2,"source":13,"source_dataset_name":"remap","custom_wires":[]},{"pk":132,"output_idx":5,"output_name":"remap_counts","output_cdt":9,"source_step":2,"source":14,"source_dataset_name":"remap_counts","custom_wires":[]}],"removal_plan":"http://127.0.0.1:8000/api/pipelines/25/removal_plan/"};

            // Throw some more functions into the CanvasState object
            // Dirty hack for now, but it works
            drydock.CanvasState.prototype.isConnectedTo = function(node1, node2) {
                var connections = this.connectors;
                for(var i = 0; i < connections.length; i++)
                    if ((connections[i].source.parent == node1 && connections[i].dest.parent == node2) ||
                        (connections[i].source.parent == node2 && connections[i].dest.parent == node1))
                        return true;
                return false;
            };

            drydock.CanvasState.prototype.findNodeByLabel = function(label) {
                var shapes = this.shapes;
                for(var i = 0; i < shapes.length; i++)
                    if (shapes[i].label != null && shapes[i].label == label)
                        return shapes[i];
                return null;
            };
        });

        afterEach(function() {
            expect('Suppress SPEC HAS NO EXPECTATIONS').toBeDefined();
        });

        function loadApiPipeline(canvasState, pipeline){
            var ppln = new Pipeline(canvasState);
            ppln.load(pipeline);
            return ppln;
        };

        describe('Load', function(){
            it('should load pipeline from API', function() {
                var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
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
                var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);

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


            it('should connect inputs to methods API', function(){
                var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);

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


            it('should connect methods API', function(){
                var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);

                var prelim = this.canvasState.findNodeByLabel('prelim_map.py'),
                    remap  = this.canvasState.findNodeByLabel('remap.py');

                expect(this.canvasState.isConnectedTo(prelim, remap)).toBe(true);
            });



            it('should connect methods to outputs API', function(){
                var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);

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

        describe('Structure', function(){

            it('shoud have correct properties for inputs API', function(){
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



            it('shoud have correct properties for output API', function(){
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

        describe('Serialize', function(){
            it('should serialize', function(){
                var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
                pipeline.draw();

                console.log(pipeline.serialize());

            });
        });
    });
})();
