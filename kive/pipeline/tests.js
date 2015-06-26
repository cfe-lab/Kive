(function() {
    "use strict";
    
    describe("Canvas classes", function() {
        beforeEach(function() {
            var width = 300,
                height = 150;
            jasmine.addMatchers(imagediff.jasmine);
            this.rawCanvas = imagediff.createCanvas(width, height);
            this.expectedRawCanvas = imagediff.createCanvas(width, height);
            this.canvas = new drydock_objects.CanvasWrapper(this.rawCanvas);
            this.expectedCanvas = new drydock_objects.CanvasWrapper(
                    this.expectedRawCanvas);
            this.ctx = this.canvas.ctx;
            this.expectedCanvas.ctx.fillStyle = "white";
            this.rgb_tolerance = 16; // max 255
            
            this.allowedGlobals = {};
            for (var key in window) {
                this.allowedGlobals[key] = true;
            }
        });
        
        afterEach(function() {
            for (var key in window) {
                if ( ! (key in this.allowedGlobals)) {
                    fail('leaked global ' + key);
                }
            }
            expect(this.rawCanvas).toImageDiffEqual(
                    this.expectedRawCanvas,
                    this.rgb_tolerance);
        });
        
        /**
         * Check a series of points to see whether they are included.
         * 
         * @param pointsToTest: an array of parameters to test - they come in
         *  groups of 4: x, y, isExpectedToContain, label
         * @param getTarget: a function that returns the object under test
         */
        function itContains(pointsToTest, getTarget) {
            var i;
            for (i=0; i < pointsToTest.length; i += 4) {
                itContainsPoint(
                        getTarget,
                        pointsToTest[i],
                        pointsToTest[i+1],
                        pointsToTest[i+2],
                        pointsToTest[i+3]);
            }
        }
        
        /**
         * Add a test scenario for a particular point.
         * 
         * @param target: the name of the attribute that holds the object under
         *  test
         * @param x: the x coordinate of the point to test
         * @param y: the y coordinate of the point to test
         * @param isExpectedToContain: true if the point to test should be
         *  contained in the object under test
         * @param label: a brief description of the point to test
         */
        function itContainsPoint(getTarget, x, y, isExpectedToContain, label) {
            var name = 'should ' + (isExpectedToContain ? '' : 'not ') +
                'contain ' + label;
            it(name, function() {
                var target = getTarget(this),
                    pad = 5, // ignored by all except connectors
                    isContained = target.contains(x, y, pad),
                    expectedFill = isExpectedToContain ? 'green' : 'red',
                    actualFill = isContained ? 'green': 'red';
                target.draw(this.expectedCanvas.ctx);
                this.expectedCanvas.ctx.fillStyle = expectedFill;
                this.expectedCanvas.drawCircle({x: x, y: y, r: 2});
                
                target.draw(this.canvas.ctx);
                this.canvas.ctx.fillStyle = actualFill;
                this.canvas.drawCircle({x: x, y: y, r: 2});
            });
        }

        /**
         * Draw the vertices of a drydock object.
         * 
         * This will draw the object and then stroke a path of its vertices on
         * top.
         * 
         * @param canvas: the canvas to draw on
         * @param target: the object under test
         * @param vertices: the array of vertex objects
         */
        function drawVertices(canvas, target, vertices) {
            var ctx = canvas.ctx;
            target.draw(ctx);
            ctx.fillStyle = "green";
            for (var i = 0; i < vertices.length; i++) {
                var vertex = vertices[i],
                    roundedX = Math.round(vertex.x),
                    roundedY = Math.round(vertex.y);
                canvas.drawCircle({x: roundedX, y: roundedY, r: 2});
            }
        }
        
        it('should allow raw calls to canvas context', function() {
            this.expectedCanvas.drawCircle({x: 100, y:10, r:5});
            
            this.ctx.beginPath();
            this.ctx.arc(100, 10, 5, 0, 2 * Math.PI);
            this.ctx.closePath();
            this.ctx.fillStyle = "white";
            this.ctx.fill();
        });
        
        describe("CanvasWrapper", function() {
            it("should draw a circle", function() {
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.arc(100, 10, 5, 0, 2 * Math.PI);
                this.expectedCanvas.ctx.closePath();
                this.expectedCanvas.ctx.fill();
                
                this.canvas.ctx.fillStyle = "white";
                this.canvas.drawCircle({x: 100, y: 10, r:5});
            });
            
            it("should draw a big circle", function() {
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.arc(50, 50, 25, 0, 2 * Math.PI);
                this.expectedCanvas.ctx.closePath();
                this.expectedCanvas.ctx.fill();
                
                this.canvas.ctx.fillStyle = "white";
                this.canvas.drawCircle({x: 50, y: 50, r:25});
            });
        });
        
        describe("Magnet", function() {
            beforeEach(function() {
                var r = 5,
                    parent = this,
                    attract = 3;
                this.magnet = new drydock_objects.Magnet(parent, r, attract);
                this.magnet.x = 100;
                this.magnet.y = 10;
                this.magnet.label = 'example';
            });
            
            it('should draw an unlit magnet', function() {
                this.expectedCanvas.drawCircle({x: 100, y: 10, r: 5});
                
                this.magnet.draw(this.ctx);
            });
            
            it('should draw a magnet with highlight', function() {
                this.expectedCanvas.drawText({x:90, y: 10, text: 'example'});
                this.expectedCanvas.drawCircle({x: 100, y: 10, r: 5});
                
                this.magnet.draw(this.ctx);
                this.magnet.highlight(this.ctx);
            });
            
            it('should highlight when marked as accepting connector', function() {
                this.expectedCanvas.drawText({x:90, y: 10, text: 'example'});
                this.expectedCanvas.drawCircle({x: 100, y: 10, r: 5});

                this.magnet.acceptingConnector = true;
                this.magnet.draw(this.ctx);
            });
            
            it('should offset the highlight', function() {
                this.expectedCanvas.drawText({x: 88, y: 10, text: 'example'});
                this.expectedCanvas.drawCircle({x: 100, y: 10, r: 5});
                
                expect(this.magnet.offset).toBe(5);
                this.magnet.offset += 2;
                this.magnet.acceptingConnector = true;
                this.magnet.draw(this.ctx);
            });
            
            it('should have custom fill colour', function() {
                this.expectedCanvas.drawText({x: 90, y: 10, text: 'example'});
                this.expectedCanvas.ctx.fillStyle = '#ff8';
                this.expectedCanvas.drawCircle({x: 100, y: 10, r:5});
                
                this.magnet.fill = '#ff8';
                this.magnet.acceptingConnector = true;
                this.magnet.draw(this.ctx);
            });
            
            it('should display output label on the right', function() {
                this.expectedCanvas.drawCircle({x: 100, y: 10, r: 5});
                this.expectedCanvas.ctx.translate(100, 10);
                this.expectedCanvas.ctx.rotate(Math.PI/6);
                this.expectedCanvas.drawText({
                    x: 10,
                    y: 0,
                    text: 'example',
                    dir: 1});

                this.magnet.isOutput = true;
                this.magnet.acceptingConnector = true;
                this.magnet.draw(this.ctx);
            });
            
            itContains([100, 10, true, 'centre',
                        108, 10, true, 'right edge',
                        109, 10, false, 'past right edge',
                        105, 16, true, 'diagonal edge',
                        105, 17, false, 'past diagonal edge'],
                       function(testCase) { return testCase.magnet; });
            itContains([109, 10, true, 'right edge with expanded attraction',
                        110, 10, false, 'past right edge with expanded attraction'],
                        function(testCase) {
                testCase.magnet.attract += 1;
                return testCase.magnet;
            });
        });
        
        describe("OutputNode", function() {
            beforeEach(function() {
                var x = 100,
                    y = 40,
                    label = 'example';
                this.node = new drydock_objects.OutputNode(x, y, label);
            });
            
            it('should draw', function() {
                this.expectedCanvas.ctx.fillStyle = "#d40";
                this.expectedCanvas.drawEllipse({x: 100, y: 52.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.fillRect(80, 27.5, 40, 25);
                this.expectedCanvas.drawEllipse({x: 100, y: 27.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.fillStyle = "white";
                this.expectedCanvas.ctx.globalAlpha = 0.35;
                this.expectedCanvas.drawEllipse({x: 100, y: 27.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.globalAlpha = 1;
                var magnet = new drydock_objects.Magnet();
                magnet.x = 88;
                magnet.y = 27.5;
                magnet.r = 5;
                magnet.draw(this.expectedCanvas.ctx);
                
                this.node.draw(this.ctx);
            });
            
            it('should draw with clear status', function() {
                this.expectedCanvas.ctx.strokeStyle = "green";
                this.expectedCanvas.ctx.lineWidth = 5;
                this.expectedCanvas.strokeEllipse({x: 100, y: 27.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.strokeRect(80, 27.5, 40, 25);
                this.expectedCanvas.strokeEllipse({x: 100, y: 52.5, rx: 20, ry: 10});
                
                this.expectedCanvas.ctx.fillStyle = "#d40";
                this.expectedCanvas.drawEllipse({x: 100, y: 52.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.fillRect(80, 27.5, 40, 25);
                this.expectedCanvas.drawEllipse({x: 100, y: 27.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.fillStyle = "white";
                this.expectedCanvas.ctx.globalAlpha = 0.35;
                this.expectedCanvas.drawEllipse({x: 100, y: 27.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.globalAlpha = 1;
                var magnet = new drydock_objects.Magnet();
                magnet.x = 88;
                magnet.y = 27.5;
                magnet.r = 5;
                magnet.draw(this.expectedCanvas.ctx);
                
                this.node.status = 'CLEAR';
                this.node.draw(this.ctx);
            });
            
            it('should draw as match for MD5', function() {
                this.expectedCanvas.ctx.fillStyle = "blue";
                this.expectedCanvas.drawEllipse({x: 100, y: 52.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.fillRect(80, 27.5, 40, 25);
                this.expectedCanvas.drawEllipse({x: 100, y: 27.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.fillStyle = "white";
                this.expectedCanvas.ctx.globalAlpha = 0.35;
                this.expectedCanvas.drawEllipse({x: 100, y: 27.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.globalAlpha = 1;
                var magnet = new drydock_objects.Magnet();
                magnet.x = 88;
                magnet.y = 27.5;
                magnet.r = 5;
                magnet.draw(this.expectedCanvas.ctx);
                
                this.node.found_md5 = true;
                this.node.draw(this.ctx);
            });
            
            itContains([100, 40, true, 'centre',
                        119, 40, true, 'right edge',
                        120, 40, false, 'past right edge',
                        81, 40, true, 'left edge',
                        80, 40, false, 'past left edge',
                        100, 18, true, 'top edge',
                        100, 17, false, 'past top edge'],
                       function(testCase) { return testCase.node; });
            
            it('should have vertices', function() {
                drawVertices(this.expectedCanvas,
                        this.node,
                        [{x: 100, y: 40},
                         {x: 120, y: 52.5},
                         {x: 80, y: 52.5},
                         {x: 120, y: 27.5},
                         {x: 80, y: 27.5},
                         {x: 100, y: 62.5},
                         {x: 100, y: 17.5}]);
                
                drawVertices(this.canvas, this.node, this.node.getVertices());
            });
            
            it('should highlight', function() {
                this.expectedCanvas.ctx.strokeStyle = "#7bf";
                this.expectedCanvas.ctx.lineWidth = 4;
                this.expectedCanvas.strokeEllipse({x: 100, y: 27.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.strokeRect(80, 27.5, 40, 25);
                this.expectedCanvas.strokeEllipse({x: 100, y: 52.5, rx: 20, ry: 10});
                this.node.draw(this.expectedCanvas.ctx);
                
                this.node.draw(this.ctx);
                this.ctx.strokeStyle = "#7bf";
                this.ctx.lineWidth = 4;
                this.node.highlight(this.ctx);
            });
            
            it('should highlight cable', function() {
                var sourceParent = {},
                    r = 5,
                    attract = 3,
                    source = new drydock_objects.Magnet(sourceParent, r, attract),
                    connector = new drydock_objects.Connector(source);
                source.x = 50;
                source.y = 10;
                source.label = "example";
                connector.dest = this.node.in_magnets[0];
                this.node.in_magnets[0].connected.push(connector);
                this.node.x = 200;
                this.expectedCanvas.ctx.strokeStyle = "#7bf";
                this.expectedCanvas.ctx.lineWidth = 4;
                this.expectedCanvas.strokeEllipse({x: 200, y: 27.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.strokeRect(180, 27.5, 40, 25);
                this.expectedCanvas.strokeEllipse({x: 200, y: 52.5, rx: 20, ry: 10});
                this.node.draw(this.expectedCanvas.ctx);
                connector.draw(this.expectedCanvas.ctx);
                this.expectedCanvas.ctx.strokeStyle = "#7bf";
                this.expectedCanvas.ctx.lineWidth = 4;
                this.expectedCanvas.ctx.font = '9pt Lato, sans-serif';
                this.expectedCanvas.ctx.textBaseline = 'middle';
                connector.highlight(this.expectedCanvas.ctx);
                
                this.node.draw(this.ctx);
                connector.draw(this.ctx);
                this.ctx.strokeStyle = "#7bf";
                this.ctx.lineWidth = 4;
                this.node.highlight(this.ctx);
            });
            
            it('should have label', function() {
                this.node.draw(this.expectedCanvas.ctx);
                this.expectedCanvas.drawText(
                        {x: 100, y: 9.5, text: 'example', dir: 0, style: 'node'});
                
                this.node.draw(this.canvas.ctx);
                var label = this.node.getLabel();
                this.canvas.drawText(
                        {x: label.x, y: label.y, text: label.label, dir:0, style: 'node'});
            });
        });
        
        describe("RawNode", function() {
            beforeEach(function() {
                var x = 100,
                    y = 40,
                    label = 'example';
                this.node = new drydock_objects.RawNode(x, y, label);
            });
            
            it('should draw', function() {
                this.expectedCanvas.ctx.fillStyle = "#8D8";
                this.expectedCanvas.drawEllipse({x: 100, y: 52.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.fillRect(80, 27.5, 40, 25);
                this.expectedCanvas.drawEllipse({x: 100, y: 27.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.fillStyle = "white";
                this.expectedCanvas.ctx.globalAlpha = 0.35;
                this.expectedCanvas.drawEllipse({x: 100, y: 27.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.globalAlpha = 1;
                var magnet = new drydock_objects.Magnet();
                magnet.x = 110;
                magnet.y = 45;
                magnet.r = 5;
                magnet.draw(this.expectedCanvas.ctx);
                
                this.node.draw(this.ctx);
            });
            
            it('should highlight', function() {
                this.expectedCanvas.ctx.strokeStyle = "#7bf";
                this.expectedCanvas.ctx.lineWidth = 4;
                this.expectedCanvas.strokeEllipse({x: 100, y: 27.5, rx: 20, ry: 10});
                this.expectedCanvas.ctx.strokeRect(80, 27.5, 40, 25);
                this.expectedCanvas.strokeEllipse({x: 100, y: 52.5, rx: 20, ry: 10});
                this.node.draw(this.expectedCanvas.ctx);
                
                this.node.draw(this.ctx);
                this.ctx.strokeStyle = "#7bf";
                this.ctx.lineWidth = 4;
                this.node.highlight(this.ctx);
            });
            
            itContains([100, 40, true, 'centre',
                        119, 40, true, 'right edge',
                        120, 40, false, 'past right edge',
                        81, 40, true, 'left edge',
                        80, 40, false, 'past left edge',
                        100, 18, true, 'top edge',
                        100, 17, false, 'past top edge'],
                       function(testCase) { return testCase.node; });
            
            it('should have vertices', function() {
                drawVertices(this.expectedCanvas,
                        this.node,
                        [{x: 100, y: 40},
                         {x: 120, y: 52.5},
                         {x: 80, y: 52.5},
                         {x: 120, y: 27.5},
                         {x: 80, y: 27.5},
                         {x: 100, y: 62.5},
                         {x: 100, y: 17.5}]);
                
                drawVertices(this.canvas, this.node, this.node.getVertices());
            });
            
            it('should have label', function() {
                this.node.draw(this.expectedCanvas.ctx);
                this.expectedCanvas.drawText(
                        {x: 100, y: 9.5, text: 'example', dir: 0, style: 'node'});
                
                this.node.draw(this.canvas.ctx);
                var label = this.node.getLabel();
                this.canvas.drawText(
                        {x: label.x, y: label.y, text: label.label, dir:0, style: 'node'});
            });
        });
        
        describe("CdtNode", function() {
            beforeEach(function() {
                var x = 100,
                    y = 40,
                    label = 'example';
                this.expected_cdt_pk = 1234;
                this.node = new drydock_objects.CdtNode(
                        this.expected_cdt_pk,
                        x,
                        y,
                        label);
            });
            
            it("should record compound data type's key", function() {
                expect(this.node.pk).toBe(this.expected_cdt_pk);
            });
            
            it('should draw', function() {
                this.expectedCanvas.ctx.fillStyle = "#88D";
                // draw base
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(77.5, 54);
                this.expectedCanvas.ctx.lineTo(100, 65.25);
                this.expectedCanvas.ctx.lineTo(122.5, 54);
                this.expectedCanvas.ctx.lineTo(122.5, 26);
                this.expectedCanvas.ctx.lineTo(77.5, 26);
                this.expectedCanvas.ctx.closePath();
                this.expectedCanvas.ctx.fill();
                // draw top
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(77.5, 26);
                this.expectedCanvas.ctx.lineTo(100, 37.25);
                this.expectedCanvas.ctx.lineTo(122.5, 26);
                this.expectedCanvas.ctx.lineTo(100, 14.75);
                this.expectedCanvas.ctx.closePath();
                this.expectedCanvas.ctx.fill();
                // some shading
                this.expectedCanvas.ctx.fillStyle = 'white';
                this.expectedCanvas.ctx.globalAlpha = 0.35;
                this.expectedCanvas.ctx.fill();
                this.expectedCanvas.ctx.globalAlpha = 1.0;
                var magnet = new drydock_objects.Magnet();
                magnet.x = 113;
                magnet.y = 45.625;
                magnet.r = 5;
                magnet.draw(this.expectedCanvas.ctx);
                
                this.node.draw(this.ctx);
            });

            it('should have vertices', function() {
                drawVertices(this.expectedCanvas,
                        this.node,
                        [{x: 100, y: 40},
                         {x: 77.5, y: 54},
                         {x: 100, y: 65.25},
                         {x: 122.5, y: 54},
                         {x: 122.5, y: 26},
                         {x: 100, y: 14.75},
                         {x: 77.5, y: 26}]);
                
                drawVertices(this.canvas, this.node, this.node.getVertices());
            });
            
            it('should highlight', function() {
                this.expectedCanvas.ctx.strokeStyle = "orange";
                this.expectedCanvas.ctx.lineWidth = 4;
                this.expectedCanvas.ctx.lineJoin = 'bevel';
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(77.5, 54);
                this.expectedCanvas.ctx.lineTo(100, 65.25);
                this.expectedCanvas.ctx.lineTo(122.5, 54);
                this.expectedCanvas.ctx.lineTo(122.5, 26);
                this.expectedCanvas.ctx.lineTo(100, 14.75);
                this.expectedCanvas.ctx.lineTo(77.5, 26);
                this.expectedCanvas.ctx.closePath();
                this.expectedCanvas.ctx.stroke();
                this.node.draw(this.expectedCanvas.ctx);
                
                this.node.draw(this.ctx);
                this.ctx.strokeStyle = "orange";
                this.ctx.lineWidth = 4;
                this.node.highlight(this.ctx);
            });
            
            itContains([100, 40, true, 'centre',
                        122, 40, true, 'right edge',
                        123, 40, false, 'past right edge',
                        110, 60, true, 'bottom-right edge',
                        110, 61, false, 'past bottom-right edge',
                        100, 65, true, 'bottom edge',
                        100, 66, false, 'past bottom edge',
                        78, 40, true, 'left edge',
                        77, 40, false, 'past left edge',
                        100, 15, true, 'top edge',
                        100, 14, false, 'past top edge'],
                       function(testCase) { return testCase.node; });
            
            it('should have label', function() {
                this.node.draw(this.expectedCanvas.ctx);
                this.expectedCanvas.drawText(
                        {x: 100, y: 11, text: 'example', dir: 0, style: 'node'});
                
                this.node.draw(this.canvas.ctx);
                var label = this.node.getLabel();
                this.canvas.drawText(
                        {x: label.x, y: label.y, text: label.label, dir:0, style: 'node'});
            });
        });
        
        describe("MethodNode", function() {
            beforeEach(function() {
                var method_pk = 37,
                    family_pk = 7,
                    x = 150,
                    y = 35,
                    fill = "#999",
                    label = "example",
                    inputs = [{dataset_idx: 1,dataset_name: "in", structure: {compounddatatype: 7} }],
                    outputs = [{dataset_idx: 1,dataset_name: "out",structure: {compounddatatype: 7}}];
                    
                this.node = new drydock_objects.MethodNode(
                        method_pk,
                        family_pk,
                        x,
                        y,
                        fill,
                        label,
                        inputs,
                        outputs);
            });
            
            function buildMethodBodyPath(ctx) {
                ctx.beginPath();
                ctx.moveTo(179.44486372867092, 74);
                ctx.lineTo(198.49742261192856, 63);
                ctx.lineTo(198.49742261192856, 41);
                ctx.bezierCurveTo(
                        169.05255888325766, 25.473720558371177,
                        169.05255888325766, 25.473720558371177,
                        169.05255888325766, 15);
                ctx.lineTo(150, 4);
                ctx.lineTo(130.94744111674234, 15);
                ctx.bezierCurveTo(
                        130.94744111674234, 44.52627944162882,
                        130.94744111674234, 44.52627944162882,
                        179.44486372867092, 74);
                ctx.closePath();
            }
            
            it('should draw', function() {
                this.expectedCanvas.ctx.save();
                this.expectedCanvas.ctx.fillStyle = "#999";
                
                // body
                buildMethodBodyPath(this.expectedCanvas.ctx);
                this.expectedCanvas.ctx.fill();
                
                // input plane (shading)
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(150, 26);
                this.expectedCanvas.ctx.lineTo(169.05255888325766, 15);
                this.expectedCanvas.ctx.lineTo(150, 4);
                this.expectedCanvas.ctx.lineTo(130.94744111674234, 15);
                this.expectedCanvas.ctx.fillStyle = '#fff';
                this.expectedCanvas.ctx.globalAlpha = 0.35;
                this.expectedCanvas.ctx.fill();
                
                // top bend (shading)
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(198.49742261192856, 41);
                this.expectedCanvas.ctx.lineTo(179.44486372867092, 52);
                this.expectedCanvas.ctx.bezierCurveTo(
                        150, 36.47372055837118,
                        150, 36.47372055837118,
                        150, 26);
                this.expectedCanvas.ctx.lineTo(169.05255888325766, 15);
                this.expectedCanvas.ctx.bezierCurveTo(
                        169.05255888325766, 25.473720558371177,
                        169.05255888325766, 25.473720558371177,
                        198.49742261192856, 41);
                this.expectedCanvas.ctx.globalAlpha = 0.12;
                this.expectedCanvas.ctx.fill();
                this.expectedCanvas.ctx.restore();
                
                // in magnet
                var magnet = new drydock_objects.Magnet();
                magnet.x = 150;
                magnet.y = 15;
                magnet.r = 5;
                magnet.draw(this.expectedCanvas.ctx);
                
                // out magnet
                magnet.x = 188.97114317029974;
                magnet.y = 57.5;
                magnet.draw(this.expectedCanvas.ctx);
                
                this.node.draw(this.ctx);
            });
            
            it('should draw with status', function() {
                this.expectedCanvas.ctx.strokeStyle = "green";
                this.expectedCanvas.ctx.lineWidth = 5;
                buildMethodBodyPath(this.expectedCanvas.ctx);
                this.expectedCanvas.ctx.stroke();
                this.node.draw(this.expectedCanvas.ctx);
                
                this.node.status = "CLEAR";
                this.node.draw(this.ctx);
            });
            
            it('should highlight', function() {
                this.expectedCanvas.ctx.strokeStyle = "#7bf";
                this.expectedCanvas.ctx.lineWidth = 4;
                buildMethodBodyPath(this.expectedCanvas.ctx);
                this.expectedCanvas.ctx.stroke();
                this.node.draw(this.expectedCanvas.ctx);
                // in magnet
                var magnet = new drydock_objects.Magnet();
                magnet.x = 150;
                magnet.y = 15;
                magnet.r = 5;
                magnet.label = "in";
                magnet.highlight(this.expectedCanvas.ctx);
                
                // out magnet
                magnet.x = 188.97114317029974;
                magnet.y = 57.5;
                magnet.label = "out";
                magnet.isOutput = true;
                magnet.highlight(this.expectedCanvas.ctx);
                
                this.ctx.strokeStyle = "#7bf";
                this.ctx.lineWidth = 4;
                this.node.draw(this.ctx);
                this.node.highlight(this.ctx);
            });
            
            it('should highlight with input cable', function() {
                this.expectedCanvas.ctx.strokeStyle = "#7bf";
                this.expectedCanvas.ctx.lineWidth = 4;
                buildMethodBodyPath(this.expectedCanvas.ctx);
                this.expectedCanvas.ctx.stroke();
                this.node.draw(this.expectedCanvas.ctx);
                // source magnet
                var sourceParent = {label: "in"},
                    source = new drydock_objects.Magnet(sourceParent);
                source.x = 50;
                source.y = 10;
                source.label = "in";
                
                // in magnet
                var in_magnet = new drydock_objects.Magnet(),
                    expectedCable = new drydock_objects.Connector(source);
                in_magnet.x = 150;
                in_magnet.y = 15;
                in_magnet.r = 5;
                in_magnet.label = "in";
                expectedCable.dest = in_magnet;
                in_magnet.connected.push(expectedCable);
                expectedCable.draw(this.expectedCanvas.ctx);
                expectedCable.drawLabel(this.expectedCanvas.ctx);
                
                // out magnet
                var out_magnet = new drydock_objects.Magnet();
                out_magnet.x = 188.97114317029974;
                out_magnet.y = 57.5;
                out_magnet.r = 5;
                out_magnet.label = "out";
                out_magnet.isOutput = true;
                out_magnet.highlight(this.expectedCanvas.ctx);
                
                var actualCable = new drydock_objects.Connector(source);
                actualCable.dest = this.node.in_magnets[0];
                this.node.in_magnets[0].connected.push(actualCable);
                this.node.draw(this.ctx);
                actualCable.draw(this.ctx);
                this.ctx.strokeStyle = "#7bf";
                this.ctx.lineWidth = 4;
                this.node.highlight(this.ctx);
            });
            
            it('should highlight with output cable', function() {
                this.expectedCanvas.ctx.strokeStyle = "#7bf";
                this.expectedCanvas.ctx.lineWidth = 4;
                buildMethodBodyPath(this.expectedCanvas.ctx);
                this.expectedCanvas.ctx.stroke();
                this.node.draw(this.expectedCanvas.ctx);
                // dest magnet
                var destParent = {label: "out"},
                dest = new drydock_objects.Magnet(destParent);
                dest.x = 250;
                dest.y = 75;
                dest.label = "out";
                
                // in magnet
                var in_magnet = new drydock_objects.Magnet();
                in_magnet.x = 150;
                in_magnet.y = 15;
                in_magnet.r = 5;
                in_magnet.label = "in";
                in_magnet.highlight(this.expectedCanvas.ctx);
                
                // out magnet
                var out_magnet = new drydock_objects.Magnet(this.node),
                    expectedCable = new drydock_objects.Connector(out_magnet);
                out_magnet.x = 188.97114317029974;
                out_magnet.y = 57.5;
                out_magnet.r = 5;
                out_magnet.label = "out";
                out_magnet.isOutput = true;
                expectedCable.dest = dest;
                out_magnet.connected.push(expectedCable);
                expectedCable.draw(this.expectedCanvas.ctx);
                expectedCable.drawLabel(this.expectedCanvas.ctx);
                
                var actualCable = new drydock_objects.Connector(
                        this.node.out_magnets[0]);
                actualCable.dest = dest;
                this.node.out_magnets[0].connected.push(actualCable);
                this.node.draw(this.ctx);
                actualCable.draw(this.ctx);
                this.ctx.strokeStyle = "#7bf";
                this.ctx.lineWidth = 4;
                this.node.highlight(this.ctx);
            });
            
            it('should highlight with output cable to output node', function() {
                this.expectedCanvas.ctx.strokeStyle = "#7bf";
                this.expectedCanvas.ctx.lineWidth = 4;
                buildMethodBodyPath(this.expectedCanvas.ctx);
                this.expectedCanvas.ctx.stroke();
                this.node.draw(this.expectedCanvas.ctx);
                // dest magnet
                var expectedOutput = new drydock_objects.OutputNode(250, 75, "out");
                
                // in magnet
                var in_magnet = new drydock_objects.Magnet();
                in_magnet.x = 150;
                in_magnet.y = 15;
                in_magnet.r = 5;
                in_magnet.label = "in";
                in_magnet.highlight(this.expectedCanvas.ctx);
                
                // out magnet
                var out_magnet = new drydock_objects.Magnet(this.node),
                expectedCable = new drydock_objects.Connector(out_magnet);
                out_magnet.x = 188.97114317029974;
                out_magnet.y = 57.5;
                out_magnet.r = 5;
                out_magnet.label = "out";
                out_magnet.isOutput = true;
                expectedCable.dest = expectedOutput.in_magnets[0];
                out_magnet.connected.push(expectedCable);
                expectedOutput.draw(this.expectedCanvas.ctx);
                expectedOutput.highlight(this.expectedCanvas.ctx);
                expectedCable.draw(this.expectedCanvas.ctx);
                expectedCable.drawLabel(this.expectedCanvas.ctx);
                
                var actualCable = new drydock_objects.Connector(
                    this.node.out_magnets[0]),
                    actualOutput = new drydock_objects.OutputNode(250, 75, "out");
                actualCable.dest = actualOutput.in_magnets[0];
                this.node.out_magnets[0].connected.push(actualCable);
                this.node.draw(this.ctx);
                actualOutput.draw(this.ctx);
                actualCable.draw(this.ctx);
                this.ctx.strokeStyle = "#7bf";
                this.ctx.lineWidth = 4;
                this.node.highlight(this.ctx);
            });
            
            itContains([150, 35, true, 'centre',
                        150, 4, true, 'top',
                        150, 3, false, 'beyond top',
                        131, 15, true, 'left',
                        130, 15, false, 'beyond left',
                        135, 46, true, 'bottom left',
                        135, 47, false, 'beyond bottom left',
                        179, 73, true, 'bottom',
                        179, 74, false, 'beyond bottom',
                        198, 63, true, 'bottom right',
                        199, 63, false, 'beyond bottom right',
                        198, 41, true, 'right',
                        198, 40, false, 'beyond right',
                        175, 29, true, 'top right',
                        175, 28, false, 'beyond top right'],
                       function(testCase) { return testCase.node; });

            it('should have vertices', function() {
                drawVertices(this.expectedCanvas,
                        this.node,
                        [{x: 150, y: 26},
                         {x: 169, y: 15},
                         {x: 150, y: 4},
                         {x: 131, y: 15},
                         {x: 179, y: 74},
                         {x: 198, y: 63},
                         {x: 198, y: 41},
                         {x: 179, y: 52},
                         {x: 131, y: 45},
                         {x: 150, y: 36},
                         {x: 169, y: 25}]);
                
                drawVertices(this.canvas, this.node, this.node.getVertices());
            });
            
            it('should have label', function() {
                this.node.y += 20;
                this.node.draw(this.expectedCanvas.ctx);
                this.expectedCanvas.drawText(
                        {x: 161.25, y: 19.5, text: 'example', dir: 0, style: 'node'});
                
                this.node.draw(this.canvas.ctx);
                var label = this.node.getLabel();
                this.canvas.drawText(
                        {x: label.x, y: label.y, text: label.label, dir:0, style: 'node'});
            });
        });
        
        describe("Connector", function() {
            beforeEach(function() {
                var r = 5,
                    attract = 3;
                this.sourceParent = {};
                this.destParent = {};
                this.source = new drydock_objects.Magnet(this.sourceParent, r, attract);
                this.source.x = 50;
                this.source.y = 10;
                this.source.label = 'example';
                this.dest = new drydock_objects.Magnet(this.destParent, r, attract);
                this.dest.x = 150;
                this.dest.y = 15;
                this.dest.label = 'example';
                this.connector = new drydock_objects.Connector(this.source);
                this.connector.x = 150;
                this.connector.y = 15;
            });
            
            it('should draw', function() {
                this.expectedCanvas.ctx.fillStyle = "#aaa";
                this.expectedCanvas.drawText(
                        {x: 152, y: 20, text: "example", dir: 1, style: "connector"});
                this.expectedCanvas.ctx.strokeStyle = '#abc';
                this.expectedCanvas.ctx.lineWidth = 6;
                this.expectedCanvas.ctx.lineCap = 'round';
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(50, 10);
                this.expectedCanvas.ctx.bezierCurveTo(
                        110.6218,
                        45,
                        140,
                        -18.3333,
                        150,
                        15);
                this.expectedCanvas.ctx.stroke();
                
                this.connector.draw(this.ctx);
            });
            
            it('should draw with destination attached', function() {
                // Don't draw label
                this.expectedCanvas.ctx.strokeStyle = '#abc';
                this.expectedCanvas.ctx.lineWidth = 6;
                this.expectedCanvas.ctx.lineCap = 'round';
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(50, 10);
                this.expectedCanvas.ctx.bezierCurveTo(
                        110.6218,
                        45,
                        140,
                        -18.3333,
                        150,
                        15);
                this.expectedCanvas.ctx.stroke();
                
                this.connector.dest = this.dest;
                this.connector.draw(this.ctx);
            });
            
            it('should highlight', function() {
                this.expectedCanvas.ctx.fillStyle = "#aaa";
                this.expectedCanvas.drawText(
                        {x: 152, y: 20, text: "example", dir: 1, style: "connector"});
                this.expectedCanvas.ctx.strokeStyle = '#abc';
                this.expectedCanvas.ctx.lineWidth = 6;
                this.expectedCanvas.ctx.lineCap = 'round';
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(50, 10);
                this.expectedCanvas.ctx.bezierCurveTo(
                        110.6218,
                        45,
                        140,
                        -18.3333,
                        150,
                        15);
                this.expectedCanvas.ctx.stroke();
                // stroke again in blue
                this.expectedCanvas.ctx.strokeStyle = 'blue';
                this.expectedCanvas.ctx.stroke();
                
                this.connector.draw(this.ctx);
                this.ctx.strokeStyle = 'blue';
                this.connector.highlight(this.ctx);
            });
            
            it('should highlight with destination attached', function() {
                // Don't draw label
                this.expectedCanvas.ctx.strokeStyle = '#abc';
                this.expectedCanvas.ctx.lineWidth = 6;
                this.expectedCanvas.ctx.lineCap = 'round';
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(50, 10);
                this.expectedCanvas.ctx.bezierCurveTo(
                        110.6218,
                        45,
                        140,
                        -18.3333,
                        150,
                        15);
                this.expectedCanvas.ctx.stroke();
                // stroke again in blue
                this.expectedCanvas.ctx.strokeStyle = 'blue';
                this.expectedCanvas.ctx.stroke();
                // draw label
                this.expectedCanvas.ctx.fillStyle = "#aaa";
                this.expectedCanvas.ctx.translate(101.5519, 19.2424);
                this.expectedCanvas.ctx.rotate(-0.2262);
                this.expectedCanvas.drawText(
                        {x: 0, y: 0, text: "example", dir: 0, style: "connector"});
                
                this.connector.dest = this.dest;
                this.connector.draw(this.ctx);
                this.ctx.strokeStyle = 'blue';
                this.connector.highlight(this.ctx);
            });
            
            it('should highlight without room for label', function() {
                // Don't draw label
                this.expectedCanvas.ctx.strokeStyle = '#abc';
                this.expectedCanvas.ctx.lineWidth = 6;
                this.expectedCanvas.ctx.lineCap = 'round';
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(50, 10);
                this.expectedCanvas.ctx.bezierCurveTo(
                        80.31088913245534,
                        27.5,
                        95,
                        -18.3333,
                        100,
                        15);
                this.expectedCanvas.ctx.stroke();
                // stroke again in blue
                this.expectedCanvas.ctx.strokeStyle = 'blue';
                this.expectedCanvas.ctx.stroke();
                // don't draw label
                
                this.dest.x -= 50;
                this.connector.dest = this.dest;
                this.connector.draw(this.ctx);
                this.ctx.strokeStyle = 'blue';
                this.connector.highlight(this.ctx);
            });
            
            it('should draw label', function() {
                this.expectedCanvas.ctx.strokeStyle = '#abc';
                this.expectedCanvas.ctx.lineWidth = 6;
                this.expectedCanvas.ctx.lineCap = 'round';
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(50, 10);
                this.expectedCanvas.ctx.bezierCurveTo(
                        110.6218,
                        45,
                        140,
                        -18.3333,
                        150,
                        15);
                this.expectedCanvas.ctx.stroke();
                // draw label
                this.expectedCanvas.ctx.fillStyle = "#aaa";
                this.expectedCanvas.ctx.translate(101.5519, 19.2424);
                this.expectedCanvas.ctx.rotate(-0.2262);
                this.expectedCanvas.drawText(
                        {x: 0, y: 0, text: "example", dir: 0, style: "connector"});
                
                this.connector.dest = this.dest;
                this.connector.draw(this.ctx);
                this.ctx.strokeStyle = 'blue';
                this.connector.drawLabel(this.ctx);
            });
            
            it('should draw with clear status', function() {
                // Don't draw label, green stroke
                this.expectedCanvas.ctx.strokeStyle = 'green';
                this.expectedCanvas.ctx.lineWidth = 6;
                this.expectedCanvas.ctx.lineCap = 'round';
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(50, 10);
                this.expectedCanvas.ctx.bezierCurveTo(
                        110.6218,
                        45,
                        140,
                        -18.3333,
                        150,
                        15);
                this.expectedCanvas.ctx.stroke();
                
                this.sourceParent.status = "CLEAR";
                this.destParent.status = "*";
                this.connector.dest = this.dest;
                this.connector.draw(this.ctx);
            });
            
            it('should draw with failure status', function() {
                // Don't draw label, red stroke
                this.expectedCanvas.ctx.strokeStyle = 'red';
                this.expectedCanvas.ctx.lineWidth = 6;
                this.expectedCanvas.ctx.lineCap = 'round';
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(50, 10);
                this.expectedCanvas.ctx.bezierCurveTo(
                        110.6218,
                        45,
                        140,
                        -18.3333,
                        150,
                        15);
                this.expectedCanvas.ctx.stroke();
                
                this.sourceParent.status = "FAILURE";
                this.connector.dest = this.dest;
                this.connector.draw(this.ctx);
            });
            
            it('should draw with running status', function() {
                // Don't draw label, orange stroke
                this.expectedCanvas.ctx.strokeStyle = 'orange';
                this.expectedCanvas.ctx.lineWidth = 6;
                this.expectedCanvas.ctx.lineCap = 'round';
                this.expectedCanvas.ctx.beginPath();
                this.expectedCanvas.ctx.moveTo(50, 10);
                this.expectedCanvas.ctx.bezierCurveTo(
                        110.6218,
                        45,
                        140,
                        -18.3333,
                        150,
                        15);
                this.expectedCanvas.ctx.stroke();
                
                this.sourceParent.status = "*";
                this.connector.dest = this.dest;
                this.connector.draw(this.ctx);
            });
            
            itContains([101, 19, true, 'centre',
                        101, 15, true, 'upper centre',
                        101, 14, false, 'above centre',
                        101, 24, true, 'lower centre',
                        101, 25, false, 'below centre',
                        150, 15, true, 'dest',
                        154, 15, true, 'right dest',
                        155, 15, false, 'beyond right dest',
                        150, 19, true, 'lower dest',
                        150, 20, false, 'below dest'],
                       function(testCase) {
                testCase.connector.dest = testCase.dest;
                return testCase.connector;
            });
        });
        
        describe("OutputZone", function() {
            beforeEach(function() {
                var canvas_width = 600,
                    canvas_height = 150;
                this.expectedRawCanvas.width = canvas_width;
                this.rawCanvas.width = canvas_width;
                this.zone = new drydock_objects.OutputZone(
                        canvas_width,
                        canvas_height);
            });
            
            it('should draw', function() {
                this.expectedCanvas.ctx.strokeStyle = "#aaa";
                this.expectedCanvas.ctx.setLineDash([5]);
                this.expectedCanvas.ctx.lineWidth = 1;
                this.expectedCanvas.ctx.strokeRect(492, 1, 105, 105);
                this.expectedCanvas.drawText(
                        {x: 544.5, y: 16, text:"Drag here to", style:"outputZone", dir: 0});
                this.expectedCanvas.drawText(
                        {x: 544.5, y: 31, text:"create an output", style:"outputZone", dir: 0});
                
                this.zone.draw(this.ctx);
            });
            
            itContains([550, 50, true, 'centre',
                        550, 1, true, 'upper centre',
                        550, 0, false, 'above centre',
                        550, 106, true, 'lower centre',
                        550, 107, false, 'below centre',
                        597, 50, true, 'right',
                        598, 50, false, 'beyond right',
                        492, 50, true, 'left',
                        491, 50, false, 'beyond left'],
                       function(testCase) { return testCase.zone; });
        });
        
        describe("CanvasState", function() {
            beforeEach(function() {
                this.state = new drydock.CanvasState(this.rawCanvas);
                
                this.expectedInput = new drydock_objects.RawNode(30, 50, "in");
                this.actualInput = new drydock_objects.RawNode(30, 50, "in");
                this.state.addShape(this.actualInput);
            });
            
            it('should draw', function() {
                this.expectedInput.draw(this.expectedCanvas.ctx);
                this.expectedCanvas.drawText(
                        {x: 30, y: 19.5, text: "in", style: "node", dir: 0});
                
                this.state.draw(this.ctx);
            });
            
            it('should scale', function() {
                this.expectedCanvas.ctx.scale(0.5, 0.5);
                this.expectedInput.draw(this.expectedCanvas.ctx);
                this.expectedCanvas.drawText(
                        {x: 30, y: 19.5, text: "in", style: "node", dir: 0});
                
                this.state.setScale(0.5);
                this.state.draw(this.ctx);
            });
            
            it('should scale more than once', function() {
                this.expectedCanvas.ctx.scale(0.5, 0.5);
                this.expectedInput.draw(this.expectedCanvas.ctx);
                this.expectedCanvas.drawText(
                        {x: 30, y: 19.5, text: "in", style: "node", dir: 0});
                
                this.state.setScale(0.2);
                this.state.draw(this.ctx);
                
                this.state.setScale(0.5);
                this.state.draw(this.ctx);
            });
            
            it('should move', function() {
                this.expectedInput.x += 10;
                this.expectedInput.draw(this.expectedCanvas.ctx);
                this.expectedCanvas.ctx.strokeStyle = "#7bf";
                this.expectedCanvas.ctx.lineWidth = 4;
                this.expectedInput.highlight(this.expectedCanvas.ctx);
                this.expectedCanvas.drawText(
                        {x: 40, y: 19.5, text: "in", style: "node", dir: 0});
                
                this.state.doDown({pageX: 30, pageY: 50});
                this.state.doMove({pageX: 40, pageY: 50});
                this.state.draw(this.ctx);
            });
            
            describe("with input and method", function() {
                beforeEach(function() {
                    this.methodId = 27;
                    this.methodFamilyId = 13;
                    this.methodInputs = {1: {datasetname: "in"}};
                    this.methodOutputs = {1: {cdt_pk: 17, datasetname: "out"}};


                    this.methodInputs = [{dataset_idx: 1, dataset_name: "in", structure: null}];
                    this.methodOutputs = [{dataset_idx: 1, dataset_name: "out", structure: {compounddatatype: 17}}];

                    this.expectedMethod = new drydock_objects.MethodNode(
                            this.methodId,
                            this.methodFamilyId,
                            100,
                            50,
                            null,
                            "example",
                            this.methodInputs,
                            this.methodOutputs);
                    this.actualMethod = new drydock_objects.MethodNode(
                            this.methodId,
                            this.methodFamilyId,
                            100,
                            50,
                            null,
                            "example",
                            this.methodInputs,
                            this.methodOutputs);
                    this.expectedConnector = new drydock_objects.Connector(
                            this.expectedMethod.out_magnets[0]);
                    this.expectedOutputZone = new drydock_objects.OutputZone(
                            this.expectedRawCanvas.width,
                            this.expectedRawCanvas.height);
                    this.expectedOutput = new drydock_objects.OutputNode(
                            250,
                            76,
                            "out");
                    this.state.$dialog = $('<p>Dialog</p>');
                    this.state.addShape(this.actualMethod);
                });
                
                function drawStartingPipeline(testCase) {
                    testCase.expectedInput.draw(testCase.expectedCanvas.ctx);
                    testCase.expectedCanvas.drawText(
                            {x: 30, y: 19.5, text: "in", style: "node", dir: 0});
                    testCase.expectedMethod.draw(testCase.expectedCanvas.ctx);
                    testCase.expectedCanvas.drawText(
                            {x: 111.25, y: 14.5, text: "example", style: "node", dir: 0});
                    
                }
                
                it('should draw', function() {
                    drawStartingPipeline(this);
                    
                    this.state.draw(this.ctx);
                });
                
                it('should highlight clicked input', function() {
                    drawStartingPipeline(this);
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedInput.highlight(this.expectedCanvas.ctx);
                    
                    this.state.doDown({
                        pageX: this.expectedInput.x,
                        pageY: this.expectedInput.y
                    });
                    this.state.draw(this.ctx);
                });
                
                it('should highlight clicked method', function() {
                    drawStartingPipeline(this);
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedMethod.highlight(this.expectedCanvas.ctx);
                    
                    this.state.doDown({
                        pageX: this.expectedMethod.x,
                        pageY: this.expectedMethod.y
                    });
                    this.state.draw(this.ctx);
                });
                
                it('should remove highlight with shift click', function() {
                    drawStartingPipeline(this);
                    
                    this.state.doDown({
                        pageX: this.expectedMethod.x,
                        pageY: this.expectedMethod.y
                    });
                    this.state.doUp({
                        pageX: this.expectedMethod.x,
                        pageY: this.expectedMethod.y
                    });
                    this.state.doDown({
                        pageX: this.expectedMethod.x,
                        pageY: this.expectedMethod.y,
                        shiftKey: true
                    });
                    this.state.draw(this.ctx);
                });
                
                it('should remove highlight when clicking background', function() {
                    drawStartingPipeline(this);

                    // click method
                    this.state.doDown({
                        pageX: this.expectedMethod.x,
                        pageY: this.expectedMethod.y
                    });
                    this.state.doUp({
                        pageX: this.expectedMethod.x,
                        pageY: this.expectedMethod.y
                    });
                    // click background
                    this.state.doDown({ pageX: 250, pageY: 100 });
                    
                    this.state.draw(this.ctx);
                });
                
                it('should not remove highlight when shift clicking background', function() {
                    drawStartingPipeline(this);
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedMethod.highlight(this.expectedCanvas.ctx);
                     
                    // click method
                    this.state.doDown({
                        pageX: this.expectedMethod.x,
                        pageY: this.expectedMethod.y
                    });
                    this.state.doUp({
                        pageX: this.expectedMethod.x,
                        pageY: this.expectedMethod.y
                    });
                    // click background
                    this.state.doDown({ pageX: 250, pageY: 100, shiftKey: true });
                    
                    this.state.draw(this.ctx);
                });
                
                it('should drag input', function() {
                    this.expectedInput.y += 50;
                    this.expectedInput.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 30, y: 69.5, text: "in", style: "node", dir: 0});
                    this.expectedMethod.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 111.25, y: 14.5, text: "example", style: "node", dir: 0});
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedInput.highlight(this.expectedCanvas.ctx);
                    
                    this.state.draw(this.ctx);
                    this.state.doDown(
                            {pageX: this.actualInput.x, pageY: this.actualInput.y});
                    this.state.doMove(
                            {pageX: this.expectedInput.x, pageY: this.expectedInput.y});
                    this.state.draw(this.ctx);
                });
                
                it('should drag off edge', function() {
                    this.expectedInput.y += 110;
                    this.expectedInput.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 30, y: 129.5, text: "in", style: "node", dir: 0});
                    this.expectedMethod.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 111.25, y: 14.5, text: "example", style: "node", dir: 0});
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedInput.highlight(this.expectedCanvas.ctx);
                    
                    this.state.draw(this.ctx);
                    this.state.doDown(
                            {pageX: this.actualInput.x, pageY: this.actualInput.y});
                    this.state.doMove(
                            {pageX: this.expectedInput.x, pageY: this.expectedInput.y});
                    this.state.draw(this.ctx);
                });
                
                it('should drag two objects with shift click', function() {
                    this.expectedInput.y += 50;
                    this.expectedMethod.y += 50;
                    this.expectedInput.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 30, y: 69.5, text: "in", style: "node", dir: 0});
                    this.expectedMethod.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 111.25, y: 64.5, text: "example", style: "node", dir: 0});
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedInput.highlight(this.expectedCanvas.ctx);
                    this.expectedMethod.highlight(this.expectedCanvas.ctx);
                    
                    this.state.draw(this.ctx);
                    // click input
                    this.state.doDown(
                            {pageX: this.actualInput.x, pageY: this.actualInput.y});
                    this.state.doUp(
                            {pageX: this.actualInput.x, pageY: this.actualInput.y});
                    // shift click method and drag
                    this.state.doDown({
                        pageX: this.actualMethod.x,
                        pageY: this.actualMethod.y,
                        shiftKey: true
                    });
                    this.state.doMove({
                        pageX: this.expectedMethod.x,
                        pageY: this.expectedMethod.y
                    });
                    this.state.draw(this.ctx);
                });
                
                it('should drag two objects with click', function() {
                    this.expectedInput.y += 50;
                    this.expectedMethod.y += 50;
                    this.expectedInput.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 30, y: 69.5, text: "in", style: "node", dir: 0});
                    this.expectedMethod.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 111.25, y: 64.5, text: "example", style: "node", dir: 0});
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedInput.highlight(this.expectedCanvas.ctx);
                    this.expectedMethod.highlight(this.expectedCanvas.ctx);
                    
                    this.state.draw(this.ctx);
                    // click input
                    this.state.doDown(
                            {pageX: this.actualInput.x, pageY: this.actualInput.y});
                    this.state.doUp(
                            {pageX: this.actualInput.x, pageY: this.actualInput.y});
                    // shift click method
                    this.state.doDown({
                        pageX: this.actualMethod.x,
                        pageY: this.actualMethod.y,
                        shiftKey: true
                    });
                    this.state.doUp({
                        pageX: this.actualMethod.x,
                        pageY: this.actualMethod.y
                    });
                    // click method and drag
                    this.state.doDown({
                        pageX: this.actualMethod.x,
                        pageY: this.actualMethod.y
                    });
                    this.state.doMove({
                        pageX: this.expectedMethod.x,
                        pageY: this.expectedMethod.y
                    });
                    this.state.draw(this.ctx);
                });
                
                it('should drag two objects with shift click on magnet', function() {
                    this.expectedInput.y += 50;
                    this.expectedMethod.y += 50;
                    this.expectedInput.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 30, y: 69.5, text: "in", style: "node", dir: 0});
                    this.expectedMethod.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 111.25, y: 64.5, text: "example", style: "node", dir: 0});
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedInput.highlight(this.expectedCanvas.ctx);
                    this.expectedMethod.highlight(this.expectedCanvas.ctx);
                    
                    this.state.draw(this.ctx);
                    // click input
                    this.state.doDown(
                            {pageX: this.actualInput.x, pageY: this.actualInput.y});
                    this.state.doUp(
                            {pageX: this.actualInput.x, pageY: this.actualInput.y});
                    // shift click method and drag
                    this.state.doDown({
                        pageX: this.actualMethod.out_magnets[0].x,
                        pageY: this.actualMethod.out_magnets[0].y,
                        shiftKey: true
                    });
                    this.state.doMove({
                        pageX: this.expectedMethod.out_magnets[0].x,
                        pageY: this.expectedMethod.out_magnets[0].y
                    });
                    this.state.draw(this.ctx);
                });
                
                it('should drag output', function() {
                    drawStartingPipeline(this);
                    this.expectedOutputZone.draw(this.expectedCanvas.ctx);
                    this.expectedConnector.x = 250;
                    this.expectedConnector.y = 20;
                    this.expectedCanvas.ctx.globalAlpha = 0.75;
                    this.expectedConnector.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.ctx.globalAlpha = 1.0;
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedConnector.highlight(this.expectedCanvas.ctx);
                    var magnet = this.expectedMethod.out_magnets[0];
                    
                    this.state.draw(this.ctx);
                    this.state.doDown({pageX: magnet.x, pageY: magnet.y});
                    this.state.doMove({pageX: 250, pageY: 20});
                    this.state.draw(this.ctx);
                });
                
                it('should create output', function() {
                    drawStartingPipeline(this);
                    this.expectedOutput.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 250, y: 45.5, text: "out", style: "node", dir: 0});
                    // connector
                    this.expectedConnector.dest = this.expectedOutput.in_magnets[0];
                    this.expectedCanvas.ctx.globalAlpha = 0.75;
                    this.expectedConnector.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.ctx.globalAlpha = 1.0;
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedConnector.highlight(this.expectedCanvas.ctx);
                    var magnet = this.expectedMethod.out_magnets[0];
                    
                    this.state.draw(this.ctx);
                    this.state.doDown({pageX: magnet.x, pageY: magnet.y});
                    this.state.doMove({pageX: 250, pageY: 20});
                    this.state.doUp({pageX: 250, pageY: 20}); // in output zone
                    this.state.draw(this.ctx);
                });
                
                it('should create and delete output', function() {
                    drawStartingPipeline(this);
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedMethod.highlight(this.expectedCanvas.ctx);
                    var magnet = this.expectedMethod.out_magnets[0];
                    
                    this.state.draw(this.ctx);
                    // drag to create output
                    this.state.doDown({pageX: magnet.x, pageY: magnet.y});
                    this.state.doMove({pageX: 250, pageY: 20});
                    this.state.doUp({pageX: 250, pageY: 20}); // in output zone
                    this.state.draw(this.ctx);
                    // select output and delete it
                    this.state.doDown({
                        pageX: this.expectedOutput.x,
                        pageY: this.expectedOutput.y
                    });
                    this.state.doUp({
                        pageX: this.expectedOutput.x,
                        pageY: this.expectedOutput.y
                    });
                    this.state.deleteObject();
                    // select method
                    this.state.doDown({
                        pageX: this.expectedMethod.x,
                        pageY: this.expectedMethod.y
                    });
                    this.state.draw(this.ctx);
                });
                
                it('should create and move output', function() {
                    drawStartingPipeline(this);
                    this.expectedOutput.y -= 20;
                    this.expectedOutput.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 250, y: 25.5, text: "out", style: "node", dir: 0});
                    // connector
                    this.expectedConnector.dest = this.expectedOutput.in_magnets[0];
                    this.expectedCanvas.ctx.globalAlpha = 0.75;
                    this.expectedConnector.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.ctx.globalAlpha = 1.0;
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedOutput.highlight(this.expectedCanvas.ctx);
                    this.expectedConnector.highlight(this.expectedCanvas.ctx);
                    var magnet = this.expectedMethod.out_magnets[0];
                    
                    this.state.draw(this.ctx);
                    // drag to create output
                    this.state.doDown({pageX: magnet.x, pageY: magnet.y});
                    this.state.doMove({pageX: 250, pageY: 20});
                    this.state.doUp({pageX: 250, pageY: 20}); // in output zone
                    this.state.draw(this.ctx);
                    // select connector and move it
                    var startX = this.actualMethod.out_magnets[0].connected[0].x,
                        startY = this.actualMethod.out_magnets[0].connected[0].y;
                    this.state.doDown({ pageX: startX, pageY: startY });
                    this.state.doMove({ pageX: startX, pageY: startY-20 });
                    this.state.draw(this.ctx);
                });
                
                it('should not create connector when read-only', function() {
                    drawStartingPipeline(this);
                    var magnet = this.expectedMethod.out_magnets[0];

                    this.state.can_edit = false;
                    this.state.draw(this.ctx);
                    this.state.doDown({pageX: magnet.x, pageY: magnet.y});
                    expect(this.actualMethod.out_magnets[0].connected.length).toBe(0);
                });
                
                it('should start input connector', function() {
                    this.expectedMethod.in_magnets[0].acceptingConnector = true;
                    this.expectedMethod.in_magnets[0].fill = '#ff8';
                    drawStartingPipeline(this);
                    // connector
                    this.expectedConnector.source = this.expectedInput.out_magnets[0];
                    this.expectedConnector.x = 100;
                    this.expectedConnector.y = 100;
                    this.expectedCanvas.ctx.globalAlpha = 0.75;
                    this.expectedConnector.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.ctx.globalAlpha = 1.0;
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedConnector.highlight(this.expectedCanvas.ctx);
                    var fromMagnet = this.expectedConnector.source;
                    
                    this.state.draw(this.ctx);
                    this.state.doDown({pageX: fromMagnet.x, pageY: fromMagnet.y});
                    this.state.doMove({pageX: 100, pageY: 100});
                    this.state.draw(this.ctx);
                });
                
                it('should start input connector of wrong type', function() {
                    drawStartingPipeline(this);
                    // connector
                    this.expectedConnector.source = this.expectedInput.out_magnets[0];
                    this.expectedConnector.x = 100;
                    this.expectedConnector.y = 100;
                    this.expectedCanvas.ctx.globalAlpha = 0.75;
                    this.expectedConnector.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.ctx.globalAlpha = 1.0;
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedConnector.highlight(this.expectedCanvas.ctx);
                    var fromMagnet = this.expectedConnector.source;
                    
                    this.actualMethod.in_magnets[0].cdt = 14; // won't match raw
                    
                    this.state.draw(this.ctx);
                    this.state.doDown({pageX: fromMagnet.x, pageY: fromMagnet.y});
                    this.state.doMove({pageX: 100, pageY: 100});
                    this.state.draw(this.ctx);
                });
                
                it('should create input connector', function() {
                    drawStartingPipeline(this);
                    // connector
                    this.expectedConnector.source = this.expectedInput.out_magnets[0];
                    this.expectedConnector.dest = this.expectedMethod.in_magnets[0];
                    this.expectedCanvas.ctx.globalAlpha = 0.75;
                    this.expectedConnector.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.ctx.globalAlpha = 1.0;
                    this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                    this.expectedCanvas.ctx.lineWidth = 4;
                    this.expectedConnector.highlight(this.expectedCanvas.ctx);
                    var fromMagnet = this.expectedConnector.source,
                        toMagnet = this.expectedConnector.dest;
                    
                    this.state.draw(this.ctx);
                    this.state.doDown({pageX: fromMagnet.x, pageY: fromMagnet.y});
                    this.state.doMove({pageX: toMagnet.x, pageY: toMagnet.y});
                    this.state.doUp({pageX: toMagnet.x, pageY: toMagnet.y});
                    this.state.draw(this.ctx);
                });
                
                it('should scale to canvas', function() {
                    this.expectedInput.x = 45;
                    this.expectedInput.y = 127.5;
                    this.expectedMethod.x = 255;
                    this.expectedMethod.y = 22.5;
                    
                    this.expectedInput.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 45, y: 97, text: "in", style: "node", dir: 0});
                    this.expectedMethod.draw(this.expectedCanvas.ctx);
                    this.actualInput.x = 100;
                    this.actualInput.y = 310;
                    this.actualMethod.x = 520;
                    this.actualMethod.y = 100;
                    
                    this.state.scaleToCanvas();
                    this.state.draw(this.ctx);
                });
                
                it('should align along x axis', function() {
                    drawStartingPipeline(this);
                    
                    this.actualInput.y += 5;
                    this.actualMethod.y -= 5;
                    this.state.selection = [this.actualInput, this.actualMethod];
                    this.state.alignSelection("x");
                    this.state.selection = [];
                    this.state.draw(this.ctx);
                });
                
                it('should detect collisions', function() {
                    this.expectedInput.x = 95-19.4454;
                    this.expectedInput.y = 55+19.4454;
                    this.expectedMethod.x = 100+8.8388;
                    this.expectedMethod.y = 50-8.8388;
                    this.expectedInput.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 75.555, y: 43.945, text: "in", style: "node", dir: 0});
                    this.expectedMethod.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 120.089, y: 5.661, text: "example", style: "node", dir: 0});
                    
                    this.actualInput.x = this.actualMethod.x - 5;
                    this.actualInput.y = this.actualMethod.y + 5;
                    this.state.detectCollisions(this.actualInput);
                    this.state.draw(this.ctx);
                });
                
                it('should delete the input', function() {
                    this.expectedMethod.draw(this.expectedCanvas.ctx);
                    this.expectedCanvas.drawText(
                            {x: 111.25, y: 14.5, text: "example", style: "node", dir: 0});
                    
                    this.state.deleteObject(this.actualInput);
                    this.state.draw(this.ctx);
                });
                
                describe('and connector', function() {
                    beforeEach(function() {
                        this.actualConnector = new drydock_objects.Connector(
                                this.actualInput.out_magnets[0]);
                        this.actualInput.out_magnets.push(this.actualConnector);
                        this.actualConnector.dest = this.actualMethod.in_magnets[0];
                        this.actualMethod.in_magnets[0].connected.push(
                                this.actualConnector);
                        this.state.connectors.push(this.actualConnector);
                    });
                    
                    it('should move connector', function() {
                        this.expectedMethod.in_magnets[0].acceptingConnector = true;
                        this.expectedMethod.in_magnets[0].fill = '#ff8';
                        drawStartingPipeline(this);
                        // connector
                        this.expectedConnector.source = this.expectedInput.out_magnets[0];
                        this.expectedConnector.x = 100;
                        this.expectedConnector.y = 100;
                        this.expectedCanvas.ctx.globalAlpha = 0.75;
                        this.expectedConnector.draw(this.expectedCanvas.ctx);
                        this.expectedCanvas.ctx.globalAlpha = 1.0;
                        this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                        this.expectedCanvas.ctx.lineWidth = 4;
                        this.expectedConnector.highlight(this.expectedCanvas.ctx);
                        var fromMagnet = this.expectedMethod.in_magnets[0];
                        
                        this.state.draw(this.ctx);
                        // drag away from method
                        this.state.doDown(
                                {pageX: fromMagnet.x + 6, pageY: fromMagnet.y});
                        this.state.doMove({pageX: 100, pageY: 100});
                        
                        this.state.draw(this.ctx);
                    });
                    
                    it('shift click should move connector when nothing selected', function() {
                        this.expectedMethod.in_magnets[0].acceptingConnector = true;
                        this.expectedMethod.in_magnets[0].fill = '#ff8';
                        drawStartingPipeline(this);
                        // connector
                        this.expectedConnector.source = this.expectedInput.out_magnets[0];
                        this.expectedConnector.x = 100;
                        this.expectedConnector.y = 100;
                        this.expectedCanvas.ctx.globalAlpha = 0.75;
                        this.expectedConnector.draw(this.expectedCanvas.ctx);
                        this.expectedCanvas.ctx.globalAlpha = 1.0;
                        this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                        this.expectedCanvas.ctx.lineWidth = 4;
                        this.expectedConnector.highlight(this.expectedCanvas.ctx);
                        var fromMagnet = this.expectedMethod.in_magnets[0];
                        
                        this.state.draw(this.ctx);
                        // drag away from method
                        this.state.doDown({
                            pageX: fromMagnet.x,
                            pageY: fromMagnet.y,
                            shiftKey: true
                        });
                        this.state.doMove({pageX: 100, pageY: 100});
                        
                        this.state.draw(this.ctx);
                    });
                    
                    it('shift click on connector should move input', function() {
                        this.expectedMethod.in_magnets[0].acceptingConnector = true;
                        this.expectedMethod.in_magnets[0].fill = '#ff8';
                        drawStartingPipeline(this);
                        // connector
                        this.expectedConnector.source = this.expectedInput.out_magnets[0];
                        this.expectedConnector.x = 100;
                        this.expectedConnector.y = 100;
                        this.expectedCanvas.ctx.globalAlpha = 0.75;
                        this.expectedConnector.draw(this.expectedCanvas.ctx);
                        this.expectedCanvas.ctx.globalAlpha = 1.0;
                        this.expectedCanvas.ctx.strokeStyle = this.state.selectionColor;
                        this.expectedCanvas.ctx.lineWidth = 4;
                        this.expectedConnector.highlight(this.expectedCanvas.ctx);
                        var fromMagnet = this.expectedMethod.in_magnets[0];
                        
                        this.state.draw(this.ctx);
                        // select input node
                        this.state.doDown({
                            pageX: this.actualInput.x,
                            pageY: this.actualInput.y
                        });
                        this.state.doUp({
                            pageX: this.actualInput.x,
                            pageY: this.actualInput.y
                        });
                        // drag away from method
                        this.state.doDown({
                            pageX: fromMagnet.x,
                            pageY: fromMagnet.y
                        });
                        this.state.doMove({ pageX: 100, pageY: 100 });
                        
                        this.state.draw(this.ctx);
                    });
                    
                    it('should not move connector when read-only', function() {
                        drawStartingPipeline(this);
                        // connector
                        this.expectedConnector.source = this.expectedInput.out_magnets[0];
                        this.expectedConnector.dest = this.expectedMethod.in_magnets[0];
                        this.expectedCanvas.ctx.globalAlpha = 0.75;
                        this.expectedConnector.draw(this.expectedCanvas.ctx);
                        var fromMagnet = this.expectedMethod.in_magnets[0];

                        this.state.can_edit = false;
                        this.state.draw(this.ctx);
                        // drag away from method
                        this.state.doDown({
                            pageX: fromMagnet.x,
                            pageY: fromMagnet.y
                        });
                        this.state.doMove({ pageX: 100, pageY: 100 });
                        
                        this.state.draw(this.ctx);
                    });
                    
                    it('should autolayout', function() {
                        this.expectedInput.x = 60.10463893352559;
                        this.expectedInput.y = 22.5;
                        this.expectedMethod.x = 239.8953610664744;
                        this.expectedMethod.y = 127.5;
                        this.expectedInput.draw(this.expectedCanvas.ctx);
                        this.expectedMethod.draw(this.expectedCanvas.ctx);
                        this.expectedConnector.source = this.expectedInput.out_magnets[0];
                        this.expectedConnector.dest = this.expectedMethod.in_magnets[0];
                        this.expectedCanvas.ctx.globalAlpha = 0.75;
                        this.expectedConnector.draw(this.expectedCanvas.ctx);
                        this.expectedCanvas.drawText(
                                {x: 251.1453610664744, y: 92, text: "example", style: "node", dir: 0});
                        
                        this.state.draw(this.ctx);
                        this.state.testExecutionOrder();
                        this.state.autoLayout();
                        this.state.draw(this.ctx);
                    });
                });
            });
        });
    });
    
    describe('Pipeline families', function() {
        beforeEach(function() {
            this.$table = $('<table/>');
            this.is_user_admin = false;
            this.initial_data = [{
                name: 'Example',
                num_revisions: 1,
                published_version_display_name: null}];
        });
        
        it('should build a table', function() {
            pipeline_families.main(
                    this.is_user_admin,
                    this.$table,
                    this.initial_data);
            
            var $rows = this.$table.find('tr');
            expect($rows.length).toBe(2);
            var $cells = $rows.eq(1).find('td');
            expect($cells.eq(0).text()).toBe('Example'); // Name
            expect($cells.eq(3).text()).toBe('None'); // Published version
        });
        
        it('should display published version', function() {
            this.initial_data[0].published_version_display_name = "1: First";
            pipeline_families.main(
                    this.is_user_admin,
                    this.$table,
                    this.initial_data);
            
            var $rows = this.$table.find('tr');
            var $cells = $rows.eq(1).find('td');
            expect($cells.eq(3).text()).toBe('1: First'); // Published version
        });
    });

    describe("Pipeline functions", function() {
        beforeEach(function() {
            var width = 600,
                height = 300;

            jasmine.addMatchers(imagediff.jasmine);
            this.rawCanvas = imagediff.createCanvas(width, height);
            this.canvas = new drydock_objects.CanvasWrapper(this.rawCanvas);
            this.canvasState = new drydock.CanvasState(this.rawCanvas);

            this.api_pipeline = {"id":25,"url":"http://127.0.0.1:8000/api/pipelines/25/", "family_pk": 2, "family":"Test","revision_name":"","revision_desc":"Carl Sagan's science slam-jam","revision_number":3,"revision_parent":24,"revision_DateTime":"2015-06-10T19:36:31.570191Z","user":"kive","users_allowed":[],"groups_allowed":[],"inputs":[{"dataset_name":"input2","dataset_idx":1,"x":0.15,"y":0.15,"structure":{'compounddatatype':9, min_row:null, max_row:null }},{"dataset_name":"input1","dataset_idx":2,"x":0.05,"y":0.3,"structure":null}],"outputs":[{"dataset_name":"unmapped2_fastq","dataset_idx":1,"x":0.637772562280456,"y":0.633208895290869,"structure":null},{"dataset_name":"unmapped1_fastq","dataset_idx":2,"x":0.637772562280456,"y":0.633208895290869,"structure":null},{"dataset_name":"remap_conseq","dataset_idx":3,"x":0.637772562280456,"y":0.633208895290869,"structure":{"compounddatatype":10,"min_row":null,"max_row":null}},{"dataset_name":"remap","dataset_idx":4,"x":0.637772562280456,"y":0.633208895290869,"structure":{"compounddatatype":8,"min_row":null,"max_row":null}},{"dataset_name":"remap_counts","dataset_idx":5,"x":0.637772562280456,"y":0.633208895290869,"structure":{"compounddatatype":9,"min_row":null,"max_row":null}}],"steps":[{"transformation":4,"transformation_family":3,"step_num":1,"outputs_to_delete":[],"x":0.344662650584514,"y":0.5,"name":"prelim_map.py","cables_in":[{"source_step":0,"source":143,"source_dataset_name":"input1","dest":7,"dest_dataset_name":"fastq1","custom_wires":[],"keep_output":false},{"source_step":0,"source":142,"source_dataset_name":"input2","dest":8,"dest_dataset_name":"fastq2","custom_wires":[],"keep_output":false}],"outputs":[{"dataset_name":"prelim","dataset_idx":1,"x":0.0,"y":0.0,"structure":{"compounddatatype":7,"min_row":null,"max_row":null}}],"inputs":[{"dataset_name":"fastq1","dataset_idx":1,"x":0.0,"y":0.0,"structure":null},{"dataset_name":"fastq2","dataset_idx":2,"x":0.0,"y":0.0,"structure":null}]},{"transformation":5,"transformation_family":4,"step_num":2,"outputs_to_delete":[],"x":0.450583501602465,"y":0.257130788000083,"name":"remap.py","cables_in":[{"source_step":0,"source":143,"source_dataset_name":"input1","dest":10,"dest_dataset_name":"fastq1","custom_wires":[],"keep_output":false},{"source_step":0,"source":142,"source_dataset_name":"input2","dest":11,"dest_dataset_name":"fastq2","custom_wires":[],"keep_output":false},{"source_step":1,"source":9,"source_dataset_name":"prelim","dest":12,"dest_dataset_name":"prelim","custom_wires":[],"keep_output":false}],"outputs":[{"dataset_name":"remap","dataset_idx":1,"x":0.0,"y":0.0,"structure":{"compounddatatype":8,"min_row":null,"max_row":null}},{"dataset_name":"remap_counts","dataset_idx":2,"x":0.0,"y":0.0,"structure":{"compounddatatype":9,"min_row":null,"max_row":null}},{"dataset_name":"remap_conseq","dataset_idx":3,"x":0.0,"y":0.0,"structure":{"compounddatatype":10,"min_row":null,"max_row":null}},{"dataset_name":"unmapped1_fastq","dataset_idx":4,"x":0.0,"y":0.0,"structure":null},{"dataset_name":"unmapped2_fastq","dataset_idx":5,"x":0.0,"y":0.0,"structure":null}],"inputs":[{"dataset_name":"fastq1","dataset_idx":1,"x":0.0,"y":0.0,"structure":null},{"dataset_name":"fastq2","dataset_idx":2,"x":0.0,"y":0.0,"structure":null},{"dataset_name":"prelim","dataset_idx":3,"x":0.0,"y":0.0,"structure":{"compounddatatype":7,"min_row":null,"max_row":null}}]}],"outcables":[{"pk":128,"output_idx":1,"output_name":"unmapped2_fastq","output_cdt":null,"source_step":2,"source":55,"source_dataset_name":"unmapped2_fastq","custom_wires":[]},{"pk":129,"output_idx":2,"output_name":"unmapped1_fastq","output_cdt":null,"source_step":2,"source":54,"source_dataset_name":"unmapped1_fastq","custom_wires":[]},{"pk":130,"output_idx":3,"output_name":"remap_conseq","output_cdt":10,"source_step":2,"source":15,"source_dataset_name":"remap_conseq","custom_wires":[]},{"pk":131,"output_idx":4,"output_name":"remap","output_cdt":8,"source_step":2,"source":13,"source_dataset_name":"remap","custom_wires":[]},{"pk":132,"output_idx":5,"output_name":"remap_counts","output_cdt":9,"source_step":2,"source":14,"source_dataset_name":"remap_counts","custom_wires":[]}],"removal_plan":"http://127.0.0.1:8000/api/pipelines/25/removal_plan/"};

            // Throw some more functions into the CanvasState object
            // Dirty hack for now TODO: move this into CanvasState

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
                    if (shapes[i].label == label)
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
        }

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
            
            it('should ignore order of cables_in when wiring', function(){
                var pipeline = loadApiPipeline(this.canvasState, this.api_pipeline),
                    expectedImage;
                pipeline.draw();
                expectedImage = imagediff.toImageData(this.rawCanvas);
                
                // Now reverse the order of the cables_in
                this.api_pipeline.steps[1].cables_in.reverse();
                pipeline = loadApiPipeline(this.canvasState, this.api_pipeline);
                pipeline.draw();
                
                expect(this.rawCanvas).toImageDiffEqual(
                        expectedImage);
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

        function loadAndSerialize(canvasState, api_pipeline, additional_args){
            var pipeline = loadApiPipeline(canvasState, api_pipeline);
            pipeline.draw();
            return pipeline.serialize(additional_args);
        }

        describe('Serialize', function(){
            it('should serialize', function(){
                var test = loadAndSerialize(this.canvasState, this.api_pipeline);
            });

            it('should check structure', function() {
                var serialized = loadAndSerialize(
                    this.canvasState,
                    this.api_pipeline,
                    {'pass_thru': "pass this argument thru"}
                );

                expect(serialized.pass_thru).toBe("pass this argument thru");
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

                    if(ser_input.structure === null) {
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
                    var api_output = self.api_pipeline.outcables[index];


                    expect(ser_output.output_cdt).toBe(api_output.output_cdt);
                    expect(ser_output.output_idx).toBe(api_output.output_idx);
                    expect(ser_output.output_name).toBe(api_output.output_name);
                    expect(ser_output.source_dataset_name).toBe(api_output.source_dataset_name);
                    expect(ser_output.source_step).toBe(api_output.source_step);
                });
            });
        });
    });
})();
