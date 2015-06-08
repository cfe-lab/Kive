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
        });
        
        afterEach(function() {
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
                var vertex = vertices[i];
                canvas.drawCircle({x: vertex.x, y: vertex.y, r: 2});
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
                    inputs = { 1: { cdt_pk: 7, datasetname: "in" } },
                    outputs = { 1: { cdt_pk: 7, datasetname: "out" } };

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
                         {x: 169.1, y: 15},
                         {x: 150, y: 4},
                         {x: 130.9, y: 15},
                         {x: 179.4, y: 74},
                         {x: 198.5, y: 63},
                         {x: 198.5, y: 41},
                         {x: 179.4, y: 52},
                         {x: 130.95, y: 44.53},
                         {x: 150, y: 36.5},
                         {x: 169.1, y: 25.5}]);
                
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
    });
})();
