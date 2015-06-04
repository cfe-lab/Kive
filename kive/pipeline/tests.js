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
        });
        
        afterEach(function() {
            var rgb_tolerance = 16; // max 255
            expect(this.rawCanvas).toImageDiffEqual(
                    this.expectedRawCanvas,
                    rgb_tolerance);
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
                    expectedFill = isExpectedToContain ? 'green' : 'red',
                    isContained,
                    actualFill,
                    pad = 5; // ignored by all except connectors
                target.draw(this.expectedCanvas.ctx);
                this.expectedCanvas.ctx.fillStyle = expectedFill;
                this.expectedCanvas.drawCircle({x: x, y: y, r: 2});
                
                target.draw(this.canvas.ctx);
                isContained = target.contains(x, y, pad);
                actualFill = isContained ? 'green': 'red';
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
                this.expectedCanvas.drawCircle({x: 100, y: 10, r: 5});
                this.expectedCanvas.drawText({x:90, y: 10, text: 'example'});

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
                this.ctx.font = '9pt Lato, sans-serif';
                this.ctx.textBaseline = 'middle';
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
                this.expectedCanvas.ctx.translate(101.552, 19.242);
                this.expectedCanvas.ctx.rotate(-0.226);
                this.expectedCanvas.drawText(
                        {x: 0, y: 0, text: "example", dir: 0, style: "midconnector"});
                
                this.connector.dest = this.dest;
                this.ctx.font = '9pt Lato, sans-serif';
                this.ctx.textBaseline = 'middle';
                this.connector.draw(this.ctx);
                this.ctx.strokeStyle = 'blue';
                this.connector.highlight(this.ctx);
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
    });
})();
