(function() {
    "use strict";
    
    describe("Canvas classes", function() {
        beforeEach(function() {
            jasmine.addMatchers(imagediff.jasmine);
            this.canvas = imagediff.createCanvas();
            this.expectedRawCanvas = imagediff.createCanvas();
            this.ctx = this.canvas.getContext('2d');
            this.expectedCanvas = new drydock_objects.CanvasWrapper(
                    this.expectedRawCanvas);
            this.expectedCanvas.ctx.fillStyle = "#fff";
        });
        
        afterEach(function() {
            expect(this.canvas).toImageDiffEqual(this.expectedRawCanvas);
        });
        
        it('should allow raw calls to canvas context', function() {
            this.expectedCanvas.drawCircle({x: 100, y:10, r:5});
            
            this.ctx.beginPath();
            this.ctx.arc(100, 10, 5, 0, 2 * Math.PI);
            this.ctx.closePath();
            this.ctx.fillStyle = "#fff";
            this.ctx.fill();
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
            
            it('should detect connection at centre', function() {
                expect(this.magnet.contains(100, 10)).toBe(true);
            });
            it('should detect connection at right edge', function() {
                expect(this.magnet.contains(108, 10)).toBe(true);
            });
            it('should detect no connection past right edge', function() {
                expect(this.magnet.contains(109, 10)).toBe(false);
            });
            it('should allow changes to attraction range', function() {
                this.magnet.attract += 1;
                expect(this.magnet.contains(109, 10)).toBe(true);
                expect(this.magnet.contains(110, 10)).toBe(false);
            });
            it('should detect connection at diagonal edge', function() {
                expect(this.magnet.contains(105, 16)).toBe(true);
            });
            it('should detect no connection past diagonal edge', function() {
                expect(this.magnet.contains(105, 17)).toBe(false);
            });
        });
    });
})();