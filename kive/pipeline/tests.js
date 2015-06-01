(function() {
    "use strict";
    
    describe("Canvas classes", function() {
        beforeEach(function() {
            jasmine.addMatchers(imagediff.jasmine);
            this.canvas = imagediff.createCanvas();
            this.expectedCanvas = imagediff.createCanvas();
            this.rgb_tolerance = 4; // maximum is 255
            this.ctx = this.canvas.getContext('2d');
            this.expectedCtx = this.expectedCanvas.getContext('2d');
            this.fill = "#fff";
            
            this.expectCircle = function(args) {
                this.expectedCtx.save();
                this.expectedCtx.beginPath();
                this.expectedCtx.arc(args.x, args.y, args.r, 0, 2 * Math.PI);
                this.expectedCtx.closePath();
                this.expectedCtx.fillStyle = args.fill || "#fff";
                this.expectedCtx.fill();
                this.expectedCtx.restore();
            };
            this.expectText = function(args) {
                this.expectedCtx.save();
                this.expectedCtx.font = '9pt Lato, sans-serif';
                this.expectedCtx.textBaseline = 'middle';
                this.expectedCtx.textAlign = 'right';
                this.expectedCtx.fillStyle = '#fff';
                this.expectedCtx.globalAlpha = 0.5;
                this.expectedCtx.fillRect(
                    args.x + 2,
                    args.y - 7.5,
                    -4 - this.expectedCtx.measureText(args.text).width,
                    15
                );
                this.expectedCtx.globalAlpha = 1;
                this.expectedCtx.fillStyle = '#000';
                this.expectedCtx.fillText(args.text, args.x, args.y);
                this.expectedCtx.restore();
            };
        });
        
        afterEach(function() {
            expect(this.canvas).toImageDiffEqual(
                    this.expectedCanvas,
                    this.rgb_tolerance);
        });
        
        
        it('should allow raw calls to canvas context', function() {
            this.expectCircle({x: 100, y:10, r:5});
            
            this.ctx.beginPath();
            this.ctx.arc(100, 10, 5, 0, 2 * Math.PI);
            this.ctx.closePath();
            this.ctx.fillStyle = this.fill;
            this.ctx.fill();
        });
        
        describe("Magnet", function() {
            beforeEach(function() {
                var r = 5,
                    parent = this,
                    attract = 5;
                this.magnet = new drydock_objects.Magnet(parent, r, attract);
                this.magnet.x = 100;
                this.magnet.y = 10;
                this.magnet.label = 'example';
            });
            
            it('should draw an unlit magnet', function() {
                this.expectCircle({x: 100, y:10, r:5});
                
                this.magnet.draw(this.ctx);
            });
            
            it('should draw a magnet with highlight', function() {
                this.expectCircle({x: 100, y:10, r:5});
                this.expectText({x:90, y:10, text: 'example'});
                
                this.magnet.draw(this.ctx);
                this.magnet.highlight(this.ctx);
            });
        });
    });
    
    describe("Old canvas classes", function() {
        beforeAll(function() {
            if (window.location.href.indexOf('http') !== 0) {
                pending('imagediff not supported in file:// protocol.' +
                        'Try this: cd Kive/kive; python -m SimpleHTTPServer 8080');
            }
        });
        
        beforeEach(function() {
            jasmine.addMatchers(imagediff.jasmine);
            this.canvas = imagediff.createCanvas();
            this.expectedImage = new Image();
            this.rgb_tolerance = 4; // maximum is 255
            this.ctx = this.canvas.getContext('2d');
            this.fill = "#fff";
        });
        
        afterEach(function(done) {
            $(this.expectedImage).load(this, function(e) {
                var testCase = e.data,
                tolerance = 4; // out of 255
                expect(testCase.canvas).toImageDiffEqual(
                        testCase.expectedImage,
                        testCase.rgb_tolerance);
                done();
            });
        });
        
        describe("Magnet", function() {
            beforeEach(function() {
                var r = 5,
                    parent = this,
                    attract = 5;
                this.magnet = new drydock_objects.Magnet(parent, r, attract);
                this.magnet.x = 100;
                this.magnet.y = 10;
                this.magnet.label = 'example';
            });
            
            it('should highlight when marked as accepting connector', function() {
                this.expectedImage.src = 'pipeline/test_assets/magnet-highlight.png';

                this.magnet.acceptingConnector = true;
                this.magnet.draw(this.ctx);
            });
            
            it('should have custom fill colour', function() {
                this.expectedImage.src = 'pipeline/test_assets/magnet-highlight-new-fill.png';
                
                this.magnet.fill = '#ff8';
                this.magnet.acceptingConnector = true;
                this.magnet.draw(this.ctx);
            });
        });
    });
})();