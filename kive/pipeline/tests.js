(function() {
    "use strict";
    
    describe("Canvas classes", function() {
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
        
        it('should allow raw calls to canvas context', function() {
            this.expectedImage.src = 'pipeline/test_assets/magnet-unlit.png';
            
            this.ctx.beginPath();
            this.ctx.arc(100, 10, 5, 0, 2 * Math.PI, true);
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
                this.expectedImage.src = 'pipeline/test_assets/magnet-unlit.png';
                
                this.magnet.draw(this.ctx);
            });
            
            it('should draw a magnet with highlight', function() {
                this.expectedImage.src = 'pipeline/test_assets/magnet-highlight.png';
                
                this.magnet.draw(this.ctx);
                this.magnet.highlight(this.ctx);
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