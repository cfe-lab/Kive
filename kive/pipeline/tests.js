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
            this.ctx = this.canvas.getContext('2d');
            this.fill = "#999";
        });
        
        it('should draw a method with inputs and outputs', function(done) {
            this.expectedImage = new Image();
            this.expectedImage.src = 'pipeline/test_assets/magnet-unlit.png';
            
            this.ctx.beginPath();
            this.ctx.arc(20, 10, 5, 0, 2 * Math.PI, true);
            this.ctx.closePath();
            this.ctx.fillStyle = this.fill;
            this.ctx.fill();
            
            $(this.expectedImage).load(this, function(e) {
                var testCase = e.data,
                    tolerance = 4; // out of 255
                expect(testCase.canvas).toImageDiffEqual(testCase.expectedImage, 4);
                done();
            });
        });
    });
})();