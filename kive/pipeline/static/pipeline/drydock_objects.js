/**
 * drydock_objects.js
 *   JS prototypes that are used to populate canvasState
 *   (see drydock.js)
 */
var drydock_objects = (function() {
    "use strict";
    var my = {};
    
    // TODO: make this an internal variable once all the classes that use it are in this block
    my.statusColorMap = {
            'CLEAR': 'green',
            'FAILURE': 'red',
            'RUNNING': 'orange',
            'READY': 'orange',
            'WAITING': 'yellow',
    };
    
    /**
     * A helper class to easily draw primitive shapes on the canvas.
     */
    my.CanvasWrapper = function(canvas, ctx) {
        this.ctx = ctx !== undefined ? ctx : canvas.getContext('2d');
    };
    
    /**
     * Draw a circle.
     * 
     * @param args.x: the x position of the circle centre
     * @param args.y: the y position of the circle centre
     * @param args.r: the radius of the circle
     */
    my.CanvasWrapper.prototype.drawCircle = function(args) {
        this.ctx.beginPath();
        this.ctx.arc(args.x, args.y, args.r, 0, 2 * Math.PI);
        this.ctx.closePath();
        this.ctx.fill();
    };
    
    /**
     * Draw an ellipse.
     * 
     * @param args.x: the x position of the ellipse centre
     * @param args.y: the y position of the ellipse centre
     * @param args.rx: the radius of the ellipse along the x axis
     * @param args.ry: the radius of the ellipse along the y axis
     */
    my.CanvasWrapper.prototype.drawEllipse = function(args) {
        this.ctx.save(); // save state
        this.ctx.translate(args.x - args.rx, args.y - args.ry);
        this.ctx.scale(args.rx, args.ry);
        this.drawCircle({x: 1, y: 1, r: 1});
        this.ctx.restore(); // restore to original state
    };
    
    /**
     * Stroke an ellipse.
     * 
     * @param args.x: the x position of the ellipse centre
     * @param args.y: the y position of the ellipse centre
     * @param args.rx: the radius of the ellipse along the x axis
     * @param args.ry: the radius of the ellipse along the y axis
     */
    my.CanvasWrapper.prototype.strokeEllipse = function(args) {
        this.ctx.save(); // save state
        this.ctx.translate(args.x - args.rx, args.y - args.ry);
        this.ctx.scale(args.rx, args.ry);
        this.ctx.beginPath();
        this.ctx.arc(1, 1, 1, 0, 2 * Math.PI);
        this.ctx.closePath();
        this.ctx.restore(); // restore to original state
        this.ctx.stroke();
    };
    
    /**
     * Draw text with a standard font, colour, and background rectangle.
     * 
     * The background rectangle is drawn around the text, so the anchor point
     * is inside the rectangle.
     * 
     * @param args.x: the x position of the anchor point
     * @param args.y: the y position of the middle of the line of text
     * @param args.text: the text to draw
     * @param args.dir: 1 for anchor point on the left, -1 for the right(default),
     *  or 0 for the centre
     */
    my.CanvasWrapper.prototype.drawText = function(args) {
        var width,
            height,
            yoff,
            dir = args.dir === 1 ? 1 : args.dir === 0 ? 0 : -1,
            rectArgs = {x: args.x, y: args.y},
            textFill = "black",
            margin = 2;
        this.ctx.save();
        this.ctx.globalAlpha = 0.5;
        switch (args.style) {
        case 'node':
            this.ctx.font = '10pt Lato, sans-serif';
            this.ctx.textBaseline = 'alphabetic';
            rectArgs.height = 14;
            rectArgs.y -= 11;
            break;
        case 'connector':
            this.ctx.font = '10pt Lato, sans-serif';
            this.ctx.textBaseline = 'middle';
            rectArgs.height = 14;
            rectArgs.y -= 7;
            rectArgs.r = 6;
            margin = 5;
            textFill = "white";
            this.ctx.globalAlpha = 1;
            break;
        default:
            this.ctx.font = '9pt Lato, sans-serif';
            this.ctx.textBaseline = 'middle';
            rectArgs.height = 15;
            rectArgs.y -= 7.5;
        }
        this.ctx.textAlign = dir === 1 ? 'left' : dir === 0 ? 'center' : 'right';
        // make a backing box so the label is on the fill colour
        rectArgs.width = 2*margin + this.ctx.measureText(args.text).width;
        if (dir === 0) {
            rectArgs.x -= rectArgs.width/2;
        }
        else {
            rectArgs.x -= dir * margin;
            rectArgs.width *= dir;
        }
        this.fillRect(rectArgs);
        this.ctx.globalAlpha = 1;
        this.ctx.fillStyle = textFill;
        this.ctx.fillText(args.text, args.x, args.y);
        this.ctx.restore();
    };
    
    /**
     * Draw a rectangle or rounded rectangle.
     * 
     * @param args.x: the left edge of the rectangle
     * @param args.y: the top edge of the rectangle
     * @param args.width: the width of the rectangle
     * @param args.height: the height of the rectangle
     * @param args.r: the radius of the corners, or undefined for a regular
     *  rectangle
     */
    my.CanvasWrapper.prototype.fillRect = function(args) {
        if (args.r === undefined) {
            this.ctx.fillRect(args.x, args.y, args.width, args.height);
        }
        else {
            this.ctx.beginPath();
            // middle of top edge
            this.ctx.moveTo(args.x + args.width/2, args.y);
            // to middle of right edge
            this.ctx.arcTo(
                    args.x + args.width, args.y,
                    args.x + args.width, args.y + args.height/2,
                    args.r);
            // to middle of bottom edge
            this.ctx.arcTo(
                    args.x + args.width, args.y + args.height,
                    args.x + args.width/2, args.y + args.height,
                    args.r);
            // to middle of left edge
            this.ctx.arcTo(
                    args.x, args.y + args.height,
                    args.x, args.y + args.height/2,
                    args.r);
            // to middle of top edge
            this.ctx.arcTo(
                    args.x, args.y,
                    args.x + args.width/2, args.y,
                    args.r);
            this.ctx.closePath();
            this.ctx.fill();
        }
    };
    
    // TODO: Convert the whole file to this module, then combine all the sections.
    return my;
}());

var _statusColorMap = drydock_objects.statusColorMap;

var Geometry = {
    inEllipse: function(mx, my, cx, cy, rx, ry) {
        var dx = mx - cx,
            dy = my - cy;
        return (dx*dx) / (rx*rx)
             + (dy*dy) / (ry*ry) <= 1;
    },
    inRectangle: function(mx, my, x, y, w, h) {
        return mx > x && mx < x + w
            && my > y && my < y + h;
    },
    inRectangleFromCentre: function(mx, my, cx, cy, w2, h2) {
        return Math.abs(mx - cx) < w2
            && Math.abs(my - cy) < h2;
    },
    inCircle: function(mx, my, cx, cy, r) {
        var dx = cx - mx,
            dy = cy - my;
        return Math.sqrt(dx*dx + dy*dy) <= r;
    },
    ltLine: function(mx, my, x1, y1, x2, y2) {
        return x2 != x1 ? (y2 - y1) / (x2 - x1) * (mx - x1) > my - y1 : null;
    },
    inPolygon: function(mx, my, shape) {
        // ray casting algorithm
        // argument 'shape' is an array of objects each with properties x and y
        var o = [ -300, -300 ],
            line, s1_x, s1_y, s, t,
            s2_x = mx - o[0],
            s2_y = my - o[1],
            intersections = 0;
        
        for (var j = 0; j < shape.length; j++) {
            line = shape.slice(j, j+2);
            if (line.length == 1) line.push(shape[0]);
            
            s1_x = line[1].x - line[0].x;
            s1_y = line[1].y - line[0].y;
            s = (-s1_y * (line[0].x - o[0]) + s1_x * (line[0].y - o[1])) / (-s2_x * s1_y + s1_x * s2_y);
            t = ( s2_x * (line[0].y - o[1]) - s2_y * (line[0].x - o[0])) / (-s2_x * s1_y + s1_x * s2_y);
            
            if (s >= 0 && s <= 1 && t >= 0 && t <= 1)
                intersections++;
//            console.log('line', line[0].x, line[0].y, line[1].x, line[1].y, 's-t', s,t, 'x', intersections);
        }
    
        return intersections % 2;
    },
    isometricXCoord: function(x,y) {
        // isometric x-coordinate is explained in issue #277.
        // using a -30° line that intersects (0,0) and a 30° line that intersects (x,y), find the intersection of the two.
        // then compute the distance from this intersection to (x,y). tan(pi/6) = 1/sqrt(3) ~ 0.577350269
        return x * 0.577350269 - y;
    },
    isometricYCoord: function(x,y) {
        // isometric y-coordinate is explained in issue #277.
        // using a 30° line that intersects (0,0) and a -30° line that intersects (x,y), find the intersection of the two.
        // then compute the distance from this intersection to (x,y). tan(pi/6) = 1/sqrt(3) ~ 0.577350269
        return x * 0.577350269 + y;
        
        /*
         * unabridged version:
         
        var tan30 = Math.tan(Math.PI/6),
            // (x0, y0) is the coordinate of the intersection
            x0 = (x * tan30 - y) / (2 * tan30),
            y0 = - x0 * tan30,
            dx = x - x0,
            dy = y - y0,
            // dh is the distance from (x0,y0) to (x,y). it is a 30° line.
            dh = Math.sqrt(dx*dx + dy*dy);
        return dh;
        
         */
    },
    iso2twodim: function(iso_x, iso_y) {
        // inverse of [ isometricXCoord, isometricYCoord ]
        return {
            x: (iso_y + iso_x) / 1.154700538, 
            y: (iso_y - iso_x) / 2
        };
    },
    isometricSort: function(x1,y1,x2,y2) {
        // returns 1 if the first set of coordinates is after the second,
        // -1 if the reverse is true, 0 if it's a tie. order goes left-to-right,
        // top-to-bottom if you sort of rotate your screen 30° clockwise and get
        // in the isometric plane.
        // includes ±7 pixels of fuzziness in the top-to-bottom decision. 
        
        if (x1 instanceof Object && y1 instanceof Object && [ x1.x, x1.y, y1.x, y1.y ].indexOf(undefined) === -1) {
            // transform alternative syntax
            return Geometry.isometricSort(x1.x, x1.y, y1.x, y1.y);
        }
        
        var y_diff = (x1 - x2) * 0.577350269 + y1 - y2; //tan(pi/6) = 1/sqrt(3) ~ 0.577350269
        if (y_diff > 7) {
            return 1;
        } else if (y_diff < -7) {
            return -1;
        } else {
            var x_diff = y_diff + (y2 - y1) * 2;
            if (x_diff > 0) {
                return 1;
            } else if (x_diff < 0) {
                return -1;
            } else {
                return 0;
            }
        }
    }
};

/*
To be implemented in the future:
Parent classes from which RawNode, CDtNode, MethodNode, and OutputNode will stem.
*/
function Node(pk, label, x, y, w, h, inset, offset, fill) {
    this.pk = pk;
    this.label = label || '';
    this.x = x || 0; // defaults to top left corner
    this.y = y || 0;
    this.w = w || 25; // defaults to top left corner
    this.h = h || 25;
    this.inset = inset || 10; // distance of magnet from center
    this.offset = offset || 18; // distance of label from center
    this.dx = 0;// display offset to avoid collisions, relative to its "true" coordinates
    this.dy = 0;
    this.fill = fill || '#888';
    this.in_magnets = [];
    this.out_magnets = [];
}
function InputNode(pk, x, y, w, h, fill, inset, offset, label) {
    Node.call(this, pk, label, x, y, w, h, inset, offset, fill);
}
InputNode.prototype = Object.create(Node.prototype);
InputNode.prototype.constructor = InputNode;

drydock_objects = (function(my) {
    "use strict";
    
    /**
     * A base class for both cylindrical nodes: RawNode and OutputNode.
     */
    my.CylinderNode = function(x, y, label) {
        /*
        Node representing an output.
        Rendered as a cylinder.
         */
        this.x = x || 0; // defaults to top left corner
        this.y = y || 0;
        this.dx = 0;// display offset to avoid collisions, relative to its "true" coordinates
        this.dy = 0;
        this.r = 20; // x-radius (ellipse)
        this.r2 = this.r / 2; // y-radius (ellipse)
        this.w = this.r; // for compatibility
        this.h = 25; // height of cylinder
        this.offset = 18; // distance of label from center
        this.label = label || '';
        this.fill = "grey";
        this.magnetOffset = {x: -12, y: -this.h/2};
        this.in_magnets = [];
        this.out_magnets = [];
    }
    
    my.CylinderNode.prototype.draw = function(ctx) {
        var cx = this.x + this.dx,
            cy = this.y + this.dy,
            canvas = new my.CanvasWrapper(undefined, ctx);
        
        // Highlight the method based on status.
        if(this.highlightStroke !== undefined) {
            ctx.save();
    
            ctx.strokeStyle = this.highlightStroke;
            ctx.lineWidth = 5;
    
            // draw bottom ellipse
            canvas.strokeEllipse({x: cx, y: cy + this.h/2, rx: this.r, ry: this.r2});
    
            // draw stack
            ctx.strokeRect(cx - this.r, cy - this.h/2, this.r * 2, this.h);
    
            // draw top ellipse
            canvas.strokeEllipse({x: cx, y: cy - this.h/2, rx: this.r, ry: this.r2});
    
            ctx.restore();
        }
        
        // draw bottom ellipse
        ctx.fillStyle = this.fill;
        canvas.drawEllipse({x: cx, y: cy + this.h/2, rx: this.r, ry: this.r2});
        
        // draw stack 
        ctx.fillRect(cx - this.r, cy - this.h/2, this.r * 2, this.h);
        
        // draw top ellipse
        canvas.drawEllipse({x: cx, y: cy - this.h/2, rx: this.r, ry: this.r2});
        
        // some shading
        ctx.fillStyle = '#fff';
        ctx.globalAlpha = 0.35;
        canvas.drawEllipse({x: cx, y: cy - this.h/2, rx: this.r, ry: this.r2});
        ctx.globalAlpha = 1.0;
        
        // draw magnet
        var magnet = this.in_magnets[0] || this.out_magnets[0];
        magnet.x = cx + this.magnetOffset.x;
        magnet.y = cy + this.magnetOffset.y;
        magnet.draw(ctx);
    };
    
    my.CylinderNode.prototype.highlight = function(ctx) {
        var cx = this.x + this.dx,
            cy = this.y + this.dy,
            cable = this.in_magnets[0].connected[0],
            canvas = new my.CanvasWrapper(undefined, ctx);
        
        // This line means that we are drawing "behind" the canvas now.
        // We must set it back after we're done otherwise it'll be utter chaos.
        ctx.globalCompositeOperation = 'destination-over';
        
        // draw bottom ellipse
        canvas.strokeEllipse({x: cx, y: cy + this.h/2, rx: this.r, ry: this.r2});
        
        // draw stack
        ctx.strokeRect(cx - this.r, cy - this.h/2, this.r * 2, this.h);
        
        // draw top ellipse
        canvas.strokeEllipse({x: cx, y: cy - this.h/2, rx: this.r, ry: this.r2});
        
        ctx.globalCompositeOperation = 'source-over';
        
        if (cable) {
            cable.highlight(ctx);
        }
    }
    
    my.CylinderNode.prototype.contains = function(mx, my) {
        var cx = this.x + this.dx,
            cy = this.y + this.dy;
        // node is comprised of a rectangle and two ellipses
        return Geometry.inRectangleFromCentre(mx, my, cx, cy, this.r, this.h/2)
            || Geometry.inEllipse(mx, my, cx, cy - this.h/2, this.r, this.r2)
            || Geometry.inEllipse(mx, my, cx, cy + this.h/2, this.r, this.r2);
    };
    
    my.CylinderNode.prototype.getVertices = function() {
        var cx = this.x + this.dx,
            cy = this.y + this.dy;
            
        var x1 = cx + this.r,
            x2 = cx - this.r,
            y1 = cy + this.h/2,
            y2 = y1 - this.h;
        
        // Include centre to collide with small objects completely inside border.
        return [
            { x: cx, y: cy },
            { x: x1, y: y1 },
            { x: x2, y: y1 },
            { x: x1, y: y2 },
            { x: x2, y: y2 },
            { x: cx, y: cy + this.h/2 + this.r2 },
            { x: cx, y: cy - this.h/2 - this.r2 }
        ];
    };
    
    my.CylinderNode.prototype.highlight = function(ctx) {
        var cx = this.x + this.dx,
            cy = this.y + this.dy,
            canvas = new my.CanvasWrapper(undefined, ctx);
        
        // This line means that we are drawing "behind" the canvas now.
        // We must set it back after we're done otherwise it'll be utter chaos.
        ctx.globalCompositeOperation = 'destination-over';
        
        // draw bottom ellipse
        canvas.strokeEllipse({x: cx, y: cy + this.h/2, rx: this.r, ry: this.r2});
        
        // draw stack
        ctx.strokeRect(cx - this.r, cy - this.h/2, this.r * 2, this.h);
        
        // draw top ellipse
        canvas.strokeEllipse({x: cx, y: cy - this.h/2, rx: this.r, ry: this.r2});
        
        ctx.globalCompositeOperation = 'source-over';
    }
    
    my.CylinderNode.prototype.getLabel = function() {
        return new NodeLabel(
                this.label,
                this.x + this.dx,
                this.y + this.dy - this.h/2 - this.offset);
    };

    my.RawNode = function(x, y, label) {
        /*
        Node representing an unstructured (raw) datatype.
         */
        my.CylinderNode.call(this, x, y, label);
        this.fill = "#8D8";
        this.magnetOffset = {x: 10, y: this.r2/2};
        this.inset = 10; // distance of magnet from center
        // Input node always has one magnet
        this.out_magnets.push(new Magnet(this, 5, 2, "white", null, this.label, null, true));
    }
    my.RawNode.prototype = Object.create(my.CylinderNode.prototype);
    my.RawNode.prototype.constructor = my.RawNode;

    my.CdtNode = function(pk, x, y, label) {
        /*
        Node represents a Compound Datatype (CSV structured data).
        Rendered as a square shape.
         */
        this.pk = pk;
        this.x = x || 0;
        this.y = y || 0;
        this.dx = 0;// display offset to avoid collisions, relative to its "true" coordinates
        this.dy = 0;
        this.w = 45;
        this.h = 28;
        this.fill = "#88D";
        this.inset = 13;
        this.offset = 15;
        this.label = label || '';
        this.in_magnets = [];
        this.out_magnets = [ new Magnet(this, 5, 2, "white", this.pk, this.label, null, true) ];
    }
    
    my.CdtNode.prototype.draw = function(ctx) {
        var cx = this.x + this.dx,
            cy = this.y + this.dy,
            out_magnet;
        
        ctx.fillStyle = this.fill;
        
        // draw base
        var prism_base = cy + this.h/2;
        ctx.beginPath();
        ctx.moveTo(cx - this.w/2, prism_base);
        ctx.lineTo(cx, prism_base + this.w/4);
        ctx.lineTo(cx + this.w/2, prism_base);
        ctx.lineTo(cx + this.w/2, prism_base - this.h);
        ctx.lineTo(cx - this.w/2, prism_base - this.h);
        ctx.closePath();
        ctx.fill();
        
        // draw top
        var prism_cap = cy - this.h/2;
        ctx.beginPath();
        ctx.moveTo(cx - this.w/2, prism_cap);
        ctx.lineTo(cx, prism_cap + this.w/4);
        ctx.lineTo(cx + this.w/2, prism_cap);
        ctx.lineTo(cx, prism_cap - this.w/4);
        ctx.closePath();
        ctx.fill();
        
        // some shading
        ctx.fillStyle = '#fff';
        ctx.globalAlpha = 0.35;
        ctx.fill();
        ctx.globalAlpha = 1.0;
    
        // draw magnet
        out_magnet = this.out_magnets[0];
        out_magnet.x = cx + this.inset;
        out_magnet.y = cy + this.w/8;
        out_magnet.draw(ctx);
    };
    
    my.CdtNode.prototype.getVertices = function() {
        var cx = this.x + this.dx,
            cy = this.y + this.dy;
        
        var w2 = this.w/2,
            butt = cy + this.h/2,
            cap  = cy - this.h/2;
        
        return [
            { x: cx,      y: cy },
            { x: cx - w2, y: butt },
            { x: cx,      y: butt + w2/2 },
            { x: cx + w2, y: butt },
            { x: cx + w2, y: cap },
            { x: cx,      y: cap - w2/2 },
            { x: cx - w2, y: cap }
        ];
    };
    
    my.CdtNode.prototype.highlight = function(ctx) {
        var cx = this.x + this.dx,
            cy = this.y + this.dy;
        
        ctx.globalCompositeOperation = 'destination-over';
        ctx.lineJoin = 'bevel';
        
        var w2 = this.w/2,
            h2 = this.h/2,
            butt = cy + h2,
            cap = cy - h2;
        
        ctx.beginPath();
        ctx.moveTo(cx - w2, butt);
        ctx.lineTo(cx,      butt + w2/2);
        ctx.lineTo(cx + w2, butt);
        ctx.lineTo(cx + w2, cap);
        ctx.lineTo(cx,      cap - w2/2);
        ctx.lineTo(cx - w2, cap);
        ctx.closePath();
        ctx.stroke();
        
        ctx.globalCompositeOperation = 'source-over';
    };
    
    
    my.CdtNode.prototype.contains = function(mx, my) {
        /*
        Are mouse coordinates within the perimeter of this node?
         */
        var dx = Math.abs(this.x + this.dx - mx),
            dy = Math.abs(this.y + this.dy - my);
        
        // mouse coords are within the 4 diagonal lines.
        // can be checked with 1 expression because the 4 lines are mirror images of each other
        return this.h/2 + this.w/4 - dy > dx / 2 
        // then check the horizontal boundaries on the sides of the hexagon
            && dx < this.w/2;
    };
    
    my.CdtNode.prototype.getLabel = function() {
        return new NodeLabel(this.label, this.x + this.dx, this.y + this.dy - this.h/2 - this.offset);
    };
    
    // TODO: Convert the whole file to this module, then combine all the sections.
    return my;
}(drydock_objects));
var CDtNode = drydock_objects.CdtNode;
var RawNode = drydock_objects.RawNode;

function MethodNode (pk, family, x, y, fill, label, inputs, outputs, status, log_id) {
    /*
    CONSTRUCTOR
    A MethodNode is a rectangle of constant width (w) and varying height (h)
    where h is proportional to the maximum number of xputs (inputs or outputs).
    h = max(n_inputs, n_ouputs) * spacing
    Holes for inputs and outputs are drawn at some (inset) into the left
    and right sides, respectively.  The width must be greater than 2 * inset.
    */
    this.pk = pk;
    this.family = family; // can be passed from database

    this.x = x || 0;
    this.y = y || 0;
    this.dx = 0;// display offset to avoid collisions, relative to its "true" coordinates
    this.dy = 0;
    this.w = 10;
    this.inputs = inputs;
    this.outputs = outputs;

    this.n_inputs = Object.keys(inputs).length;
    this.n_outputs = Object.keys(outputs).length;

    this.inset = 10; // distance from left or right edge to center of hole
    this.offset = 10; // space between bottom of node and label

    this.spacing = 20; // separation between pins
    this.h = Math.max(this.n_inputs, this.n_outputs) * this.spacing;
    this.fill = fill || '#999';
    this.label = label || '';
    
    this.stack = 20;
    this.scoop = 45;

    // Members for instances of methods in runs
    this.status = status;
    this.log_id = log_id;

    this.in_magnets = [];
    var sorted_in_keys = Object.keys(this.inputs).sort(function(a,b){return a-b});
    for (var keyIndex in sorted_in_keys) {
        var key = sorted_in_keys[keyIndex],
            this_input = this.inputs[key],
            magnet = new Magnet(
                parent = this,
                r = 5,
                attract = 5,
                fill = '#fff',
                cdt = this_input['cdt_pk'],
                label = this_input['datasetname'],
                null,
                false
            );

        if (this.n_inputs == 1) {
            magnet.x -= this.h/3
        }

        this.in_magnets.push(magnet);
    }

    this.out_magnets = [];
    var sorted_out_keys = Object.keys(outputs).sort(function(a,b){return a-b});
    for (keyIndex in sorted_out_keys) {
        var key = sorted_out_keys[keyIndex],
            this_output = this.outputs[key],
            magnet = new Magnet(
                parent = this,
                r = 5,
                attract = 5,
                fill = '#fff',
                cdt = this_output['cdt_pk'],
                label = this_output['datasetname'],
                null,
                true
            );

        if (this.n_inputs == 1) {
            magnet.x += this.h/3
        }

        this.out_magnets.push(magnet);
    }
}

MethodNode.prototype.draw = function(ctx) {
    ctx.fillStyle = this.fill;
    var vertices = this.getVertices();
    this.vertices = vertices;
    ctx.beginPath();
    
    // body
    ctx.moveTo( vertices[4].x, vertices[4].y );
    ctx.lineTo( vertices[5].x, vertices[5].y );
    ctx.lineTo( vertices[6].x, vertices[6].y );
    ctx.bezierCurveTo( vertices[10].x, vertices[10].y, vertices[10].x, vertices[10].y, vertices[1].x, vertices[1].y );
    ctx.lineTo( vertices[2].x, vertices[2].y );
    ctx.lineTo( vertices[3].x, vertices[3].y );
    ctx.bezierCurveTo( vertices[8].x, vertices[8].y, vertices[8].x, vertices[8].y, vertices[4].x, vertices[4].y );
    ctx.fill();
    
    // input plane (shading)
    ctx.beginPath();
    ctx.moveTo( vertices[0].x, vertices[0].y );
    ctx.lineTo( vertices[1].x, vertices[1].y );
    ctx.lineTo( vertices[2].x, vertices[2].y );
    ctx.lineTo( vertices[3].x, vertices[3].y );
    ctx.fillStyle = '#fff';
    ctx.globalAlpha = 0.35;
    ctx.fill();
    
    // top bend (shading)
    ctx.beginPath();
    ctx.moveTo( vertices[6].x, vertices[6].y );
    ctx.lineTo( vertices[7].x, vertices[7].y );
    ctx.bezierCurveTo( vertices[9].x,  vertices[9].y,  vertices[9].x,  vertices[9].y,  vertices[0].x, vertices[0].y );
    ctx.lineTo( vertices[1].x, vertices[1].y );
    ctx.bezierCurveTo( vertices[10].x, vertices[10].y, vertices[10].x, vertices[10].y, vertices[6].x, vertices[6].y );
    ctx.globalAlpha = 0.12;
    ctx.fill();
    
    ctx.fillStyle = this.fill;
    ctx.globalAlpha = 1.0;
    
/*  // output plane
    ctx.moveTo( vertices[4].x, vertices[4].y );
    ctx.lineTo( vertices[5].x, vertices[5].y );
    ctx.lineTo( vertices[6].x, vertices[6].y );
    ctx.lineTo( vertices[7].x, vertices[7].y );

    // side bend
    ctx.moveTo( vertices[4].x, vertices[4].y );
    ctx.lineTo( vertices[7].x, vertices[7].y );
    ctx.bezierCurveTo( vertices[9].x, vertices[9].y, vertices[9].x, vertices[9].y, vertices[0].x, vertices[0].y );
    ctx.lineTo( vertices[3].x, vertices[3].y );
    ctx.bezierCurveTo( vertices[8].x, vertices[8].y, vertices[8].x, vertices[8].y, vertices[4].x, vertices[4].y );
    */

    // draw magnets
    var cx = this.x + this.dx,
        cy = this.y + this.dy,
        cos30 = Math.sqrt(3)/2,
     // sin30 = 0.5 (this is trivial)
        magnet_margin = 6,
        y_inputs = cy - this.stack,
        x_outputs = cx + this.scoop * cos30,
        y_outputs = cy + this.scoop * .5,
        c2c = this.in_magnets[0].r * 2 + magnet_margin,
        ipl  = (this.in_magnets.length  * c2c + magnet_margin) / 2;// distance from magnet centre to edge

    this.input_plane_len = ipl;
    
    for (var i = 0, len = this.in_magnets.length; i < len; i++) {
        magnet = this.in_magnets[i];
        var pos = i - len/2 + .5;
        magnet.x = cx + pos * cos30 * c2c;
        magnet.y = y_inputs - pos * c2c/2;
        magnet.draw(ctx);
    }
    for (i = 0, len = this.out_magnets.length; i < len; i++) {
        magnet = this.out_magnets[i];
        var pos = i - len/2 + .5;
        magnet.x = x_outputs + pos * cos30 * c2c;
        magnet.y = y_outputs - pos * c2c/2;
        magnet.draw(ctx);
    }

    // Highlight the method based on status.
    if(typeof this.status === 'string') {
        ctx.save();

        ctx.strokeStyle = _statusColorMap[this.status] || 'black';
        ctx.lineWidth = 5;

        ctx.globalCompositeOperation = 'destination-over';

        // body
        ctx.beginPath();
        ctx.moveTo( vertices[4].x, vertices[4].y );
        ctx.lineTo( vertices[5].x, vertices[5].y );
        ctx.lineTo( vertices[6].x, vertices[6].y );
        ctx.bezierCurveTo( vertices[10].x, vertices[10].y, vertices[10].x, vertices[10].y, vertices[1].x, vertices[1].y );
        ctx.lineTo( vertices[2].x, vertices[2].y );
        ctx.lineTo( vertices[3].x, vertices[3].y );
        ctx.bezierCurveTo( vertices[8].x, vertices[8].y, vertices[8].x, vertices[8].y, vertices[4].x, vertices[4].y );
        ctx.closePath();

        ctx.stroke();
        ctx.restore();
    }
};

MethodNode.prototype.highlight = function(ctx, dragging) {
    // highlight this node shape
    var vertices = this.getVertices();
    ctx.globalCompositeOperation = 'destination-over';

    // body
    ctx.beginPath();
    ctx.moveTo( vertices[4].x, vertices[4].y );
    ctx.lineTo( vertices[5].x, vertices[5].y );
    ctx.lineTo( vertices[6].x, vertices[6].y );
    ctx.bezierCurveTo( vertices[10].x, vertices[10].y, vertices[10].x, vertices[10].y, vertices[1].x, vertices[1].y );
    ctx.lineTo( vertices[2].x, vertices[2].y );
    ctx.lineTo( vertices[3].x, vertices[3].y );
    ctx.bezierCurveTo( vertices[8].x, vertices[8].y, vertices[8].x, vertices[8].y, vertices[4].x, vertices[4].y );
    ctx.closePath();
    
    ctx.stroke();
    ctx.globalCompositeOperation = 'source-over';
    
    // Any output nodes will also be highlighted.
    var magnet, connected_node, i, j;
    for (i = 0; i < this.out_magnets.length; i++) {
        magnet = this.out_magnets[i];
        for (j = 0; j < magnet.connected.length; j++) {
            connected_node = magnet.connected[j].dest.parent;
            if (connected_node.constructor == OutputNode) {
                connected_node.highlight(ctx);
            }
            
            // Draw label on cable.
            magnet.connected[j].drawLabel(ctx);
        }
        
        if (magnet.connected.length === 0) {
            // Highlight (label) the magnet
            magnet.highlight(ctx);
        }
    }
    for (var i = 0; i < this.in_magnets.length; i++) {
        magnet = this.in_magnets[i];
        if (magnet.connected.length === 0) {
            magnet.highlight(ctx);
        } else {
            magnet.connected[0].drawLabel(ctx);
        }
    }
}

MethodNode.prototype.contains = function(mx, my) {
    var vertices = this.getVertices();
    var polygon = [ 1,2,3,8,4,5,6,10 ];
    var shape = [];
    
    for ( var i=0; i < polygon.length; i++ )
        shape.push(vertices[polygon[i]]);
    return Geometry.inPolygon(mx, my, shape);
};

MethodNode.prototype.getVertices = function() {
    var cx = this.x + this.dx,
        cy = this.y + this.dy;
    
    // experimental draw
    var cos30 = Math.sqrt(3)/2,
     // sin30 = 0.5 (this is trivial)
        magnet_radius = this.in_magnets[0].r, 
        magnet_margin = 6,
        dmc = magnet_radius + magnet_margin,// distance from magnet centre to edge
        c2c = dmc + magnet_radius,//centre 2 centre of adjacent magnets
        cosdmc = cos30 * dmc,
        ipy = cy - this.stack,
        input_plane_len  = (this.n_inputs * c2c + magnet_margin) / 2,
        cosipl = cos30 * input_plane_len;
    
    if (typeof this.vertices == 'undefined' 
        || this.vertices[1].x != cx + cosdmc + cosipl 
        || this.vertices[1].y != ipy + (dmc - input_plane_len) / 2
        ) {
        
        var opx = cx + this.scoop * cos30,
            opy = cy + this.scoop * .5,
            output_plane_len = (this.n_outputs * c2c + magnet_margin) / 2,
            cosopl = cos30 * output_plane_len; // half of the length of the parallelogram ("half hypoteneuse")
    
        var vertices = [
            { x: cx + cosdmc - cosipl, y: ipy + (dmc + input_plane_len) / 2 },
            { x: cx + cosdmc + cosipl, y: ipy + (dmc - input_plane_len) / 2 },
            { x: cx - cosdmc + cosipl, y: ipy - (dmc + input_plane_len) / 2 },
            { x: cx - cosdmc - cosipl, y: ipy - (dmc - input_plane_len) / 2 },
            { x: opx - cosopl, y: opy + dmc + output_plane_len / 2 },
            { x: opx + cosopl, y: opy + dmc - output_plane_len / 2 },
            { x: opx + cosopl, y: opy - dmc - output_plane_len / 2 },
            { x: opx - cosopl, y: opy - dmc + output_plane_len / 2 }
        ];
    
        if (this.in_magnets.length > this.out_magnets.length) {
            vertices.push(
                { x: cx - cosdmc - cosopl, y: cy + (dmc + output_plane_len) / 2 },
                { x: cx + cosdmc - cosopl, y: cy - (dmc - output_plane_len) / 2 },
                { x: cx + cosdmc + cosopl, y: cy - (dmc + output_plane_len) / 2 }
    //            { x: cx + cosdmc - cosopl, y: cy + dmc * 1.5 + output_plane_len / 2 },
    //            { x: cx + cosdmc + cosopl, y: cy + dmc * 1.5 - output_plane_len / 2 },
    //            { x: cx - cosdmc + cosopl, y: cy + (dmc - output_plane_len) / 2 },
    //            { x: cx - cosdmc + cosopl, y: cy - dmc * 1.5 - output_plane_len / 2 },
    //            { x: cx - cosdmc - cosopl, y: cy - dmc * 1.5 + output_plane_len / 2 }
            );
        } else { 
            vertices.push(
                { x: cx - cosdmc - cosipl, y: cy + cosdmc - (dmc - input_plane_len) / 2 },
                { x: cx + cosdmc - cosipl, y: cy - cosdmc + (dmc + input_plane_len) / 2 },
                { x: cx + cosdmc + cosipl, y: cy - cosdmc + (dmc - input_plane_len) / 2 }
    //            { x: cx + cosdmc - cosipl, y: cy + cosdmc + (dmc + input_plane_len) / 2 },
    //            { x: cx + cosdmc + cosipl, y: cy + cosdmc + (dmc - input_plane_len) / 2 },
    //            { x: cx - cosdmc + cosipl, y: cy + cosdmc - (dmc + input_plane_len) / 2 },
    //            { x: cx - cosdmc + cosipl, y: cy - cosdmc - (dmc + input_plane_len) / 2 },
    //            { x: cx - cosdmc - cosipl, y: cy - cosdmc - (dmc - input_plane_len) / 2 }
            );
        }
        
        this.vertices = vertices;
    }
    
    return this.vertices;
};

MethodNode.prototype.getLabel = function() {
    return new NodeLabel(this.label, this.x + this.dx + this.scoop/4, this.y + this.dy - this.stack - this.input_plane_len/2 - this.offset);
};

drydock_objects = (function(my) {
    "use strict";
    my.Magnet = function(parent, r, attract, fill, cdt, label, offset, isOutput) {
        /*
        CONSTRUCTOR
        A Magnet is the attachment point for a Node (shape) given a
        Connector.  It is always contained within a shape.
        x and y coordinates will be set by parent object draw().
         */
        this.parent = parent;  // the containing shape
        this.x = null;
        this.y = null;
        this.r = r; // radius
        this.attract = attract; // radius of Connector attraction
        this.fill = fill || "#fff";
        this.cdt = cdt; // primary key to CDT
        this.label = label || '';
        this.offset = offset || 5;
        this.isOutput = isOutput || false;
        this.isInput = !this.isOutput;
        this.connected = [];  // hold references to Connectors
        this.acceptingConnector = false; // true if a connector is being dragged
    };

    my.Magnet.prototype.draw = function(ctx) {
        // magnet coords are set by containing shape
        var canvas = new my.CanvasWrapper(undefined, ctx);
        ctx.fillStyle = this.fill;
        canvas.drawCircle({x: this.x, y: this.y, r: this.r});
        
        if (this.acceptingConnector)
            this.highlight(ctx);
    };

    my.Magnet.prototype.highlight = function(ctx) {
        // draw label
        var dir = -1;
    
        ctx.save();
        ctx.translate(this.x, this.y);
        if (this.isOutput) {
            dir = 1;
            var angle = Math.PI/6;
            
            /* 
             *   I'm not sold on this display method. Commented out for now. —jn
             *
            sin_ = Math.sin(angle), 
            cos_ = Math.cos(angle);
             *   isometric perspective transform
             *   rotate(ϑ), shear(ϑ, 1), scale(1, cosϑ)
             *
             *   Affine transformation:
             *   cosϑ -sinϑ  0     1  ϑ  0     1   0   0
             *   sinϑ  cosϑ  0  ×  0  1  0  ×  0  cosϑ 0
             *    0     0    1     0  0  1     0   0   1
             *   
             *   Final product:
             *   cosϑ  cosϑ(ϑcosϑ - sinϑ)   0
             *   sinϑ  cosϑ(ϑsinϑ + cosϑ)   0
             *   0            0             1
             */
            
    //        ctx.transform(cos_, sin_, cos_*(angle*cos_ - sin_), cos_*(angle*sin_ + cos_), 0, 0);
            ctx.rotate(angle);
        }
        
        ctx.fillStyle = '#fff';
        new my.CanvasWrapper(undefined, ctx).drawText({
            x: dir * (this.r + this.offset),
            y: 0,
            text: this.label,
            dir: dir});
    
        ctx.restore();
    };

    my.Magnet.prototype.contains = function(mx, my) {
        var dx = this.x - mx;
        var dy = this.y - my;
        return Math.sqrt(dx*dx + dy*dy) <= this.r + this.attract;
    };
    
    my.Connector = function(out_magnet) {
        /*
        Constructor.
        A Connector is a line drawn between two Magnets.
        Actions:
        - on mouse down in raw data end-zone OR an out-magnet:
            - push new Connector to list
            - assign out-magnet CDT
            - else assign x, y in end-zone (TODO: zip to other coords?)
        - on mouse move
            - update line with mouse move
            - light up all CDT-matched in-magnets
            - if mouse in vicinity of CDT-matched in-magnet, jump to magnet
        - on mouse up
            - if mouse NOT on CDT-matched in-magnet, delete Connector
         */
        this.dest = null;
        this.source = out_magnet;
    
        this.fromX = out_magnet.x;
        this.fromY = out_magnet.y;
    
        this.x = this.from_x; // for compatibility with shape-based functions
        this.y = this.from_y;
            
    }
    
    my.Connector.prototype.draw = function(ctx) {
        /*
        Draw a line to represent a Connector originating from a Magnet.
         */
        var canvas = new my.CanvasWrapper(undefined, ctx);
        this.calculateCurve();
        ctx.strokeStyle = '#abc';
        ctx.lineWidth = 6;
        ctx.lineCap = 'round';
        
        if (this.dest === null) {
            // if connector doesn't have a destination yet,
            // give it the label of the source magnet it's coming from 
            
            // save the canvas state to start applying transformations
            ctx.save();
            ctx.fillStyle = '#aaa';
            canvas.drawText({
                x: this.x + 2,
                y: this.y + 5,
                text: this.source.label,
                dir: 1,
                style: "connector"});
            ctx.restore();
            $(ctx.canvas).css("cursor", "none");
        }
        else {
            // Recolour this path if the statuses of the source and dest are meaningful
            var src = this.source.parent, 
                dst = this.dest.parent, 
                cable_stat;
    
            if (typeof src.status === 'string') {
                cable_stat = "RUNNING";
    
                // Upper cable is done!
               if(src.status == 'CLEAR' && typeof dst.status == 'string' ) {
                    // Whatever, everything else is fine!
                    cable_stat = "CLEAR";
                }
    
                // Source is borked
                else if(src.status == 'FAILURE') {
                    // so is any cable that pokes out of it...
                    cable_stat = "FAILURE";
                }
            }
    
            if(_statusColorMap.hasOwnProperty(cable_stat)) {
                 ctx.strokeStyle = _statusColorMap[cable_stat];
            }
        }

        ctx.beginPath();
        ctx.moveTo(this.fromX, this.fromY);
        ctx.bezierCurveTo(
                this.ctrl1.x, this.ctrl1.y,
                this.ctrl2.x, this.ctrl2.y,
                this.x, this.y);
        ctx.stroke();
    };
    
    my.Connector.prototype.highlight = function(ctx) {
        /*
        Highlight this Connector by drawing another line along
        its length. Colour and line width set by canvasState.
         */
        this.calculateCurve();
        ctx.beginPath();
        ctx.moveTo(this.fromX, this.fromY);
        ctx.bezierCurveTo(this.ctrl1.x, this.ctrl1.y, this.ctrl2.x, this.ctrl2.y, this.x, this.y);
        ctx.stroke();
        
        if (this.dest !== null) {
            this.drawLabel(ctx);
        }
    }
    
    // make an object in the format of jsBezier lib
    my.Connector.prototype.calculateCurve = function() {
        if (this.dest !== null) {
            // move with the attached shape
            this.x = this.dest.x;
            this.y = this.dest.y;
        }
        if (this.ctrl1 === undefined ||
                this.fromX !== this.source.x ||
                this.fromY !== this.source.y ||
                this.x !== this.prevX ||
                this.y !== this.prevY) {
            // Either the curve has never been calculated or an end moved.
            this.fromX = this.source.x;
            this.fromY = this.source.y;
            this.prevX = this.x;
            this.prevY = this.y;
            
            this.dx = this.x - this.fromX,
            this.dy = this.y - this.fromY;
            
            this.ctrl1 = {
                    /*
            - Comes from origin at a 30-degree angle
            - cos(30) = sqrt(3)/2  &  sin(30) = 0.5
            - Distance of ctrl from origin is 70% of dx
            - Minimum dx is 50, so minimum distance of ctrl is 35.
                     */
                    x: this.fromX + Math.max(this.dx, 50) * Math.sqrt(3) / 2 * .7,
                    y: this.fromY + Math.max(this.dx, 50) / 2 * .7
            };
            this.ctrl2 = {
                    /*
            - Vertical offset is 2/3 of dy, or 2/5 of -dy, whichever is positive
            - Minimum vertical offset is 50
            - Horizontal offset is 10% of dx
                     */
                    x: this.x - this.dx / 10,
                    y: this.y - Math.max( (this.dy > 0 ? 1 : -.6) * this.dy, 50) / 1.5
            };
        }
        return [
            { x: this.fromX,   y: this.fromY   },
            this.ctrl1, this.ctrl2,
            { x: this.x,       y: this.y       }
        ];
    };
    
    my.Connector.prototype.drawLabel = function(ctx) {
        var jsb = this.calculateCurve(),
            label = this.source.label,
            canvas = new my.CanvasWrapper(undefined, ctx);
        if (this.source.label !== this.dest.label) {
            label += "->" + this.dest.label;
        }
        
        this.label_width = ctx.measureText(label).width + 10;
        this.dx = this.x - this.fromX,
        this.dy = this.y - this.fromY;
        
        if ( this.dx * this.dx + this.dy * this.dy > this.label_width * this.label_width / .49) {
            // determine the angle of the bezier at the midpoint
            var midpoint = jsBezier.nearestPointOnCurve(
                    { x: this.fromX + this.dx/2, y: this.fromY + this.dy/2 },
                    jsb),
                midpointAngle = jsBezier.gradientAtPoint(jsb, midpoint.location);
            
            // save the canvas state to start applying transformations
            ctx.save();
            
            // set the bezier midpoint as the origin
            ctx.translate(midpoint.point.x, midpoint.point.y);
            ctx.rotate(midpointAngle);
            ctx.fillStyle = '#aaa';
            
            canvas.drawText(
                    {x: 0, y: 0, dir: 0, text: label, style: "connector"});
            ctx.restore();
        }
    }
    
    my.Connector.prototype.debug = function(ctx) {
        var jsb = this.calculateCurve(),
            midpoint = jsBezier.nearestPointOnCurve({ x: this.fromX + this.dx/2, y: this.fromY + this.dy/2 }, jsb),
            midpointAngle = jsBezier.gradientAtPoint(jsb, midpoint.location),
            wrong_midpoint = jsBezier.pointOnCurve(jsb, 0.5);
        
        ctx.fillStyle = '#000';
        ctx.beginPath();
        ctx.arc(this.ctrl1.x, this.ctrl1.y, 5, 0, 2 * Math.PI, true);
        ctx.arc(this.ctrl2.x, this.ctrl2.y, 5, 0, 2 * Math.PI, true);
        ctx.fill();
        
        ctx.fillStyle = '#ff0';
        ctx.beginPath();
        ctx.arc(wrong_midpoint.x, wrong_midpoint.y, 5, 0, 2 * Math.PI, true);
        ctx.fill();
        
        var atan_bez = function(pts) {
            var lin_dy = pts[1].y - pts[0].y,
                lin_dx = pts[1].x - pts[0].x;
            return Math.atan(lin_dy / lin_dx);
        };
        
        var quadPt = [];
        ctx.fillStyle = '#600';
        for (var i=0; i+1 < jsb.length; i++) {
            var quadMid = {
                x: (jsb[i+1].x - jsb[i].x) * midpoint.location + jsb[i].x,
                y: (jsb[i+1].y - jsb[i].y) * midpoint.location + jsb[i].y
            };
            quadPt.push(quadMid);
            ctx.beginPath();
            ctx.arc(quadMid.x, quadMid.y, 5, 0, 2 * Math.PI, true);
            ctx.fill();
        }
        console.group('quadratic tangents');
        for (i=0; i < quadPt.length - 1; i++)
            console.log(atan_bez(quadPt.slice(i, i+2)) * 180/Math.PI);
        console.groupEnd();
        
        var linPt = [];
        ctx.fillStyle = '#A00';
        for (i=0; i+1 < quadPt.length; i++) {
            var linMid = {
                x: (quadPt[i+1].x - quadPt[i].x) * midpoint.location + quadPt[i].x,
                y: (quadPt[i+1].y - quadPt[i].y) * midpoint.location + quadPt[i].y
            };
            linPt.push(linMid);
            ctx.beginPath();
            ctx.arc(linMid.x, linMid.y, 5, 0, 2 * Math.PI, true);
            ctx.fill();
        }
        
        var pt = [];
        ctx.fillStyle = '#F00';
        for (i=0; i+1 < linPt.length; i++) {
            var mid = {
                x: (linPt[i+1].x - linPt[i].x) * midpoint.location + linPt[i].x,
                y: (linPt[i+1].y - linPt[i].y) * midpoint.location + linPt[i].y
            };
            pt.push(mid);
            ctx.beginPath();
            ctx.arc(mid.x, mid.y, 5, 0, 2 * Math.PI, true);
            ctx.fill();
        }
        
        var final_tangent = atan_bez(linPt);
        
        console.group('linPt');
        console.log('linPt[0]: '+ linPt[0].x + ', ' + linPt[0].y);
        console.log('linPt[1]: '+ linPt[1].x + ', ' + linPt[1].y);
        console.groupEnd();
    
        console.group('connector debug');
        console.log('midpoint location: '+ midpoint.location);
        console.log('midpoint coord: '+ midpoint.point.x + ', ' + midpoint.point.y);
        console.log('jsbez angle: '+ ( midpointAngle * 180/Math.PI) );
        console.log('calculated angle: ' + (final_tangent * 180/Math.PI) );
        console.groupEnd();
        
        ctx.fillStyle = '#0f0';
        ctx.beginPath();
        ctx.arc(midpoint.point.x, midpoint.point.y, 3, 0, 2 * Math.PI, true);
        ctx.fill();
    };
    
    my.Connector.prototype.contains = function(mx, my, pad) {
        // Uses library jsBezier to accomplish certain tasks.
        // Since precise bezier distance is expensive to compute, we start by
        // running a faster algorithm to see if mx,my is outside the rectangle
        // given by the beginning, end, and control points (plus padding).
        this.calculateCurve();
        var ys = [ this.y, this.fromY, this.ctrl1.y, this.ctrl2.y ],
            xs = [ this.x, this.fromX, this.ctrl1.x, this.ctrl2.x ],
            bottom = Math.max.apply(null, ys),
            top = Math.min.apply(null, ys),
            right = Math.max.apply(null, xs),
            left = Math.min.apply(null, xs);
        
        if (mx > left - pad && mx < right + pad
            && my > top - pad && my < bottom + pad
            ) {
            // expensive route: run bezier distance algorithm
            return pad > 
                jsBezier.distanceFromCurve(
                    { x: mx, y: my }, 
                    this.calculateCurve()
                ).distance;
        }
        // mx,my is outside the rectangle, don't bother computing the bezier distance
        else return false;
    };
    
    // TODO: Convert the whole file to this module, then combine all the sections.
    return my;
}(drydock_objects));
var Magnet = drydock_objects.Magnet;
var Connector = drydock_objects.Connector;

function NodeLabel (label, x, y) {
    this.label = label || '';
    this.x = x || 0;
    this.y = y || 0;
}

function OutputZone (cw, ch, inset) {
    this.x = cw * .82;
    this.w = cw * .175;
    this.h = this.w;
    this.y = 1;
    
    while (this.h + this.y > ch) {
        this.h /= 1.5;
    }
    
    this.inset = inset || 15; // distance of label from center
}

OutputZone.prototype.draw = function (ctx) {
    // draw output zone
    ctx.fillStyle = this.fill;
    
    ctx.beginPath();
    ctx.strokeStyle = "#aaa";
    ctx.setLineDash([5]);
    ctx.lineWidth = 1;
    ctx.rect(this.x, this.y, this.w, this.h);
    ctx.closePath();
    ctx.stroke();
    ctx.setLineDash([0]);

    // draw label
    ctx.fillStyle = '#aaa';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'alphabetic';
    ctx.font = 'bold 10pt Lato, sans-serif';
    ctx.fillText("Drag here to", this.x + this.w/2, this.y + this.inset);
    ctx.fillText("create an output", this.x + this.w/2, this.y + this.inset*2);
};

OutputZone.prototype.contains = function (mx, my) {
    return (
        mx >= this.x 
        && mx <= this.x + this.w 
        && my >= this.y 
        && my <= this.y + this.h
    );
};

drydock_objects = (function(my) {
    "use strict";
    my.OutputNode = function (x, y, label, pk, status, md5, dataset_id) {
        /*
        Node representing an output.
         */
        my.CylinderNode.call(this, x, y, label);
        this.fill = this.defaultFill = "#d40";
        this.diffFill = "blue";
        this.inset = 12; // distance of magnet from center
        this.in_magnets.push(new Magnet(this, 5, 2, "white", null, this.label));
        this.pk = pk;
        this.status = status;
        this.md5 = md5;
        this.dataset_id = dataset_id;
    
        // Marks whether or not this node
        // was being searched for and was found
        // (when doing an md5 lookup)
        this.found_md5 = false;
    }
    my.OutputNode.prototype = Object.create(my.CylinderNode.prototype);
    my.OutputNode.prototype.constructor = my.OutputNode;
    
    my.OutputNode.prototype.draw = function(ctx) {
        // Highlight the method based on status.
        if(typeof this.status === 'string') {
            this.highlightStroke = my.statusColorMap[this.status] || 'black';
        }
        else {
            this.highlightStroke = undefined;
        }
        
        this.fill = this.found_md5 ? this.diffFill : this.defaultFill;
        my.CylinderNode.prototype.draw.call(this, ctx);
    };
    
    my.OutputNode.prototype.highlight = function(ctx) {
        var cable = this.in_magnets[0].connected[0];
        
        my.CylinderNode.prototype.highlight.call(this, ctx);
        
        if (cable) {
            cable.highlight(ctx);
        }
    }

    my.OutputNode.prototype.debug = function(ctx) {
        this.in_magnets[0].connected[0].debug(ctx);
    };
    
    // TODO: Convert the whole file to this module, then combine all the sections.
    return my;
}(drydock_objects));
var OutputNode = drydock_objects.OutputNode;
