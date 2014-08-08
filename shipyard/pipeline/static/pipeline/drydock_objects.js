/**
 * drydock_objects.js
 *   JS prototypes that are used to populate canvasState
 *   (see drydock.js)
 */

var Geometry = {
    inEllipse: function(mx, my, cx, cy, rx, ry) {
        var dx = mx - cx, dy = my - cy;
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
    }
};

// Add ellipses to canvas element prototype.
CanvasRenderingContext2D.prototype.ellipse = function (cx, cy, rx, ry) {
    this.save(); // save state
    this.beginPath();

    this.translate(cx - rx, cy - ry);
    this.scale(rx, ry);
    this.arc(1, 1, 1, 0, 2 * Math.PI, false);

    this.restore(); // restore to original state
};

function RawNode (x, y, r, h, fill, inset, offset, label) {
    /*
    Node representing an unstructured (raw) datatype.
    Rendered as a circle.
     */
    this.x = x || 0; // defaults to top left corner
    this.y = y || 0;
    this.r = r || 10; // x-radius
    this.r2 = this.r/2; // y-radius
    this.w = this.r; // for compatibility
    this.h = h || 10; // stack height
    this.fill = fill || "#aaa";
    this.inset = inset || 5; // distance of magnet from center
    this.offset = offset || 12; // distance of label from center
    this.label = label || '';
    this.in_magnets = []; // for compatibility

    // CDT node always has one magnet
    var magnet = new Magnet(this, 5, 2, "white", null, this.label);
    this.out_magnets = [ magnet ];
}

RawNode.prototype.draw = function(ctx) {

    // draw bottom ellipse
    ctx.fillStyle = this.fill;
    ctx.ellipse(this.x, this.y + this.h/2, this.r, this.r2);
    ctx.fill();
    
    // draw stack 
    ctx.fillRect(this.x - this.r, this.y - this.h/2, this.r * 2, this.h);
    
    // draw top ellipse
    ctx.ellipse(this.x, this.y - this.h/2, this.r, this.r2);
    ctx.fill();
    
    // some shading
    ctx.fillStyle = '#fff';
    ctx.globalAlpha = 0.35;
    ctx.ellipse(this.x, this.y - this.h/2, this.r, this.r2);
    ctx.fill();
    ctx.globalAlpha = 1.0;

    // draw label
    ctx.fillStyle = 'black';
    ctx.textAlign = 'center';
    ctx.font = '10pt Lato, sans-serif';
    ctx.fillText(this.label, this.x, this.y - this.h/2 - this.offset);

    // draw magnet
    out_magnet = this.out_magnets[0];
    out_magnet.x = this.x + this.inset;
    out_magnet.y = this.y + this.r2/2;
    out_magnet.draw(ctx);
    
/*    // draw circle
    ctx.fillStyle = this.fill;
    ctx.beginPath();
    ctx.arc(this.x, this.y, this.r, 0, 2 * Math.PI, false);
    ctx.closePath();
    ctx.fill();

    // draw label
    ctx.fillStyle = 'black';
    ctx.textAlign = 'center';
    ctx.font = '10pt Lato, sans-serif';
    ctx.fillText(this.label, this.x, this.y - this.offset);

    // draw magnet
    out_magnet = this.out_magnets[0];
    out_magnet.x = this.x + this.inset;
    out_magnet.y = this.y;
    out_magnet.draw(ctx);*/
};

RawNode.prototype.highlight = function(ctx) {
    ctx.globalCompositeOperation = 'destination-over';
    
    // draw bottom ellipse
    ctx.ellipse(this.x, this.y + this.h/2, this.r, this.r2);
    ctx.stroke();
    
    // draw stack 
    ctx.strokeRect(this.x - this.r, this.y - this.h/2, this.r * 2, this.h);
    
    // draw top ellipse
    ctx.ellipse(this.x, this.y - this.h/2, this.r, this.r2);
    ctx.stroke();
    
    ctx.globalCompositeOperation = 'source-over';
}

RawNode.prototype.contains = function(mx, my) {
    // node is comprised of a rectangle and two ellipses
    return Geometry.inRectangleFromCentre(mx, my, this.x, this.y, this.r, this.h/2)
        || Geometry.inEllipse(mx, my, this.x, this.y - this.h/2, this.r, this.r2)
        || Geometry.inEllipse(mx, my, this.x, this.y + this.h/2, this.r, this.r2);
};


function CDtNode (pk, x, y, w, fill, inset, offset, label) {
    /*
    Node represents a Compound Datatype (CSV structured data).
    Rendered as a square shape.
     */
    this.pk = pk;
    this.x = x || 0;
    this.y = y || 0;
    this.w = w || 20;
    this.fill = fill || "#AAAAAA";
    this.inset = inset || 5;
    this.offset = offset || 12;
    this.label = label || '';
    this.in_magnets = [];

    var magnet = new Magnet(this, 5, 2, "white", this.pk, this.label);
    this.out_magnets = [ magnet ];
}

CDtNode.prototype.draw = function(ctx) {
    // draw square
    ctx.fillStyle = this.fill;
    ctx.fillRect(this.x, this.y, this.w, this.w);

    // draw label
    ctx.fillStyle = 'black';
    ctx.textAlign = 'center';
    ctx.font = '10pt Lato, sans-serif';
    ctx.fillText(this.label, this.x + this.w/2., this.y - this.offset);

    // draw magnet
    out_magnet = this.out_magnets[0];
    out_magnet.x = this.x + this.w - this.inset;
    out_magnet.y = this.y + this.w/2.;
    out_magnet.draw(ctx);
};

CDtNode.prototype.highlight = function(ctx) {
    ctx.globalCompositeOperation = 'destination-over';
    ctx.beginPath();
    ctx.moveTo(this.x, this.y);
    ctx.lineTo(this.x+this.w, this.y);
    ctx.lineTo(this.x+this.w, this.y+this.w);
    ctx.lineTo(this.x, this.y+this.w);
    ctx.closePath();
    ctx.stroke();
    ctx.globalCompositeOperation = 'source-over';
};


CDtNode.prototype.contains = function(mx, my) {
    /*
    Are mouse coordinates within the perimeter of this node?
     */
    return this.x <= mx && this.x + this.w >= mx && this.y <= my && this.y + this.w >= my;
};


function MethodNode (pk, x, y, w, inset, spacing, fill, label, offset, inputs, outputs) {
    /*
    CONSTRUCTOR
    A MethodNode is a rectangle of constant width (w) and varying height (h)
    where h is proportional to the maximum number of xputs (inputs or outputs).
    h = max(n_inputs, n_ouputs) * spacing
    Holes for inputs and outputs are drawn at some (inset) into the left
    and right sides, respectively.  The width must be greater than 2 * inset.
    */
    this.pk = pk;
    this.family = null; // can be passed from database

    this.x = x || 0;
    this.y = y || 0;
    this.w = w || 10;
    this.inputs = inputs;
    this.outputs = outputs;

    this.n_inputs = Object.keys(inputs).length;
    this.n_outputs = Object.keys(outputs).length;

    this.inset = inset || 10; // distance from left or right edge to center of hole
    this.offset = offset || 10; // space between bottom of node and label

    this.spacing = spacing || 10; // vertical separation between pins
    this.h = Math.max(this.n_inputs, this.n_outputs) * this.spacing;
    this.fill = fill || "#ddd";
    this.label = label || '';

    this.in_magnets = [];
    for (var key in this.inputs) {
        var this_input = this.inputs[key];
        var magnet = new Magnet(
            parent = this,
            r = 5,
            attract = 5,
            fill = '#fff',
            cdt = this_input['cdt_pk'],
            label = this_input['datasetname']
        );

        if (this.n_inputs == 1) {
            magnet.x -= this.h/3
        }

        this.in_magnets.push(magnet);
    }

    this.out_magnets = [];
    for (key in this.outputs) {
        var this_output = this.outputs[key];
        magnet = new Magnet(
            parent = this,
            r = 5,
            attract = 5,
            fill = '#fff',
            cdt = this_output['cdt_pk'],
            label = this_output['datasetname']
        );

        if (this.n_inputs == 1) {
            magnet.x += this.h/3
        }

        this.out_magnets.push(magnet);
    }
}

MethodNode.prototype.draw = function(ctx) {
    // draw rectangle
    ctx.fillStyle = this.fill;
//    ctx.fillRect(this.x, this.y, this.w, this.h);

    // draw a hexagon
    var hx, hy;
    ctx.beginPath();
    ctx.moveTo(hx = this.x, hy = this.y);
    ctx.lineTo(hx += this.w, hy);
    ctx.lineTo(hx += this.h/4, hy += this.h/2);
    ctx.lineTo(hx -= this.h/4, hy += this.h/2);
    ctx.lineTo(hx = this.x, hy);
    //ctx.lineTo(hx - this.h/3, hy - this.h/2);
    ctx.closePath();
    ctx.fill();

    // draw magnets
    for (var i = 0, len = this.in_magnets.length; i < len; i++) {
        magnet = this.in_magnets[i];
        magnet.x = this.x + this.inset;
        if (len == 1) {
            // Special case if there's only 1 input (or output).
            // I may rethink this later. —JN
            magnet.x -= this.h * .1;
        }
        magnet.y = this.y + this.h/2 + this.spacing * (i - len/2 + .5);
        magnet.draw(ctx);
    }
    for (i = 0, len = this.out_magnets.length; i < len; i++) {
        magnet = this.out_magnets[i];
        magnet.x = this.x + this.w - this.inset;
        if (len == 1) {
            magnet.x += this.h * .1;
        }
        magnet.y = this.y + this.h/2 + this.spacing * (i - len/2 + .5);
        magnet.draw(ctx);
    }

    // draw label
    ctx.fillStyle = '#000';
    ctx.textAlign = 'center';
    ctx.font = '10pt Lato, sans-serif';
    ctx.fillText(this.label, this.x + this.w / 2, this.y - this.offset);
};

MethodNode.prototype.highlight = function(ctx, dragging) {
    // highlight this node shape
    var hx, hy;
    ctx.globalCompositeOperation = 'destination-over';
    ctx.beginPath();
    ctx.moveTo(hx = this.x, hy = this.y);
    ctx.lineTo(hx += this.w, hy);
    ctx.lineTo(hx += this.h/4, hy += this.h/2);
    ctx.lineTo(hx -= this.h/4, hy += this.h/2);
    ctx.lineTo(hx = this.x, hy);
    ctx.closePath();
    ctx.stroke();
    ctx.globalCompositeOperation = 'source-over';
}

MethodNode.prototype.contains = function(mx, my) {
    return this.x <= mx 
        && this.x + this.w >= mx 
        && this.y <= my 
        && this.y + this.h >= my;
};


function Magnet (parent, r, attract, fill, cdt, label) {
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
    this.connected = [];  // hold references to Connectors
}

Magnet.prototype.draw = function(ctx) {
    // magnet coords are set by containing shape
    ctx.beginPath();
    ctx.arc(this.x, this.y, this.r, 0, 2 * Math.PI, true);
    ctx.closePath();
    ctx.fillStyle = this.fill;
    ctx.fill();
};

Magnet.prototype.contains = function(mx, my) {
    var dx = this.x - mx;
    var dy = this.y - my;
    return Math.sqrt(dx*dx + dy*dy) <= this.r + this.attract;
};


function Connector (from_x, from_y, out_magnet) {
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
    this.source = out_magnet || null;

    // is this Connector being drawn from an out-magnet?
    if (this.source == null) {
        // FIXME: currently this should never be the case - afyp
        this.fromX = from_x;
        this.fromY = from_y;
    } else {
        this.fromX = out_magnet.x;
        this.fromY = out_magnet.y;
    }

    this.x = this.from_x; // for compatibility with shape-based functions
    this.y = this.from_y;
    this.midX = this.fromX + (this.x - this.fromX) / 2;
}

Connector.prototype.draw = function(ctx) {
    /*
    Draw a line to represent a Connector originating from a Magnet.
     */
    ctx.strokeStyle = '#aaa';
    ctx.lineWidth = 6;
    ctx.lineCap = 'round';

    if (this.source !== null) {
        // update coordinates in case magnet has moved
        this.fromX = this.source.x;
        this.fromY = this.source.y;
    }

    if (this.dest !== null) {
        if (this.dest.constructor === Magnet) {
            // move with the attached shape
            this.x = this.dest.x;
            this.y = this.dest.y;
        }
    }
    
    this.midX = this.fromX + (this.x - this.fromX) / 2;
    
    ctx.beginPath();
    ctx.moveTo(this.fromX, this.fromY);
    ctx.bezierCurveTo(this.midX, this.fromY, this.midX, this.y, this.x, this.y);
    ctx.stroke();
};

Connector.prototype.highlight = function(ctx) {
    /*
    Highlight this Connector by drawing another line along
    its length. Colour and line width set by canvasState.
    
    (I removed this requirement - to me it makes more sense for the
     connector to be highlighted while dragging. -JN)
    ##Requires [dragging] to be false so Connector is not highlighted
    while it is being drawn.
    
     */
    ctx.beginPath();
    ctx.moveTo(this.fromX, this.fromY);
    ctx.bezierCurveTo(this.midX, this.fromY, this.midX, this.y, this.x, this.y);
    ctx.stroke();
}

Connector.prototype.contains = function(mx, my, pad) {
    // Uses library jsBezier to accomplish certain tasks.
    // Since precise bezier distance is expensive to compute, we start by
    // running a faster algorithm to see if mx,my is outside the rectangle
    // given by fromX,fromY,x,y (plus padding).

    // assume certain things about top/bottom/right/left
    var bottom = this.y,
        top = this.fromY,
        right = this.x,
        left = this.fromX;
    
    // now check if our assumptions were correct
    if (this.fromX > this.x) {
        left = this.x,
        right = this.fromX;
    }
    if (this.fromY > this.y) {
        top = this.y,
        bottom = this.fromY;
    }
    
    if (mx > left - pad && mx < right + pad
        && my > top - pad && my < bottom + pad
        ) {
        // expensive route: run bezier distance algorithm
        return pad > 
            jsBezier.quickDistFromCurve(
                mx, my, 
                this.fromX, this.fromY, 
                this.midX, 
                this.x, this.y
            ).distance;
    }
    // mx,my is outside the rectangle, don't bother computing the bezier distance
    else return false;

/*  Old code for straight line

    /*
    Determine if mouse coordinates (x,y) are on or close to this
    connector with coordinates (x1,y1) and (x2,y2).
    This is based on three criteria:
    (1) x1 < x < x2
    (2) y1 < y < y2
    (3) the distance of x,y to the line is below cutoff,
        see http://en.wikipedia.org/wiki/Distance_from_a_point_to_a_line
     * /
     
    var dx = this.x - this.fromX,
        dy = this.y - this.fromY;
        
    return (
        (this.x < mx) !== (this.fromX < mx)
        && (this.y < my) !== (this.fromY < my)
        && Math.abs(
            (
                dy * mx
                - dx * my
                + this.x * this.fromY
                - this.y * this.fromX
            )
            / Math.sqrt(dx * dx + dy * dy)
        ) < pad
    )*/
};

function OutputZone (cw, ch, inset) {
    this.x = cw * .8;
    this.w = cw * .15;
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

function OutputNode (x, y, r, h, fill, inset, offset, label) {
    /*
    Node representing an output.
    Rendered as a cylinder.
     */
    this.x = x || 0; // defaults to top left corner
    this.y = y || 0;
    this.r = r || 20; // x-radius (ellipse)
    this.r2 = this.r / 2; // y-radius (ellipse)
    this.w = this.r; // for compatibility
    this.h = h || 25; // height of cylinder
    this.fill = fill || "#aaa";
    this.inset = inset || 12; // distance of magnet from center
    this.offset = offset || 18; // distance of label from center
    this.label = label || '';
    this.out_magnets = []; // for compatibility

    // CDT node always has one magnet
    var magnet = new Magnet(this, 5, 2, "white", null, this.label);
    this.in_magnets = [ magnet ];
}

OutputNode.prototype.draw = function(ctx) {
    // draw bottom ellipse
    ctx.fillStyle = this.fill;
    ctx.ellipse(this.x, this.y + this.h/2, this.r, this.r2);
    ctx.fill();
    
    // draw stack 
    ctx.fillRect(this.x - this.r, this.y - this.h/2, this.r * 2, this.h);
    
    // draw top ellipse
    ctx.ellipse(this.x, this.y - this.h/2, this.r, this.r2);
    ctx.fill();
    
    // some shading
    ctx.fillStyle = '#fff';
    ctx.globalAlpha = 0.35;
    ctx.ellipse(this.x, this.y - this.h/2, this.r, this.r2);
    ctx.fill();
    ctx.globalAlpha = 1.0;

    // draw label
    ctx.fillStyle = 'black';
    ctx.textAlign = 'center';
    ctx.font = '10pt Lato, sans-serif';
    ctx.fillText(this.label, this.x, this.y - this.h/2 - this.offset);

    // draw magnet
    in_magnet = this.in_magnets[0];
    in_magnet.x = this.x - this.inset;
    in_magnet.y = this.y + this.r2/2;
    in_magnet.draw(ctx);
};

OutputNode.prototype.contains = function(mx, my) {
    // node is comprised of a rectangle and two ellipses
    return Math.abs(mx - this.x) < this.r
        && Math.abs(my - this.y) < this.h/2
        || Geometry.inEllipse(mx, my, this.x, this.y - this.h/2, this.r, this.r2)
        || Geometry.inEllipse(mx, my, this.x, this.y + this.h/2, this.r, this.r2);
};

OutputNode.prototype.highlight = function(ctx) {
    // This line means that we are drawing "behind" the canvas now.
    // We must set it back after we're done otherwise it'll be utter chaos.
    ctx.globalCompositeOperation = 'destination-over';
    
    // draw bottom ellipse
    ctx.ellipse(this.x, this.y + this.h/2, this.r, this.r2);
    ctx.stroke();
    
    // draw stack 
    ctx.strokeRect(this.x - this.r, this.y - this.h/2, this.r * 2, this.h);
    
    // draw top ellipse
    ctx.ellipse(this.x, this.y - this.h/2, this.r, this.r2);
    ctx.stroke();
    
    ctx.globalCompositeOperation = 'source-over';
    
    // The cable leading to the output is also selected.
    this.in_magnets[0].connected[0].highlight(ctx);
}