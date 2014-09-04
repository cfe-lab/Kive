/**
 * drydock_objects.js
 *   JS prototypes that are used to populate canvasState
 *   (see drydock.js)
 */

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
    this.r = r || 20; // x-radius
    this.r2 = this.r/2; // y-radius
    this.w = this.r; // for compatibility
    this.h = h || 25; // stack height
    this.fill = fill || "#8D8";
    this.inset = inset || 10; // distance of magnet from center
    this.offset = offset || 18; // distance of label from center
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

    // draw magnet
    out_magnet = this.out_magnets[0];
    out_magnet.x = this.x + this.inset;
    out_magnet.y = this.y + this.r2/2;
    out_magnet.draw(ctx);
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

RawNode.prototype.getVertices = function() {
    var x1 = this.x + this.r,
        x2 = this.x - this.r,
        y1 = this.y + this.h/2,
        y2 = y1 - this.h;
    
    return [
    { x: this.x, y: this.y },
        { x: x1, y: y1 },
        { x: x2, y: y1 },
        { x: x1, y: y2 },
        { x: x2, y: y2 },
        { x: this.x, y: this.y + this.h/2 + this.r2 },
        { x: this.x, y: this.y - this.h/2 - this.r2 }
    ];
};

RawNode.prototype.getLabel = function() {
    return new NodeLabel(this.label, this.x, this.y - this.h/2 - this.offset);
};


function CDtNode (pk, x, y, w, h, fill, inset, offset, label) {
    /*
    Node represents a Compound Datatype (CSV structured data).
    Rendered as a square shape.
     */
    this.pk = pk;
    this.x = x || 0;
    this.y = y || 0;
    this.w = w || 45;
    this.h = h || 28;
    this.fill = fill || "#88D";
    this.inset = inset || 13;
    this.offset = offset || 15;
    this.label = label || '';
    this.in_magnets = [];

    var magnet = new Magnet(this, 5, 2, "white", this.pk, this.label);
    this.out_magnets = [ magnet ];
}

CDtNode.prototype.draw = function(ctx) {
    ctx.fillStyle = this.fill;
    
    // draw base
    var prism_base = this.y + this.h/2;
    ctx.beginPath();
    ctx.moveTo(this.x - this.w/2, prism_base);
    ctx.lineTo(this.x, prism_base + this.w/4);
    ctx.lineTo(this.x + this.w/2, prism_base);
    ctx.lineTo(this.x, prism_base - this.w/4);
    ctx.closePath();
    ctx.fill();
    
    // draw stack 
    ctx.fillRect(this.x - this.w/2, this.y - this.h/2, this.w, this.h);
    
    // draw top
    var prism_cap = this.y - this.h/2;
    ctx.beginPath();
    ctx.moveTo(this.x - this.w/2, prism_cap);
    ctx.lineTo(this.x, prism_cap + this.w/4);
    ctx.lineTo(this.x + this.w/2, prism_cap);
    ctx.lineTo(this.x, prism_cap - this.w/4);
    ctx.closePath();
    ctx.fill();
    
    // some shading
    ctx.fillStyle = '#fff';
    ctx.globalAlpha = 0.35;
    ctx.fill();
    ctx.globalAlpha = 1.0;

    // draw magnet
    out_magnet = this.out_magnets[0];
    out_magnet.x = this.x + this.inset;
    out_magnet.y = this.y + this.w/8;
    out_magnet.draw(ctx);
};

CDtNode.prototype.getVertices = function() {
    var w2 = this.w/2,
        butt = this.y + this.h/2,
        cap  = this.y - this.h/2;
    
    return [
    { x: this.x,      y: this.y },
        { x: this.x - w2, y: butt },
        { x: this.x,      y: butt + w2/2 },
        { x: this.x + w2, y: butt },
        { x: this.x + w2, y: cap },
        { x: this.x,      y: cap - w2/2 },
        { x: this.x - w2, y: cap }
    ];
};

CDtNode.prototype.highlight = function(ctx) {
    ctx.globalCompositeOperation = 'destination-over';
//    ctx.lineJoin = 'bevel';
    
    var w2 = this.w/2,
        h2 = this.h/2,
        butt = this.y + h2,
        cap = this.y - h2;
    
    ctx.beginPath();
    ctx.moveTo(this.x - w2, butt);
    ctx.lineTo(this.x,      butt + w2/2);
    ctx.lineTo(this.x + w2, butt);
    ctx.lineTo(this.x + w2, cap);
    ctx.lineTo(this.x,      cap - w2/2);
    ctx.lineTo(this.x - w2, cap);
    ctx.closePath();
    ctx.stroke();
    
    ctx.globalCompositeOperation = 'source-over';
};


CDtNode.prototype.contains = function(mx, my) {
    /*
    Are mouse coordinates within the perimeter of this node?
     */
    var dx = Math.abs(this.x - mx),
        dy = Math.abs(this.y - my);
    
    // mouse coords are within the 4 diagonal lines.
    // can be checked with 1 expression because the 4 lines are mirror images of each other
    return this.h/2 + this.w/4 - dy > dx / 2 
    // then check the horizontal boundaries on the sides of the hexagon
        && dx < this.w/2;
};

CDtNode.prototype.getLabel = function() {
    return new NodeLabel(this.label, this.x, this.y - this.h/2 - this.offset);
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
    
    this.stack = 20;
    this.scoop = 45;

    this.in_magnets = [];
    for (var key in this.inputs) {
        var this_input = this.inputs[key];
        var magnet = new Magnet(
            parent = this,
            r = 5,
            attract = 5,
            fill = '#fff',
            cdt = this_input['cdt_pk'],
            label = this_input['datasetname'],
            null, false
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
            label = this_output['datasetname'],
            null, true
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
    var cos30 = Math.sqrt(3)/2,
     // sin30 = 0.5 (this is trivial)
        magnet_margin = 6,
        y_inputs = this.y - this.stack,
        x_outputs = this.x + this.scoop * cos30,
        y_outputs = this.y + this.scoop * .5,
        c2c = this.in_magnets[0].r * 2 + magnet_margin,
        ipl  = (this.in_magnets.length  * c2c + magnet_margin) / 2;// distance from magnet centre to edge

    this.input_plane_len = ipl;
    
    for (var i = 0, len = this.in_magnets.length; i < len; i++) {
        magnet = this.in_magnets[i];
        var pos = i - len/2 + .5;
        magnet.x = this.x + pos * cos30 * c2c;
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
    /*
    @todo
    Make this check more precise.
    */
    return mx < this.vertices[6].x && mx > this.vertices[3].x
        && !Geometry.ltLine(mx, my, this.vertices[1].x, this.vertices[1].y, this.vertices[2].x, this.vertices[2].y)
        && !Geometry.ltLine(mx, my, this.vertices[3].x, this.vertices[3].y, this.vertices[2].x, this.vertices[2].y)
        &&  Geometry.ltLine(mx, my, this.vertices[4].x, this.vertices[4].y, this.vertices[5].x, this.vertices[5].y)
        &&  Geometry.ltLine(mx, my, this.vertices[4].x, this.vertices[4].y, this.vertices[8].x, this.vertices[8].y);
};

MethodNode.prototype.getVertices = function() {
    // experimental draw
    var cos30 = Math.sqrt(3)/2,
     // sin30 = 0.5 (this is trivial)
        magnet_radius = this.in_magnets[0].r, 
        magnet_margin = 6,
        ipy = this.y - this.stack,
        opx = this.x + this.scoop * cos30,
        opy = this.y + this.scoop * .5,
        dmc = magnet_radius + magnet_margin,// distance from magnet centre to edge
        c2c = dmc + magnet_radius,//centre 2 centre of adjacent magnets
        cosdmc = cos30 * dmc,
        input_plane_len  = (this.in_magnets.length  * c2c + magnet_margin) / 2,
        output_plane_len = (this.out_magnets.length * c2c + magnet_margin) / 2,
        cosipl = cos30 * input_plane_len,
        cosopl = cos30 * output_plane_len; // half of the length of the parallelogram ("half hypoteneuse")
    
    var vertices = [
        { x: this.x + cosdmc - cosipl, y: ipy + (dmc + input_plane_len) / 2 },
        { x: this.x + cosdmc + cosipl, y: ipy + (dmc - input_plane_len) / 2 },
        { x: this.x - cosdmc + cosipl, y: ipy - (dmc + input_plane_len) / 2 },
        { x: this.x - cosdmc - cosipl, y: ipy - (dmc - input_plane_len) / 2 },
        { x: opx - cosopl, y: opy + dmc + output_plane_len / 2 },
        { x: opx + cosopl, y: opy + dmc - output_plane_len / 2 },
        { x: opx + cosopl, y: opy - dmc - output_plane_len / 2 },
        { x: opx - cosopl, y: opy - dmc + output_plane_len / 2 }
    ];
    
    if (this.in_magnets.length > this.out_magnets.length) {
        vertices.push(
            { x: this.x - cosdmc - cosopl, y: this.y + (dmc + output_plane_len) / 2 },
            { x: this.x + cosdmc - cosopl, y: this.y - (dmc - output_plane_len) / 2 },
            { x: this.x + cosdmc + cosopl, y: this.y - (dmc + output_plane_len) / 2 }
//            { x: this.x + cosdmc - cosopl, y: this.y + dmc * 1.5 + output_plane_len / 2 },
//            { x: this.x + cosdmc + cosopl, y: this.y + dmc * 1.5 - output_plane_len / 2 },
//            { x: this.x - cosdmc + cosopl, y: this.y + (dmc - output_plane_len) / 2 },
//            { x: this.x - cosdmc + cosopl, y: this.y - dmc * 1.5 - output_plane_len / 2 },
//            { x: this.x - cosdmc - cosopl, y: this.y - dmc * 1.5 + output_plane_len / 2 }
        );
    } else { 
        vertices.push(
            { x: this.x - cosdmc - cosipl, y: this.y + cosdmc - (dmc - input_plane_len) / 2 },
            { x: this.x + cosdmc - cosipl, y: this.y - cosdmc + (dmc + input_plane_len) / 2 },
            { x: this.x + cosdmc + cosipl, y: this.y - cosdmc + (dmc - input_plane_len) / 2 }
//            { x: this.x + cosdmc - cosipl, y: this.y + cosdmc + (dmc + input_plane_len) / 2 },
//            { x: this.x + cosdmc + cosipl, y: this.y + cosdmc + (dmc - input_plane_len) / 2 },
//            { x: this.x - cosdmc + cosipl, y: this.y + cosdmc - (dmc + input_plane_len) / 2 },
//            { x: this.x - cosdmc + cosipl, y: this.y - cosdmc - (dmc + input_plane_len) / 2 },
//            { x: this.x - cosdmc - cosipl, y: this.y - cosdmc - (dmc - input_plane_len) / 2 }
        );
    }
    
    return vertices;
};

MethodNode.prototype.getLabel = function() {
    return new NodeLabel(this.label, this.x + this.scoop/4, this.y - this.stack - this.input_plane_len/2 - this.offset);
};

function Magnet (parent, r, attract, fill, cdt, label, offset, isOutput) {
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
    this.offset = offset || 11;
    this.isOutput = isOutput || false;
    this.isInput = !this.isOutput;
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

Magnet.prototype.highlight = function(ctx) {
    // draw label
    ctx.font = '9pt Lato, sans-serif';
    ctx.textBaseline = 'middle';
    
    ctx.textAlign = 'right';
    var dir = -1;
    if (this.isOutput) {
        ctx.textAlign = 'left';
        dir = 1;
    }
    
    // make a backing box so the label is on white
    ctx.fillStyle = '#fff';
    ctx.globalAlpha = 0.5;
    ctx.fillRect(
        this.x + dir * (this.r + this.offset - 3),
        this.y - 7.5,
        dir * (ctx.measureText(this.label).width + 6),
        15
    );
    ctx.globalAlpha = 1.0;
    ctx.fillStyle = '#000';
    
    ctx.fillText(this.label, this.x + dir * (this.r + this.offset), this.y);
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
    } else {
        // if connector doesn't have a destination yet,
        // give it the label of the source magnet it's coming from 
        ctx.fillStyle = '#000';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'top';
        ctx.font = '10pt Lato, sans-serif';
        ctx.fillText(this.source.label, this.x, this.y + ctx.lineWidth );
    }
    
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
    
    this.midX = this.fromX + this.dx / 2;
    
    ctx.beginPath();
    ctx.moveTo(this.fromX, this.fromY);
    ctx.bezierCurveTo(this.ctrl1.x, this.ctrl1.y, this.ctrl2.x, this.ctrl2.y, this.x, this.y);
    ctx.stroke();
};

Connector.prototype.highlight = function(ctx) {
    /*
    Highlight this Connector by drawing another line along
    its length. Colour and line width set by canvasState.
     */
    ctx.beginPath();
    ctx.moveTo(this.fromX, this.fromY);
    ctx.bezierCurveTo(this.ctrl1.x, this.ctrl1.y, this.ctrl2.x, this.ctrl2.y, this.x, this.y);
    ctx.stroke();
    
    if (this.dest !== null) {
        this.drawLabel(ctx);
    }
}

// make an object in the format of jsBezier lib
Connector.prototype.getJsBez = function() {
    return [
        { x: this.fromX,   y: this.fromY   },
        { x: this.ctrl1.x, y: this.ctrl1.y },
        { x: this.ctrl2.x, y: this.ctrl2.y },
        { x: this.x,       y: this.y       }
    ];
};

Connector.prototype.drawLabel = function(ctx) {
    this.label_width = ctx.measureText(this.source.label).width + 10;
    this.dx = this.x - this.fromX,
    this.dy = this.y - this.fromY;
    
    if ( this.dx * this.dx + this.dy * this.dy > this.label_width * this.label_width / .49) {
        // determine the angle of the bezier at the midpoint
        var jsb = this.getJsBez(),
            midpoint = jsBezier.nearestPointOnCurve({ x: this.fromX + this.dx/2, y: this.fromY + this.dy/2 }, jsb),
            midpointAngle = jsBezier.gradientAtPoint(jsb, midpoint.location),
            corner = 6;
        
        // save the canvas state to start applying transformations
        ctx.save();
        
        // set the bezier midpoint as the origin
        ctx.translate(midpoint.point.x, midpoint.point.y);
        ctx.rotate(midpointAngle);
        ctx.fillStyle = '#aaa';
        
        var x1 = this.label_width/2,
            y1 = 6;
        
        // rounded rectangle
        ctx.beginPath();
        ctx.moveTo(-x1 + corner, -y1);
        ctx.lineTo( x1 - corner, -y1);
        ctx.arcTo ( x1, -y1,  x1, -y1 + corner, corner );
        ctx.lineTo( x1,  y1 - corner);
        ctx.arcTo ( x1,  y1,  x1 - corner, y1, corner );
        ctx.lineTo(-x1 + corner, y1);
        ctx.arcTo (-x1,  y1, -x1, y1 - corner, corner );
        ctx.lineTo(-x1, -y1 + corner);
        ctx.arcTo (-x1, -y1, -x1 + corner, -y1, corner );
        ctx.closePath();
        ctx.fill();
        
        ctx.fillStyle = 'white';
        ctx.fillText(this.source.label, 0, 0);
        ctx.restore();
    }
}

Connector.prototype.debug = function(ctx) {
    var jsb = this.getJsBez(),
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
    
    with(console) {
        group('linPt');
            log('linPt[0]: '+ linPt[0].x + ', ' + linPt[0].y);
            log('linPt[1]: '+ linPt[1].x + ', ' + linPt[1].y);
        groupEnd();
    
        group('connector debug');
            log('midpoint location: '+ midpoint.location);
            log('midpoint coord: '+ midpoint.point.x + ', ' + midpoint.point.y);
            log('jsbez angle: '+ ( midpointAngle * 180/Math.PI) );
            log('calculated angle: ' + (final_tangent * 180/Math.PI) );
        groupEnd();
    }
    
    ctx.fillStyle = '#0f0';
    ctx.beginPath();
    ctx.arc(midpoint.point.x, midpoint.point.y, 3, 0, 2 * Math.PI, true);
    ctx.fill();
};

OutputNode.prototype.debug = function(ctx) {
    this.in_magnets[0].connected[0].debug(ctx);
};

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
            jsBezier.distanceFromCurve(
                { x: mx, y: my }, 
                this.getJsBez()
            ).distance;
    }
    // mx,my is outside the rectangle, don't bother computing the bezier distance
    else return false;
};

function NodeLabel (label, x, y) {
    this.label = label || '';
    this.x = x || 0;
    this.y = y || 0;
}

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
    this.fill = fill || "#d40";
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
    
    // draw magnet
    in_magnet = this.in_magnets[0];
    in_magnet.x = this.x - this.inset;
    in_magnet.y = this.y - this.h/2;
    in_magnet.draw(ctx);
};

OutputNode.prototype.contains = function(mx, my) {
    // node is comprised of a rectangle and two ellipses
    return Math.abs(mx - this.x) < this.r
        && Math.abs(my - this.y) < this.h/2
        || Geometry.inEllipse(mx, my, this.x, this.y - this.h/2, this.r, this.r2)
        || Geometry.inEllipse(mx, my, this.x, this.y + this.h/2, this.r, this.r2);
};

OutputNode.prototype.getVertices = function() {
    var x1 = this.x + this.r,
        x2 = this.x - this.r,
        y1 = this.y + this.h/2,
        y2 = y1 - this.h;
    
    return [
    { x: this.x, y: this.y },
        { x: x1, y: y1 },
        { x: x2, y: y1 },
        { x: x1, y: y2 },
        { x: x2, y: y2 },
        { x: this.x, y: this.y + this.h/2 + this.r2 },
        { x: this.x, y: this.y - this.h/2 - this.r2 }
    ];
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

OutputNode.prototype.getVertices = function() {
    var x1 = this.x + this.r,
        x2 = this.x - this.r,
        y1 = this.y + this.h/2,
        y2 = y1 - this.h;
    
    return [
        { x: x1, y: y1 },
        { x: x2, y: y1 },
        { x: x1, y: y2 },
        { x: x2, y: y2 },
        { x: this.x, y: this.y + this.h/2 + this.r2 },
        { x: this.x, y: this.y - this.h/2 - this.r2 }
    ];
};

OutputNode.prototype.getLabel = function() {
    return new NodeLabel(this.label, this.x, this.y - this.h/2 - this.offset);
};
