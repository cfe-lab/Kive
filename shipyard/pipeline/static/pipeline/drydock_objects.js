/**
 * drydock_objects.js
 *   JS prototypes that are used to populate canvasState
 *   (see drydock.js)
 */

function RawNode (x, y, r, fill, inset, offset, label) {
    /*
    Node representing an unstructured (raw) datatype.
    Rendered as a circle.
     */
    this.x = x || 0; // defaults to top left corner
    this.y = y || 0;
    this.r = r || 10; // radius
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
    // draw circle
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
    out_magnet.draw(ctx);
};

RawNode.prototype.highlight = function(ctx) {
    ctx.beginPath();
    ctx.arc(this.x, this.y, this.r, 0, 2*Math.PI, false);
    ctx.closePath();
    ctx.stroke();
}

RawNode.prototype.contains = function(mx, my) {
    // determine if mouse pointer coordinates (mx, my) are
    // within this shape's bounds - compare length of hypotenuse
    // to radius
    var dx = this.x - mx;
    var dy = this.y - my;
    return Math.sqrt(dx*dx + dy*dy) <= this.r;
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
    ctx.beginPath();
    ctx.moveTo(this.x, this.y);
    ctx.lineTo(this.x+this.w, this.y);
    ctx.lineTo(this.x+this.w, this.y+this.w);
    ctx.lineTo(this.x, this.y+this.w);
    ctx.closePath();
    ctx.stroke();
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
            attract = 2,
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
            attract = 2,
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
    ctx.beginPath();
    ctx.moveTo(hx = this.x, hy = this.y);
    ctx.lineTo(hx += this.w, hy);
    ctx.lineTo(hx += this.h/4, hy += this.h/2);
    ctx.lineTo(hx -= this.h/4, hy += this.h/2);
    ctx.lineTo(hx = this.x, hy);
    ctx.closePath();
    ctx.stroke();
}

MethodNode.prototype.contains = function(mx, my) {
    return this.x <= mx && this.x + this.w >= mx && this.y <= my && this.y + this.h >= my;
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
    this.connected = null; // linked to a Connector
}

Magnet.prototype.draw = function(ctx) {
    // update values passed from shape
    // ** Update magnet.x and magnet.y and then call draw(). —JN
//    this.x = x;
//    this.y = y;
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
    this.in_magnet = null;
    this.out_magnet = out_magnet || null;

    // is this Connector being drawn from an out-magnet?
    if (this.out_magnet == null) {
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

    if (this.out_magnet !== null) {
        // update coordinates in case magnet has moved
        this.fromX = this.out_magnet.x;
        this.fromY = this.out_magnet.y;
    }

    if (this.in_magnet !== null && this.in_magnet !== '__output__') {
        // attachment to in-magnet of a shape that may have moved
        this.x = this.in_magnet.x;
        this.y = this.in_magnet.y;
    }
    ctx.beginPath();
    ctx.moveTo(this.fromX, this.fromY);
    ctx.lineTo(this.x, this.y);
    ctx.stroke();
};

Connector.prototype.highlight = function(ctx, dragging) {
    /*
    Highlight this Connector by drawing another line along
    its length.  Colour and line width set by canvasState.
    Requires [dragging] to be false so Connector is not highlighted
    while it is being drawn.
     */
    if (dragging === false) {
        ctx.beginPath();
        ctx.moveTo(this.x, this.y);
        ctx.lineTo(this.fromX, this.fromY);
        ctx.closePath();
        ctx.stroke();
    }
}

Connector.prototype.contains = function(mx, my, pad) {
    /*
    Determine if mouse coordinates (x,y) are on or close to this
    connector with coordinates (x1,y1) and (x2,y2).
    This is based on three criteria:
    (1) x1 < x < x2
    (2) y1 < y < y2
    (3) the distance of x,y to the line is below cutoff,
        see http://en.wikipedia.org/wiki/Distance_from_a_point_to_a_line
     */
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
    )
};

