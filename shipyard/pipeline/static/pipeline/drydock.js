/**
 * drydock.js
 *   Implements HTML5 Canvas interface for assembling
 *   pipelines from datatype and method nodes.
 *   Based on the canvas interactivity example by Simon
 *   Sarris, HTML5 Unleashed (2014) Pearson Education Inc.
 */

function CanvasState (canvas) {
    /*
    keeps track of canvas state (mouse drag, etc.)
     */

    // initialize "class"
    this.canvas = canvas;
    this.width = canvas.width;
    this.height = canvas.height;
    this.ctx = canvas.getContext('2d');

    // fixes issues with mouse coordinates
    this.stylePaddingLeft = 0;
    this.stylePaddingTop = 0;
    this.styleBorderLeft = 0;
    this.styleBorderTop = 0;
    if (window.getComputedStyle) {
        this.stylePaddingLeft = parseInt(getComputedStyle(canvas, null).getPropertyValue('padding-left'));
        this.stylePaddingTop = parseInt(getComputedStyle(canvas, null).getPropertyValue('padding-top'));
        this.styleBorderLeft = parseInt(getComputedStyle(canvas, null).getPropertyValue('border-left-width'));
        this.styleBorderTop = parseInt(getComputedStyle(canvas, null).getPropertyValue('border-top-width'));
    }

    // adjust for fixed-position bars at top or left of page
    var html = document.body.parentNode;
    this.htmlTop = html.offsetTop;
    this.htmlLeft = html.offsetLeft;

    this.valid = false; // if false, canvas will redraw everything
    this.shapes = []; // collection of shapes to be drawn
    this.connectors = []; // collection of connectors between shapes
    this.dragging = false; // if mouse drag

    this.selection = null; // reference to active (selected) object
    this.dragoffx = 0; // where in the object we clicked
    this.dragoffy = 0;

    // events
    var myState = this; // save reference to this particular CanvasState

    // de-activate double-click selection of text on page
    canvas.addEventListener('selectstart', function(e) { e.preventDefault(); return false; }, false);

    canvas.addEventListener('mousedown', function(e) {
        myState.doDown(e); // listener registered on mousedown event
    }, true);

    canvas.addEventListener('mousemove', function(e) {
        myState.doMove(e);
    }, true);

    canvas.addEventListener('mouseup', function(e) {
        myState.doUp(e);
    }, true);

    // options
    this.selectionColor = '#8af';
    this.selectionWidth = 2.5;
    setInterval(function() { myState.draw(); }, 30); // 30 ms between redraws
}

CanvasState.prototype.doDown = function(e) {
    var pos = this.getPos(e);
    var mx = pos.x;
    var my = pos.y;

    var shapes = this.shapes;
    var connectors = this.connectors;

    // did we click on a Connector?
    var l = connectors.length;
    for (var i = l-1; i >= 0; i--) {
        if (connectors[i].contains(mx, my, 5)) {
            this.selection = connectors[i];
            this.valid = false; // highlight but no drag
            return;
        }
    }

    // did we click on a shape?
    l = shapes.length;
    for (i = l-1; i >= 0; i--) {
        // check shapes in reverse order
        if (shapes[i].contains(mx, my)) {
            var mySel = shapes[i];
            // are we clicking on an out-magnet?
            out_magnets = mySel.out_magnets;
            for (var j = 0; j < out_magnets.length; j++) {
                out_magnet = out_magnets[j];
                // FIXME: actually an out-magnet should be able to have multiple connectors
                if (out_magnet.contains(mx, my) && out_magnet.connected == null) {
                    // create Connector from this out-magnet
                    conn = new Connector(null, null, out_magnet);
                    out_magnet.connected = conn;
                    connectors.push(conn);
                    this.selection = conn;
                    this.dragoffx = mx - conn.fromX;
                    this.dragoffy = my - conn.fromY;
                    this.dragging = true;
                    this.valid = false; // activate canvas
                    return;
                }
            }

            // otherwise we are moving the shape
            this.dragoffx = mx - mySel.x;
            this.dragoffy = my - mySel.y;
            this.dragging = true;
            this.selection = mySel;
            this.valid = false;
            return;
        }
    }

    // clicking outside of any shape de-selects any currently-selected shape
    if (this.selection) {
        this.selection = null;
        this.valid = false;
    }
};

CanvasState.prototype.doMove = function(e) {
    /*
    event handler for mouse motion over canvas
     */
    var mouse = this.getPos(e);
    var shapes = this.shapes;
    var i = 0;
    var shape = null;

    if (this.dragging) {
        // are we carrying a shape or Connector?
        if (this.selection != null) {

            // update coordinates of this shape/connector
            this.selection.x = mouse.x - this.dragoffx;
            this.selection.y = mouse.y - this.dragoffy;
            this.valid = false; // redraw

            // are we carrying a connector?
            if (typeof this.selection.fromX != 'undefined') {
                // get this connector's shape
                var own_shape = null;
                if (this.selection.out_magnet !== null) {
                    own_shape = this.selection.out_magnet.parent;
                }

                // check if connector has been dragged to an in-magnet
                for (i = 0; i < shapes.length; i++) {
                    shape = shapes[i];
                    if (own_shape !== null && shape == own_shape) {
                        continue;
                    }
                    if (typeof shape.in_magnets == 'undefined') {
                        // ignore Connectors
                        continue;
                    }
                    var in_magnets = shape.in_magnets;
                    for (var j = 0; j < in_magnets.length; j++) {
                        var in_magnet = in_magnets[j];
                        var connector_carrying_cdt;
                        if (this.selection.out_magnet === null || own_shape.constructor === RawNode) {
                            connector_carrying_cdt = '__raw__';
                        } else {
                            connector_carrying_cdt = this.selection.out_magnet.cdt;
                        }

                        if (connector_carrying_cdt == in_magnet.cdt) {
                            // light up magnet
                            in_magnet.fill = 'yellow';
                            if (in_magnet.connected == null && in_magnet.contains(this.selection.x, this.selection.y)) {
                                // jump to magnet
                                this.selection.x = in_magnet.x;
                                this.selection.y = in_magnet.y;
                                this.selection.in_magnet = in_magnet;
                                in_magnet.connected = this.selection;
                            }
                        }
                    }
                }
            } else {
                // carrying a shape
                if (mouse.x < 0.1 * this.width || mouse.x > 0.9 * this.width) {
                    // prevent shapes from being carried into end-zones
                    return;
                }
            }
        }
        // TODO: else dragging on canvas - we could implement block selection here
    }
};

CanvasState.prototype.doUp = function(e) {
    this.dragging = false;
    // check if most recent Connector is linked to a magnet
    var l = this.connectors.length;
    if (l == 0) {
        return; // no Connectors!
    }
    connector = this.connectors[ l-1 ]; // last object

    if (connector.in_magnet === null) {
        // has connector been carried into output end-zone?
        var mouse = this.getPos(e);

        if (mouse.x > 0.9 * this.canvas.width) {
            // Connector drawn into output end-zone
            if (connector.out_magnet == null || connector.out_magnet.parent.constructor == CDtNode) {
                // disallow Connectors directly between end-zones, or from CDT node to end-zone
                if (connector.out_magnet !== null) {
                    // free up this magnet
                    connector.out_magnet.connected = null;
                }
                this.connectors.pop();
                this.selection = null;
                this.valid = false;
            } else {
                // valid Connector, assign non-null value
                connector.in_magnet = '__output__';
                connector.x = mouse.x;
                connector.y = mouse.y;
            }
        } else {
            if (connector.in_magnet == null) {
                // not connected, delete Connector
                if (connector.out_magnet !== null) {
                    // free up out-magnet
                    connector.out_magnet.connected = null;
                }
                this.connectors.pop();
                this.selection = null;
                this.valid = false; // redraw canvas to remove this Connector
            }
        }
    }

    // turn off all in-magnets
    var shapes = this.shapes;
    for (var i = 0; i < shapes.length; i++) {
        var shape = shapes[i];
        if (typeof shape.in_magnets != 'undefined') {
            var in_magnets = shape.in_magnets;
            for (var j = 0; j < in_magnets.length; j++) {
                var in_magnet = in_magnets[j];
                in_magnet.fill = 'white';
            }
        }
    }
};

CanvasState.prototype.addShape = function(shape) {
    this.shapes.push(shape);
    this.valid = false;
};

CanvasState.prototype.clear = function() {
    // wipe canvas content clean before redrawing
    this.ctx.clearRect(0, 0, this.width, this.height);

    this.ctx.textAlign = 'center';
    this.ctx.font = '12pt Lato, sans-serif';

    // draw output end-zone
    this.ctx.fillStyle = '#faa';
    this.ctx.fillRect(this.width * 0.9, 0, this.width, this.height);
    this.ctx.fillStyle = 'black';
    this.ctx.fillText('Output', this.width * 0.95, 20);
};

CanvasState.prototype.draw = function() {
    /*
    Render pipeline objects to Canvas.
     */
    if (!this.valid) {
        var ctx = this.ctx;
        var shapes = this.shapes;
        var connectors = this.connectors;
        this.clear();

        // draw all shapes and magnets
        for (var i = 0; i < shapes.length; i++) {
            var shape = shapes[i];
            // skip shapes moved off the screen
            if (shape.x > this.width || shape.y > this.height || shape.x + 2 * shape.r < 0 || shape.y + 2 * shape.r < 0) {
                continue;
            }
            shapes[i].draw(ctx);
        }

        // draw all connectors
        for (i = 0; i < connectors.length; i++) {
            connectors[i].draw(ctx);
        }

        if (this.selection != null) {
            // draw selection ring
            ctx.strokeStyle = this.selectionColor;
            ctx.lineWidth = this.selectionWidth;
            var mySel = this.selection;

            // Is it possible to move these drawing instructions into the objects themselves? —JN
            ctx.beginPath();
            if (mySel.constructor == MethodNode) {
//                ctx.rect(mySel.x, mySel.y, mySel.w, mySel.h);
                // Draw a hexagon.
                var hx, hy;
                ctx.beginPath();
                ctx.moveTo(hx = mySel.x, hy = mySel.y);
                ctx.lineTo(hx += mySel.w, hy);
                ctx.lineTo(hx += mySel.h/3, hy += mySel.h/2);
                ctx.lineTo(hx -= mySel.h/3, hy += mySel.h/2);
                ctx.lineTo(hx = mySel.x, hy);
                ctx.lineTo(hx - mySel.h/3, hy - mySel.h/2);
            } else if (mySel.constructor ==CDtNode) {
                ctx.arc(mySel.x, mySel.y, mySel.r, 0, 2*Math.PI, false);
            } else if (mySel.constructor == Connector && this.dragging == false) {
                ctx.moveTo(mySel.x, mySel.y);
                ctx.lineTo(mySel.fromX, mySel.fromY);
            }
            ctx.closePath();
            ctx.stroke();
        }

        this.valid = true;
    }
};

CanvasState.prototype.getPos = function(e) {
    // returns a JavaScript object with x, y coordinates defined
    var element = this.canvas, offsetX = 0, offsetY = 0, mx, my;

    if (typeof element.offsetParent !== 'undefined') {
        do {
            offsetX += element.offsetLeft;
            offsetY += element.offsetTop;
        } while (element = element.offsetParent);
    }

    offsetX += this.stylePaddingLeft + this.styleBorderLeft + this.htmlLeft;
    offsetY += this.stylePaddingTop + this.styleBorderTop + this.htmlTop;

    mx = e.pageX - offsetX;
    my = e.pageY - offsetY;

    return { x: mx, y: my };
};

CanvasState.prototype.deleteObject = function() {
    // delete selected object
    var mySel = this.selection;
    var index = -1;
    var i = 0; // loop counter
    var in_magnets = [];
    var out_magnets = [];
    var this_connector = null;

    if (mySel !== null) {
        if (mySel.constructor == Connector) {
            // remove selected Connector from list
            mySel.in_magnet.connected = null;
            if (mySel.out_magnet !== null) {
                mySel.out_magnet.connected = null;
            }

            index = this.connectors.indexOf(mySel);
            this.connectors.splice(index, 1);
        }
        else if (mySel.constructor == MethodNode) {
            // delete Connectors associated with this shape
            in_magnets = mySel.in_magnets;
            for (i = 0; i < in_magnets.length; i++) {
                this_connector = in_magnets[i].connected;
                if (this_connector !== null) {
                    // remove from list of Connectors
                    index = this.connectors.indexOf(this_connector);
                    this.connectors.splice(index, 1);

                    // release magnets
                    in_magnets[i].connected = null;
                    if (this_connector.out_magnet !== null) {
                        this_connector.out_magnet.connected = null;
                    }
                }
            }

            out_magnets = mySel.out_magnets;
            for (i = 0; i < out_magnets.length; i++) {
                this_connector = out_magnets[i].connected;
                if (this_connector !== null) {
                    index = this.connectors.indexOf(this_connector);
                    this.connectors.splice(index, 1);

                    out_magnets[i].connected = null;
                    if (this_connector.in_magnet.constructor == Magnet) {
                        this_connector.in_magnet.connected = null;
                    }
                }
            }

            // remove MethodNode from list and any attached Connectors
            index = this.shapes.indexOf(mySel);
            this.shapes.splice(index, 1);
        }
        else if (mySel.constructor == CDtNode) {
            out_magnets = mySel.out_magnets;
            for (i = 0; i < out_magnets.length; i++) {
                this_connector = out_magnets[i].connected;
                if (this_connector !== null) {
                    index = this.connectors.indexOf(this_connector);
                    this.connectors.splice(index, 1);

                    out_magnets[i].connected = null;
                    if (this_connector.in_magnet.constructor == Magnet) {
                        // connector had terminated in a shape
                        this_connector.in_magnet.connected = null;
                    }
                }
            }
            index = this.shapes.indexOf(mySel);
            this.shapes.splice(index, 1);
        }
        else {
            return;
        }
        this.selection = null;
        this.valid = false; // re-draw canvas to make Connector disappear
    }
};

/*
CanvasState.prototype.submitForm = function() {
    var form_str = $('form').serialize();

    var shapes = this.shapes;
    for (var i = 0; i < shapes.length; i++) {

    }

    $.ajax({
        type: "POST",
        url: "pipeline_add",
        contentType: 'text/javascript; charset=UTF-8',
        data: form_str,
        dataType: "json",
        success: function(result) {
            console.log(result);
        }
    })
};
*/
