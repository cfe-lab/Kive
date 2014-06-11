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
                if (out_magnet.contains(mx, my)) {
                    // create Connector from this out-magnet
                    conn = new Connector(null, null, out_magnet);
                    //out_magnet.connected.push(conn);  // wait until mouse-up
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
                // reset to allow mouse to disengage Connector from a magnet
                this.selection.in_magnet = null;

                // get this connector's shape
                var own_shape = null;
                if (this.selection.out_magnet !== null) {
                    own_shape = this.selection.out_magnet.parent;
                }

                // check if connector has been dragged to an in-magnet
                for (i = 0; i < shapes.length; i++) {
                    shape = shapes[i];
                    if (typeof shape.in_magnets == 'undefined') {
                        // ignore Connectors
                        continue;
                    }

                    if (own_shape !== null && shape == own_shape) {
                        // disallow self-referential connections
                        continue;
                    }

                    // shape is a Method, check its in-magnets
                    var in_magnets = shape.in_magnets;
                    var connector_carrying_cdt;
                    for (var j = 0; j < in_magnets.length; j++) {
                        var in_magnet = in_magnets[j];

                        // retrieve CompoundDatatype of out-magnet
                        if (own_shape.constructor === RawNode) {
                            connector_carrying_cdt = '__raw__';
                        } else {
                            connector_carrying_cdt = this.selection.out_magnet.cdt;
                        }

                        // does this in-magnet accept this CompoundDatatype?
                        if (connector_carrying_cdt == in_magnet.cdt) {
                            // light up magnet
                            in_magnet.fill = 'yellow';
                            if (in_magnet.connected.length == 0 && in_magnet.contains(this.selection.x, this.selection.y)) {
                                // jump to magnet
                                this.selection.x = in_magnet.x;
                                this.selection.y = in_magnet.y;
                                this.selection.in_magnet = in_magnet;
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

    // are we carrying a Connector?
    if (this.selection === null || typeof this.selection.fromX === 'undefined') {
        return;
    }

    var connector = this.selection;
    var in_magnet = connector.in_magnet;
    var out_magnet = connector.out_magnet;

    if (in_magnet === null) {
        var mouse = this.getPos(e);

        if (mouse.x > 0.9 * this.canvas.width) {
            // Connector drawn into output end-zone
            if (out_magnet.parent.constructor !== MethodNode) {
                // disallow Connectors from data node directly to end-zone
                this.connectors.pop();
                this.selection = null;
                this.valid = false;
            } else {
                // valid Connector, assign non-null value
                connector.in_magnet = '__output__';
                connector.out_magnet.connected.push(connector);
                connector.x = mouse.x;  // FIXME: is this necessary?
                connector.y = mouse.y;
            }
        } else {
            // Connector not linked to anything - delete
            if (in_magnet === null) {
                // not connected
                this.connectors.pop();
                this.selection = null;
                this.valid = false; // redraw canvas to remove this Connector
            }
        }
    } else {
        // connector has been linked to an in-magnet
        if (out_magnet.connected.indexOf(connector) < 0) {
            // this is a new Connector, update source magnet
            connector.out_magnet.connected.push(connector);
        }

        // if destination is output end-zone, then do not update
        if (in_magnet.constructor === Magnet) {
            if (in_magnet.connected.indexOf(connector) < 0) {
                // this is a new Connector, update destination magnet
                connector.in_magnet.connected.push(connector);
            }
        } else {
            in_magnet = '__output__';
        }
    }

    // turn off all in-magnets
    var shapes = this.shapes;
    for (var i = 0; i < shapes.length; i++) {
        var shape = shapes[i];
        if (typeof shape.in_magnets != 'undefined') {
            var in_magnets = shape.in_magnets;
            for (var j = 0; j < in_magnets.length; j++) {
                in_magnet = in_magnets[j];
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

            mySel.highlight(ctx, this.dragging);
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
    var in_magnet;
    var out_magnets = [];
    var out_magnet;
    var this_connector = null;

    if (mySel !== null) {
        if (mySel.constructor == Connector) {
            // remove selected Connector from list
            mySel.in_magnet.connected = [];
            index = mySel.out_magnet.connected.indexOf(mySel);
            mySel.out_magnet.connected.splice(index, 1);
            index = this.connectors.indexOf(mySel);
            this.connectors.splice(index, 1);
        }
        else if (mySel.constructor == MethodNode) {
            // delete Connectors terminating in this shape
            in_magnets = mySel.in_magnets;
            for (i = 0; i < in_magnets.length; i++) {
                in_magnet = in_magnets[i];
                if (in_magnet.connected.length > 0) {
                    this_connector = in_magnet.connected[0];

                    // remove reference from out-magnet of source node
                    out_magnet = this_connector.out_magnet;
                    index = out_magnet.connected.indexOf(this_connector);
                    out_magnet.connected.splice(this_connector, 1);

                    // remove from list of Connectors
                    index = this.connectors.indexOf(this_connector);
                    this.connectors.splice(index, 1);
                    in_magnet.connected = [];
                }
            }

            // delete Connectors from this shape to other nodes
            out_magnets = mySel.out_magnets;
            for (i = 0; i < out_magnets.length; i++) {
                out_magnet = out_magnets[i];
                for (j = 0; j < out_magnets.connected.length; j++) {
                    out_magnet = out_magnets[j];

                }


                this_connector = out_magnets[i].connected;
                if (this_connector !== null) {
                    index = this.connectors.indexOf(this_connector);
                    this.connectors.splice(index, 1);

                    out_magnets[i].connected = [];
                    if (this_connector.in_magnet.constructor == Magnet) {
                        this_connector.in_magnet.connected = [];
                    }
                }
            }

            // remove MethodNode from list and any attached Connectors
            index = this.shapes.indexOf(mySel);
            this.shapes.splice(index, 1);
        }
        else {  // CDtNode or RawNode
            out_magnets = mySel.out_magnets;
            for (i = 0; i < out_magnets.length; i++) {
                out_magnet = out_magnets[i];
                for (j = 0; j < out_magnet.connected.length; j++) {
                    this_connector = out_magnets[i].connected[j];
                    index = this.connectors.indexOf(this_connector);
                    this.connectors.splice(index, 1);

                    if (this_connector.in_magnet !== undefined && this_connector.in_magnet.constructor == Magnet) {
                        // in-magnets can accept only one Connector
                        this_connector.in_magnet.connected = [];
                    }
                }
                out_magnet.connected = [];
            }
            index = this.shapes.indexOf(mySel);
            this.shapes.splice(index, 1);
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
