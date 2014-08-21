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
    this.pos_y = canvas.offsetTop;
    this.pos_x = canvas.offsetLeft;
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
    
    // FIXME: what is this for? myState will not be a clone of the object, rather it will just be a reference to the object.
    // canvasState.myState will just reference back to canvasState. —JN
    var myState = this; // save reference to this particular CanvasState
    
    this.outputZone = new OutputZone(this.width, this.height);
    
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
    this.selectionColor = '#7bf';
    this.selectionWidth = 2;
    setInterval(function() { myState.draw(); }, 50); // 50 ms between redraws
}

CanvasState.prototype.doDown = function(e) {
    var pos = this.getPos(e);
    var mx = pos.x;
    var my = pos.y;

    var shapes = this.shapes;
    var connectors = this.connectors;

    // did we click on a shape?
    for (i = shapes.length - 1; i >= 0; i--) {
        // check shapes in reverse order
        if (shapes[i].contains(mx, my)) {
            var mySel = shapes[i];
            
            // this shape is now on top.
            shapes.push(shapes.splice(i,1)[0]);
            
            // are we clicking on an out-magnet?
            out_magnets = mySel.out_magnets;
            for (var j = 0; j < out_magnets.length; j++) {
                out_magnet = out_magnets[j];
                
                if (out_magnet.contains(mx, my)) {
                    // create Connector from this out-magnet
                    conn = new Connector(null, null, out_magnet);
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
            if (this.selection.constructor === MethodNode) {
                document.getElementById('id_method_button').value = 'Revise Method';
            }
            this.valid = false;
            return;
        }
    }

    // did we click on a Connector?
    // check this -after- checking shapes, as the algorithm is slower.
    for (var i = connectors.length - 1; i >= 0; i--) {
        if (connectors[i].contains(mx, my, 5)) {
            this.selection = connectors[i];
            this.dragoffx = this.dragoffy = 0;
            this.dragging = true;
            this.valid = false;
            return;
        }
    }

    // clicking outside of any shape de-selects any currently-selected shape
    if (this.selection) {
        this.selection = null;
        this.valid = false;
        $('#id_method_button').val('Add Method');
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
            if (this.selection.constructor == Connector) {
                // reset to allow mouse to disengage Connector from a magnet
                
                if (this.selection.dest) {
                    if (this.selection.dest.parent.constructor == OutputNode) {
                        // if the cable leads to an output node, then act as if
                        // the output node itself was clicked...
                        // not sure if this is the ideal behaviour...
                        // maybe output nodes should just disappear when they are
                        // disconnected? -JN
                        this.selection = this.selection.dest.parent;
                        return;
                    }
                    else if (this.selection.dest.constructor == Magnet) {
                        this.selection.dest.connected = [];
                        this.selection.dest = null;
                    }
                }

                // get this connector's shape
                if (this.selection.source !== null) {
                    var own_shape = this.selection.source.parent;
                }

                // check if connector has been dragged to an in-magnet
                for (i = 0; i < shapes.length; i++) {
                    shape = shapes[i];
                    
                    // ignore Connectors, RawNodes, CDtNodes
                    // and disallow self-referential connections
                    if (typeof shape.in_magnets == 'undefined' 
                        || shape.in_magnets.length == 0
                        || typeof own_shape !== 'undefined' && shape == own_shape) {
                        continue;
                    }

                    // shape is a Method, check its in-magnets
                    var in_magnets = shape.in_magnets,
                        connector_carrying_cdt;
                    
                    for (var j = 0; j < in_magnets.length; j++) {
                        var in_magnet = in_magnets[j];

                        // retrieve CompoundDatatype of out-magnet
                        if (own_shape.constructor === RawNode) {
                            connector_carrying_cdt = null;
                        } else {
                            connector_carrying_cdt = this.selection.source.cdt;
                        }
                        // does this in-magnet accept this CompoundDatatype?
                        if (connector_carrying_cdt == in_magnet.cdt) {
                            // light up magnet
                            in_magnet.fill = '#ff8';
                            if (in_magnet.connected.length == 0 
                                && in_magnet.contains(this.selection.x, this.selection.y)
                                ) {
                                // jump to magnet
                                this.selection.x = in_magnet.x;
                                this.selection.y = in_magnet.y;
                                in_magnet.connected = [ this.selection ];
                                this.selection.dest = in_magnet;
                            }
                        }
                    }
                }
            } else {
                // carrying a shape
            }
        }
        // TODO: else dragging on canvas - we could implement block selection here
    }
};

CanvasState.prototype.doUp = function(e) {
    this.dragging = false;
    this.valid = false;
    var mouse = this.getPos(e);
    var index;
    
    if (this.selection === null) {
        return;
    }

    // are we carrying a Connector?
    if (this.selection.constructor != Connector) {
        if (this.outputZone.contains(mouse.x, mouse.y)) {
            // Shape dragged into output zone
            this.selection.x = this.outputZone.x - this.selection.w;
        }
        return;
    }

    var connector = this.selection;

    if (connector.dest === null) {
        // connector not yet linked to anything

        if (this.outputZone.contains(mouse.x, mouse.y)) {
            // Connector drawn into output zone
            if (connector.source.parent.constructor !== MethodNode) {
                // disallow Connectors from data node directly to end-zone
                index = this.connectors.indexOf(connector);
                this.connectors.splice(index, 1);
                this.selection = null;
                this.valid = false;
            } else {
                // valid Connector, assign non-null value
                
                var outNode = new OutputNode(mouse.x, mouse.y, null, null, '#d40', null, null, connector.source.label);
                this.addShape(outNode);
                
                connector.dest = outNode.in_magnets[0];
                connector.dest.connected = [ connector ];
                connector.source.connected.push(connector);
                
                outNode.y = this.outputZone.y + this.outputZone.h + outNode.h/2 + outNode.r2;// push out of output zone
                outNode.x = mouse.x;
                this.valid = false;

                // spawn dialog for output label
                var dialog = document.getElementById("dialog_form");
                
                $(dialog).data('node', outNode).show().css({
                    left: Math.min(mouse.x, this.outputZone.x + this.outputZone.w/2 - dialog.offsetWidth/2 ) + this.pos_x,
                    top:  Math.min(mouse.y - dialog.offsetHeight/2, this.canvas.height - dialog.offsetHeight) + this.pos_y
                });
                
                $('#output_name', dialog).val(connector.source.label).select(); // default value;
            }
        } else {
            // Connector not linked to anything - delete
            index = this.connectors.indexOf(connector);
            this.connectors.splice(index, 1);
            this.selection = null;
            this.valid = false; // redraw canvas to remove this Connector
        }
    } else if (connector.dest.constructor === Magnet) {
        // connector has been linked to an in-magnet
        if (connector.source.connected.indexOf(connector) < 0) {
            // this is a new Connector, update source magnet
            connector.source.connected.push(connector);
        }

        if (connector.dest.connected.indexOf(connector) < 0) {
            // this is a new Connector, update destination magnet
            connector.dest.connected.push(connector);
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
        
        var draggingFromMethodOut = this.dragging && this.selection 
            && this.selection.constructor == Connector
            && this.selection.source.parent.constructor == MethodNode;

        // draw output end-zone -when- dragging a connector from a MethodNode
        if (draggingFromMethodOut) {
            this.outputZone.draw(this.ctx);
        }
        
        // draw all shapes and magnets
        for (var i = 0; i < shapes.length; i++) {
            var shape = shapes[i];
            // skip shapes moved off the screen
            if (shape.x > this.width || shape.y > this.height ||
                shape.x + 2 * shape.r < 0 || shape.y + 2 * shape.r < 0) {
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
            ctx.lineWidth = this.selectionWidth * 2;
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

CanvasState.prototype.deleteObject = function(objectToDelete) {
    // delete selected object
    // @param objectToDelete optionally specifies which object should be deleted.
    // Otherwise just go with the current selection.
    var mySel = objectToDelete || this.selection,
        index = -1,
        i = 0, // loop counter
        in_magnets = [],
        in_magnet,
        out_magnets = [],
        out_magnet,
        this_connector = null;

    if (mySel !== null) {
        if (mySel.constructor == Connector) {
            // remove selected Connector from list
            
            // if a cable to an output node is severed, delete the node as well
            // contains a LEGACY CHECK since OutputNodes used to be strings
            if (typeof mySel.dest !== 'string' && mySel.dest.parent.constructor == OutputNode) {
                index = this.shapes.indexOf(mySel.dest.parent);
                this.shapes.splice(index, 1);
            } else {
                // remove connector from destination in-magnet
                index = mySel.dest.connected.indexOf(mySel);
                mySel.dest.connected.splice(index, 1);
            }

            // remove connector from source out-magnet
            index = mySel.source.connected.indexOf(mySel);
            mySel.source.connected.splice(index, 1);

            // remove Connector from master list
            index = this.connectors.indexOf(mySel);
            this.connectors.splice(index, 1);
        }
        else if (mySel.constructor == MethodNode) {
            // delete Connectors terminating in this shape
            in_magnets = mySel.in_magnets;
            for (i = 0; i < in_magnets.length; i++) {
                if (in_magnet.connected.length > 0) {
                    this.deleteObject(in_magnets[i].connected[0]);
                }
            }

            // delete Connectors from this shape to other nodes
            out_magnets = mySel.out_magnets;
            for (i = 0; i < out_magnets.length; i++) {
                for (j = 0; j < out_magnets[i].connected.length; j++) {
                    this.deleteObject(out_magnets[i].connected[j]);
                }
            }

            // remove MethodNode from list and any attached Connectors
            index = this.shapes.indexOf(mySel);
            this.shapes.splice(index, 1);
        }
        else if (mySel.constructor == OutputNode) {
            // deleting an output node is the same as deleting the cable
            this_connector = mySel.in_magnets[0].connected[0];
            this.deleteObject(this_connector);
        }
        else {  // CDtNode or RawNode
            out_magnets = mySel.out_magnets;
            for (i = 0; i < out_magnets.length; i++) {
                out_magnet = out_magnets[i];
                for (j = 0; j < out_magnet.connected.length; j++) {
                    this_connector = out_magnets[i].connected[j];
                    index = this.connectors.indexOf(this_connector);
                    this.connectors.splice(index, 1);

                    if (this_connector.dest !== undefined && this_connector.dest.constructor == Magnet) {
                        // in-magnets can accept only one Connector
                        this_connector.dest.connected = [];
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