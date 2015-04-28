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
    
    this.scale = 1;
    this.enable_labels = true;

    // adjust for fixed-position bars at top or left of page
    var html = document.body.parentNode;
    this.htmlTop = html.offsetTop;
    this.htmlLeft = html.offsetLeft;
    
    this.valid = false; // if false, canvas will redraw everything
    this.shapes = []; // collection of shapes to be drawn
    this.connectors = []; // collection of connectors between shapes
    this.dragging = false; // if mouse drag

    this.selection = []; // reference to active (selected) objects
    this.dragstart = { x: 0, y: 0 }; // where in the object we clicked
    this.dragoffx = 0;
    this.dragoffy = 0;
    
    this.exec_order = [];
    this.exec_order_is_ambiguous = null;
    
    this.collisions = 0;

    // events
    var myState = this; // save reference to this particular CanvasState
    this.outputZone = new OutputZone(this.width, this.height);

    // options
    this.selectionColor = '#7bf';
    this.selectionWidth = 2;
    setInterval(function() { myState.draw(); }, 50); // 50 ms between redraws

    // Parameters on data-x
    this.can_edit = !($(canvas).data('editable') === false);
}

CanvasState.prototype.setScale = function(factor) {
    this.scale = factor;
    this.ctx.scale(factor, factor);
};

CanvasState.prototype.getMouseTarget = function(mx, my) {
    var shape, shapes = this.shapes;
    var connectors = this.connectors;

    // did we click on a shape?
    for (var i = shapes.length - 1; i >= 0; i--) {
        shape = shapes[i];
        
        // check shapes in reverse order
        if (shape.contains(mx, my)) {
            // are we clicking on an out-magnet?
            for (var j = 0; j < shape.out_magnets.length; j++) {
                if (shape.out_magnets[j].contains(mx, my)) {
                    return shape.out_magnets[j];
                }
            }
            
            // are we clicking an in-magnet?
            for (j = 0; j < shape.in_magnets.length; j++) {
                if (shape.in_magnets[j].contains(mx, my)) {
                    return shape.in_magnets[j];
                }
            }
            
            // otherwise return the shape object.
            return shape;
        }
    }
    
    // did we click on a Connector?
    // check this -after- checking shapes, as the algorithm is slower.
    for (i = connectors.length - 1; i >= 0; i--)
        if (connectors[i].contains(mx, my, 5))
            return connectors[i];
    
    return false;
};

CanvasState.prototype.doDown = function(e) {
    var pos = this.getPos(e),
        mx = pos.x, my = pos.y,
        shift = e.shiftKey,
        mySel = this.getMouseTarget(mx, my);
    
    if (mySel === false) {
        if (!shift) {
            // nothing clicked
            this.selection = [];
            this.valid = false;
            $('#id_method_button').val('Add Method');
        }
        return false;
    }
    
    //
    if (!shift && this.selection.indexOf(mySel) > -1) {
        this.dragstart = { x: mx, y: my };
        this.dragging = true;
        this.valid = false; // activate canvas
        return;
    }
    
    if (mySel instanceof Magnet && mySel.isInput) {
        if (mySel.connected.length > 0) {
            mySel = mySel.connected[0];
        } else {
            mySel = mySel.parent;
        }
    }
    if ([ MethodNode, RawNode, CDtNode, OutputNode ].indexOf(mySel.constructor) > -1) {
        // this shape is now on top.
        var i = this.shapes.indexOf(mySel);
        this.shapes.push(this.shapes.splice(i,1)[0]);
    
        // moving the shape
        this.dragoffx = mx - mySel.x;
        this.dragoffy = my - mySel.y;
    
        var sel_stack_ix = this.selection.indexOf(mySel);
    
        if (shift && sel_stack_ix > -1) {
            this.selection.splice(sel_stack_ix,1);
        } else if (shift) {
            this.selection.push(mySel);
        } else {
            this.selection = [ mySel ];
        }
    
        if (mySel instanceof MethodNode) {
            $('#id_method_button').val('Revise Method');
        }
    }

    else if (mySel instanceof Magnet && mySel.isOutput) {

        if ((!shift || this.selection.length == 0) && this.can_edit) {
            // create Connector from this out-magnet
            conn = new Connector(null, null, mySel);
            this.connectors.push(conn);
            this.selection = [ conn ];
            this.dragoffx = mx - conn.fromX;
            this.dragoffy = my - conn.fromY;
        }
    }
    else if (mySel instanceof Connector) {
        if (!shift || this.selection.length == 0) {
            this.selection = [ mySel ];
            if(this.can_edit){
                this.dragoffx = this.dragoffy = 0;
            } else {
                this.dragging = false;
                return;
            }
        }
    }
    
    this.dragstart = { x: mx, y: my };
    this.dragging = true;
    this.valid = false; // activate canvas
    return;
};

CanvasState.prototype.doMove = function(e) {
    /*
     event handler for mouse motion over canvas
     */
    var mouse = this.getPos(e),
        shapes = this.shapes,
        dx = mouse.x - this.dragstart.x,
        dy = mouse.y - this.dragstart.y,
        i, j, shape, sel;
    
    if (this.dragging) {
        // are we carrying a shape or Connector?
        if (this.selection.length > 0) {
            for (j = 0; j < this.selection.length; j++) {
                sel = this.selection[j];
                
                // update coordinates of this shape/connector
                sel.x += dx;
                sel.y += dy;
                
                // any changes made by the collision detection algorithm on this shape are now made "official"
                if (sel.dx != 0) {
                    sel.x += sel.dx;
                    sel.dx = 0;
                }
                if (sel.dy != 0) {
                    sel.y += sel.dy;
                    sel.dy = 0;
                }
                
                this.valid = false; // redraw

                // are we carrying a connector?
                if (sel instanceof Connector && this.can_edit) {
                    // reset to allow mouse to disengage Connector from a magnet
                    
                    sel.x = mouse.x;
                    sel.y = mouse.y;
                    
                    if (sel.dest) {
                        if (sel.dest.parent instanceof OutputNode) {
                            // if the cable leads to an output node, then act as if
                            // the output node itself was clicked...
                            // not sure if this is the ideal behaviour...
                            // maybe output nodes should just disappear when they are
                            // disconnected? -JN
                            this.selection = [ sel.dest.parent ];
                            return;
                        }
                        else if (sel.dest instanceof Magnet) {
                            sel.dest.connected = [];
                            sel.dest = null;
                        }
                    }

                    // get this connector's shape
                    if (sel.source !== null) {
                        var own_shape = sel.source.parent;
                    }

                    // check if connector has been dragged to an in-magnet
                    for (i = 0; i < shapes.length; i++) {
                        shape = shapes[i];
                    
                        // ignore Connectors, RawNodes, CDtNodes
                        // and disallow self-referential connections
                        if (typeof shape.in_magnets === 'undefined' 
                                || shape.in_magnets.length === 0
                                || typeof own_shape !== 'undefined'
                                && shape === own_shape) {
                            continue;
                        }

                        // shape is a Method, check its in-magnets
                        var in_magnets = shape.in_magnets,
                            connector_carrying_cdt;
                    
                        for (var j = 0; j < in_magnets.length; j++) {
                            var in_magnet = in_magnets[j];

                            // retrieve CompoundDatatype of out-magnet
                            if (own_shape instanceof RawNode) {
                                connector_carrying_cdt = null;
                            } else {
                                connector_carrying_cdt = sel.source.cdt;
                            }
                            // does this in-magnet accept this CompoundDatatype?
                            if (shape instanceof MethodNode &&
                                connector_carrying_cdt == in_magnet.cdt) {
                                // light up magnet
                                in_magnet.fill = '#ff8';
                                in_magnet.acceptingConnector = true;
                                if (in_magnet.connected.length == 0 
                                        && in_magnet.contains(sel.x, sel.y)) {
                                    // jump to magnet
                                    sel.x = in_magnet.x;
                                    sel.y = in_magnet.y;
                                    in_magnet.connected = [ sel ];
                                    sel.dest = in_magnet;
                                }
                            }
                        }
                    }
                } else {
                    // carrying a shape
                    // if execution order is ambiguous, the tiebreaker is the y-position.
                    // dragging a method node needs to calculate this in real-time.
                    if (this.exec_order_is_ambiguous && sel instanceof MethodNode) {
                        this.disambiguateExecutionOrder();
                    }
                }
            }
        }
        // TODO: else dragging on canvas - we could implement block selection here
        
        this.dragstart = mouse;
    }
    
};

CanvasState.prototype.scaleToCanvas = function() {
    // general strategy: get the x and y coords of every shape, then get the max and mins of these sets.
    var x_ar = [], y_ar = [],
        margin = {
            x: Math.min(this.width  * .15, 100),
            y: Math.min(this.height * .15, 100)
        },
        shape, i;
    
    for (i = 0; i < this.shapes.length; i++) {
        x_ar.push(this.shapes[i].x);
        y_ar.push(this.shapes[i].y);
    }
    
    var xmin = Math.min.apply(null, x_ar),
        ymin = Math.min.apply(null, y_ar),
        pipeline_width = Math.max.apply(null, x_ar) - xmin,
        pipeline_height = Math.max.apply(null, y_ar) - ymin,
        offset = {
            x: xmin - margin.x,
            y: ymin - margin.y
        },
        scale = {
            x: (this.width  - margin.x * 2) / pipeline_width,
            y: (this.height - margin.y * 2) / pipeline_height
        };
    
    /*
    for both x and y dimensions, 4 numbers are now available:
     - the current position
     - the lowest (numerically) current position
     - the ratio to scale by
     - the lowest desired position (the margin)
     */
    for (i = 0; i < this.shapes.length; i++) {
        shape = this.shapes[i];
        shape.x = (shape.x - xmin) * scale.x + margin.x;
        shape.y = (shape.y - ymin) * scale.y + margin.y;
    }
    
    this.valid = false;
};

CanvasState.prototype.centreCanvas = function() {
    var x_ar = [], y_ar = [],
        shapes = this.shapes,
        xmove, ymove;
    
    for (var i = 0; i < shapes.length; i++) {
        x_ar.push(shapes[i].x);
        y_ar.push(shapes[i].y);
    }
    
    xmove = ( this.width  - Math.max.apply(null, x_ar) - Math.min.apply(null, x_ar) ) / 2;
    ymove = ( this.height - Math.max.apply(null, y_ar) - Math.min.apply(null, y_ar) ) / 2;
    
    for (i = 0; i < shapes.length; i++) {
        shapes[i].x += xmove;
        shapes[i].y += ymove;
    }
    
    this.valid = false;
};

CanvasState.prototype.detectCollisions = function(myShape, bias) {
    var followups = [],
        vertices = myShape.getVertices(),
        scale_width = this.canvas.width / this.scale,
        scale_height = this.canvas.height / this.scale;
        
    // Bias defines how much to move myShape vs how much to move the shape it collided with.
    // 1 would be 100% myShape movement, 0 would be 100% other shape movement, and everything
    // else in-between is possible.
    if (bias == null) bias = .75;
    
    for (var i = 0; i < this.shapes.length; i++) {
        var shape = this.shapes[i];
    
        // Objects are passed by reference in JS, so this comparison is really comparing references.
        // Identical objects at different memory addresses will not pass this condition.
        if (shape == myShape) continue;
        
        for (var j = 0; j < vertices.length; j++) {
            var vertex = vertices[j];
            while (shape.contains(vertex.x, vertex.y)) {
                // If a collision is detected and we start moving myShape, we have to re-check all previous shapes as well.
                // We do this by resetting the counter.
                // We also have to check for collisions on the other shape.
                if (i > -1) {
                    i = -1;
                    followups.push(shape);
                }
                
                // Drawing a line between the two objects' centres, move the centre 
                // of mySel to extend this line while keeping the same angle.
                var my_x = myShape.x + myShape.dx,
                    my_y = myShape.y + myShape.dy,
                    sh_x = shape.x + shape.dx,
                    sh_y = shape.y + shape.dy,
                    dx = my_x - sh_x,
                    dy = my_y - sh_y,
                    step = 5,
                    dh = (dx < 0 ? -1 : 1) * (Math.sqrt(dx*dx + dy*dy) + step),// add however many additional pixels you want to move
                    angle = dx ? Math.atan(dy / dx) : Math.PI/2,
                    Dx = Math.cos(angle) * dh - dx,
                    Dy = Math.sin(angle) * dh - dy;
                
                myShape.dx += Dx * bias;
                shape.dx   -= Dx * (1 - bias);
                my_x = myShape.x + myShape.dx;
                sh_x = shape.x + shape.dx;
                
                if (my_x > scale_width) {
                    sh_x -= my_x - scale_width;
                    my_x = scale_width;
                }
                if (my_x < 0) {
                    sh_x -= my_x;
                    my_x = 0;
                }
                if (sh_x > scale_width) {
                    my_x -= sh_x - scale_width;
                    sh_x = scale_width;
                }
                if (sh_x < 0) {
                    my_x -= sh_x;
                    sh_x = 0;
                }
                
                myShape.dx = my_x - myShape.x;
                shape.dx = sh_x - shape.x;
                
                myShape.dy += Dy * bias;
                shape.dy -= Dy * (1 - bias);
                my_y = myShape.y + myShape.dy;
                sh_y = shape.y + shape.dy;
                
                if (my_y > scale_height) {
                    sh_y -= my_y - scale_height;
                    my_y = scale_height;
                }
                if (my_y < 0) {
                    sh_y -= my_y;
                    my_y = 0;
                }
                if (shape.y > scale_height) {
                    my_y += sh_y - scale_height;
                    sh_y = scale_height;
                }
                if (sh_y < 0) {
                    my_y -= sh_y;
                    sh_y = 0;
                }
                
                myShape.dy = my_y - myShape.y;
                shape.dy = sh_y - shape.y;
        
                vertices = myShape.getVertices();
                vertex = vertices[j];
            }
        }
    }
    
    for (i = 0; i < followups.length; i++) {
        this.detectCollisions(followups[i], bias);
    }
}

CanvasState.prototype.doUp = function(e) {
    this.valid = false;
    var dialog = $("#dialog_form"),
        index, sel, i, j, connector, suffix, new_output_label, out_node, shape, in_magnet;
    $(this.canvas).css("cursor", "auto");
    
    // Collision detection!
    if (this.dragging && this.selection.length > 0) {
        for (i = 0; i < this.selection.length; i++) {
            if (typeof this.selection[i].getVertices == 'function') {
                this.detectCollisions(this.selection[i]);
            }
        }
    }
    
    this.dragging = false;
    
    if (this.selection.length == 0) {
        return;
    }

    // are we carrying a shape?
    if (!(this.selection[0] instanceof Connector)) {
        for (i = 0; i < this.selection.length; i++) {
            sel = this.selection[i];
            if (this.outputZone.contains(sel.x, sel.y)) {
                // Shape dragged into output zone
                sel.x = this.outputZone.x - sel.w;
            }
        }
    }
    
    if (this.selection[0] instanceof Connector) {
        connector = this.selection[0];
        
        if (connector.dest === null) {
            // connector not yet linked to anything
        
            if (this.outputZone.contains(connector.x, connector.y)) {
                // Connector drawn into output zone
                if (!(connector.source.parent instanceof MethodNode)) {
                    // disallow Connectors from data node directly to end-zone
                    index = this.connectors.indexOf(connector);
                    this.connectors.splice(index, 1);
                    this.selection = [];
                    this.valid = false;
                } else {
                    // valid Connector, assign non-null value
                    // make sure label is not a duplicate
                    suffix = 0;
                    new_output_label = connector.source.label;
                    for (i=0; i< this.shapes.length; i++) {
                        shape = this.shapes[i];
                        if (!(shape instanceof OutputNode)) continue;
                        if (shape.label == new_output_label) {
                            i = -1;
                            suffix++;
                            new_output_label = connector.source.label +'_'+ suffix;
                        }
                    }
                
                    out_node = new OutputNode(connector.x, connector.y, new_output_label);
                    this.addShape(out_node);
                
                    connector.dest = out_node.in_magnets[0];
                    connector.dest.connected = [ connector ];
                    connector.source.connected.push(connector);
                
                    out_node.y = this.outputZone.y + this.outputZone.h + out_node.h/2 + out_node.r2;// push out of output zone
                    out_node.x = connector.x;
                    this.valid = false;

                    // spawn dialog for output label
                    dialog
                        .data('node', out_node)
                        .show()
                        .css({
                            left: Math.min(connector.x, this.outputZone.x + this.outputZone.w/2 - dialog[0].offsetWidth/2 ) + this.pos_x,
                            top:  Math.min(connector.y - dialog[0].offsetHeight/2, this.canvas.height - dialog[0].offsetHeight) + this.pos_y
                        })
                    .find('#output_name')
                        .val(new_output_label)
                        .select(); // default value;
                }
            } else {
                // Connector not linked to anything - delete
                index = this.connectors.indexOf(connector);
                this.connectors.splice(index, 1);
                
                index = connector.source.connected.indexOf(connector);
                connector.source.connected.splice(index, 1);
                
                this.selection = [];
                this.valid = false; // redraw canvas to remove this Connector
            }
        
        } else if (connector.dest instanceof Magnet) {
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
    }
        
    // see if this has changed the execution order
    this.testExecutionOrder();

    // turn off all in-magnets
    for (i = 0; i < this.shapes.length; i++) {
        if (this.shapes[i].in_magnets instanceof Array) {
            for (j = 0; j < this.shapes[i].in_magnets.length; j++) {
                in_magnet = this.shapes[i].in_magnets[j];
                in_magnet.fill = '#fff';
                in_magnet.acceptingConnector = false;
            }
        }
    }
};

CanvasState.prototype.contextMenu = function(e) {
    var pos = this.getPos(e),
        mcm = $('#method_context_menu'),
        sel = this.selection,
        showMenu = function() {
            mcm.show().css({ top: e.pageY, left: e.pageX });
            $('li', mcm).show();
        }

    // Edit mode can popup the context menu to delete and edit nodes
    if(this.can_edit){
        if (sel.length == 1 && !(sel[0] instanceof Connector) ) {
            showMenu();
            if (sel[0] instanceof RawNode || sel[0] instanceof CDtNode) {
                $('.edit', mcm).hide();
            }
        } else if (sel.length > 1) {
            showMenu();
            $('.edit', mcm).hide();
        }
    } else {
        // Otherwise, we're read only, so only popup the context menu for outputs with datasets
        if (sel.length == 1) {
            // @FIXME: second condition here will evaluate to true if IDs have the value "undefined",
            // since null !== undefined in javascript. Are these IDs always integers? 
            // If so, !isNaN(x) would work, or possibly (typeof parseInt(x) == 'number').
            if(sel[0] instanceof OutputNode && sel[0].dataset_id !== null) {
               // Context menu for pipeline outputs
               showMenu();
               $('.output_node', mcm).show();
               $('.step_node', mcm).hide();
            } else if(sel[0] instanceof MethodNode && sel[0].log_id !== null) {
               // Context menu for pipeline steps
               showMenu();
               $('.output_node', mcm).hide();
               $('.step_node', mcm).show();
            }
        }
    }
    this.doUp(e);
    e.preventDefault();
};

CanvasState.prototype.addShape = function(shape) {
    this.shapes.push(shape);
    if (shape instanceof MethodNode) {
        this.testExecutionOrder();
    }
    this.valid = false;
    return shape;
};

// Returns nothing, but sets CanvasState.exec_order and CanvasState.exec_order_is_ambiguous
CanvasState.prototype.testExecutionOrder = function() {
    /*
        gather method nodes which have no method-node parents
        set as phase 0
        gather method nodes which are children of phase 1 methods
        eliminate method nodes which have parents not in an existing phase (0..n)
        set as phase 1
        etc...
    */

    var shapes = this.shapes,
        shape,
        phases = [],
        phase,
        n_methods = 0,
        L = 0,
        i, j, k,
        parent, 
        okay_to_add,
        found;
    
    // count up the total number of methods
    for ( i=0; i < shapes.length; i++ ) {
        if (shapes[i] instanceof MethodNode)
            n_methods++;
    }

    // esoteric syntax: label before a loop allows the statements "continue" and "break" to specify which loop they are continuing or breaking.
    fill_phases_ar: while (
            n_methods > phases.reduce( function(a,b) { return a + b.length }, 0 ) // Array.reduce lets us count up the number of methods in the phases array
            && L < 200 // sanity check... don't let this algorithm run away
        ) {
        phase = [];
        
        check_for_shape: for ( i=0; i < shapes.length; i++ ) {
            shape = shapes[i];
            if (!(shape instanceof MethodNode)) {
                continue;
            }
            
            for ( k = 0; k < phases.length; k++ ) {
                // search for parent in phases array
                if (phases[k].indexOf(shape) > -1) {
                    continue check_for_shape;
                }
            }
    
            okay_to_add = true;
            for ( j = 0; j < shape.in_magnets.length; j++ ) {
                
                // check if pipeline is incomplete
                // purposefully use fuzzy type coersion here: empty array will be 'false'
                if (shape.in_magnets[j].connected == false) {
                    // can't go any further in this case
                    phases = false;
                    break fill_phases_ar;
                }
                
                /*
                @debug
                if (typeof shape.in_magnets[j].connected[0].source == 'undefined') {                
                    console.group('debug');
                    console.log(shape);
                    console.log(shape.in_magnets);
                    console.log(shape.in_magnets[j]);
                    console.log(shape.in_magnets[j].connected);
                    console.log(shape.in_magnets[j].connected[0]);
                    console.log(shape.in_magnets[j].connected[0].source);
                    console.groupEnd();
                }*/
                
                parent = shape.in_magnets[j].connected[0].source.parent;
                
                if (!(parent instanceof MethodNode)) {
                    continue;
                }
                
                found = false;
                for ( k=0; k < phases.length; k++ ) {
                    // search for parent in phases array
                    if (phases[k].indexOf(parent) > -1) {
                        found = true;
                        break;
                    }
                }
                
                // if parent node has not been put in order yet,
                // then this node cannot be added.
                if (found === false) {
                    okay_to_add = false;
                    break;
                }
            }
        
            if (okay_to_add === true) phase.push(shape);
        }
        
        // check if pipeline is incomplete
        if (phase.length == 0) {
            // can't go any further in this case
            phases = false;
            break fill_phases_ar;
        }
        
        phases.push(phase);
        L++;
    }
    
    if (L >= 200) {
        console.log('DEBUG: Runaway topological sort algorithm');
        phases = false;
    }
    
    if (phases) {
        this.exec_order = phases;
        
        // get the maximum number of methods per phase
        // (.map counts the methods in each phase, while Math.max.apply finds the maximum and takes its input as an array rather than an argument list)
        // comparison operation 1< will be true if there is more than 1 step per phase.
        this.exec_order_is_ambiguous = 1 < Math.max.apply(null, phases.map(function(a) { return a.length }));
        
        if (this.exec_order_is_ambiguous) {
            this.disambiguateExecutionOrder();
        }
    } else {
        this.exec_order = false;
        this.exec_order_is_ambiguous = null;
    }
};

CanvasState.prototype.disambiguateExecutionOrder = function() {
    for ( k=0; k < this.exec_order.length; k++ ) {
        // @note: nodes also have dx and dy properties which are !== 0 when collisions were detected.
        // I have not accounted for these properties in this method because they could shift around
        // on window resize, and it makes no sense for the pipeline to change on window resize.
        this.exec_order[k].sort(Geometry.isometricSort);
    }
}

CanvasState.prototype.clear = function() {
    // wipe canvas content clean before redrawing
    this.ctx.clearRect(0, 0, this.width / this.scale, this.height / this.scale);
    this.ctx.textAlign = 'center';
    this.ctx.font = '12pt Lato, sans-serif';
};

CanvasState.prototype.reset = function() {
    // remove all objects from canvas
    this.clear();
    // reset containers to reflect canvas
    this.shapes = [];
    this.connectors = [];
    this.exec_order = [];
    this.selection = [];
};

CanvasState.prototype.draw = function() {
    /*
    Render pipeline objects to Canvas.
     */
    if (!this.valid) {
        var ctx = this.ctx,
            shapes = this.shapes,
            connectors = this.connectors,
            sel = this.selection,
            labels = [],
            i, j, l, L, textWidth, flat_exec_order, shape;
        this.clear();
        
        var draggingFromMethodOut = this.dragging 
            && sel.length == 1 
            && sel[0] instanceof Connector
            && sel[0].source.parent instanceof MethodNode;
        
        // draw output end-zone -when- dragging a connector from a MethodNode
        if (draggingFromMethodOut && this.can_edit) {
            this.outputZone.draw(this.ctx);
        }
        
        // draw all shapes and magnets
        for (i = 0; i < shapes.length; i++) {
            shape = shapes[i];
            // skip shapes moved off the screen
            if (shape.x > this.width / this.scale || 
                    shape.y > this.height / this.scale ||
                    shape.x + 2 * shape.r < 0 || 
                    shape.y + 2 * shape.r < 0) {
                continue;
            }
            
            shapes[i].draw(ctx);
            
            // queue label to be drawn after
            if (this.force_show_exec_order === false ||
                    !(shapes[i] instanceof MethodNode) ||
                    this.force_show_exec_order === undefined &&
                    !this.exec_order_is_ambiguous) {
                labels.push(shapes[i].getLabel());
            } else {
                // add information about execution order
                L = shapes[i].getLabel();
                flat_exec_order = [];
                for( j = 0; j < this.exec_order.length; j++) {
                    //"flatten" 2d array into 1d by concatenation.
                    flat_exec_order = flat_exec_order.concat(this.exec_order[j]);
                }
                L.label = (flat_exec_order.indexOf(shape) + 1) +': '+ L.label;
                labels.push(L);
            }
        }

        // draw all connectors
        ctx.globalAlpha = 0.75;
        for (i = 0; i < connectors.length; i++) {
            connectors[i].draw(ctx);
        }
        ctx.globalAlpha = 1.0;

        if (sel.length > 0) {
            // draw selection ring
            ctx.strokeStyle = this.selectionColor;
            ctx.lineWidth = this.selectionWidth * 2;
            ctx.font = '9pt Lato, sans-serif';
            ctx.textBaseline = 'middle';
            ctx.textAlign = 'center';
            for (i = 0; i < sel.length; i++) {
                sel[i].highlight(ctx, this.dragging);
            }
        }
        
        if (this.enable_labels) {
            // draw all labels
            ctx.textAlign = 'center';
            ctx.textBaseline = 'alphabetic';
            ctx.font = '10pt Lato, sans-serif';

            // to minimize canvas state changes, loop twice.
            // canvas state changes are computationally expensive.
            ctx.fillStyle = '#fff';
            ctx.globalAlpha = 0.4;
            for (i = 0; i < labels.length; i++) {
                l = labels[i],
                textWidth = ctx.measureText(l.label).width;
                ctx.fillRect(l.x - textWidth/2 - 1, l.y - 11, textWidth + 2, 14);
            }
            ctx.fillStyle = '#000';
            ctx.globalAlpha = 1.0;
            for (i = 0; i < labels.length; i++) {
                l = labels[i];
                ctx.fillText(l.label, l.x, l.y);
            }
        }
        
        this.valid = true;
    }
};

CanvasState.prototype.getPos = function(e) {
    // returns an object with x, y coordinates defined
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
    var mySel,
        sel,
        index = -1,
        i = 0,
        k = 0, // loop counters
        in_magnets = [],
        in_magnet,
        out_magnets = [],
        out_magnet,
        this_connector = null;
    
    if (typeof objectToDelete !== 'undefined') {
        mySel = [ objectToDelete ];
    } else {
        mySel = this.selection;
    }
    
    for (k=0; k < mySel.length; k++) {
        sel = mySel[k];
        
        if (sel !== null) {
            if (sel instanceof Connector) {
                // remove selected Connector from list
            
                // if a cable to an output node is severed, delete the node as well
                if (sel.dest.parent instanceof OutputNode) {
                    index = this.shapes.indexOf(sel.dest.parent);
                    this.shapes.splice(index, 1);
                } else {
                    // remove connector from destination in-magnet
                    index = sel.dest.connected.indexOf(sel);
                    sel.dest.connected.splice(index, 1);
                }

                // remove connector from source out-magnet
                index = sel.source.connected.indexOf(sel);
                sel.source.connected.splice(index, 1);

                // remove Connector from master list
                index = this.connectors.indexOf(sel);
                this.connectors.splice(index, 1);
            }
            else if (sel instanceof MethodNode) {
                // delete Connectors terminating in this shape
                in_magnets = sel.in_magnets;
                for (i = 0; i < in_magnets.length; i++) {
                    if (in_magnets[i].connected.length > 0) {
                        this.deleteObject(in_magnets[i].connected[0]);
                    }
                }

                // delete Connectors from this shape to other nodes
                out_magnets = sel.out_magnets;
                for (i = 0; i < out_magnets.length; i++) {
                    for (j = out_magnets[i].connected.length; j > 0; j--) {// this loop done in reverse so that deletions do not re-index the array
                        this.deleteObject(out_magnets[i].connected[j - 1]);
                    }
                }

                // remove MethodNode from list and any attached Connectors
                index = this.shapes.indexOf(sel);
                this.shapes.splice(index, 1);
            }
            else if (sel instanceof OutputNode) {
                // deleting an output node is the same as deleting the cable
                this_connector = sel.in_magnets[0].connected[0];
                this.deleteObject(this_connector);
            }
            else {  // CDtNode or RawNode
                out_magnets = sel.out_magnets;
                for (i = 0; i < out_magnets.length; i++) {
                    out_magnet = out_magnets[i];
                    for (j = 0; j < out_magnet.connected.length; j++) {
                        this_connector = out_magnets[i].connected[j];
                        index = this.connectors.indexOf(this_connector);
                        this.connectors.splice(index, 1);

                        if (this_connector.dest !== undefined && this_connector.dest instanceof Magnet) {
                            // in-magnets can accept only one Connector
                            this_connector.dest.connected = [];
                        }
                    }
                    out_magnet.connected = [];
                }
                index = this.shapes.indexOf(sel);
                this.shapes.splice(index, 1);
            }
        
            // see if this has changed the execution order
            this.testExecutionOrder();

            this.selection = [];
            this.valid = false; // re-draw canvas to make Connector disappear
        }
    }
};

CanvasState.prototype.findMethodNode = function(method_pk) {
    var shapes = this.shapes;
    for(var i = 0; i < shapes.length; i++)
        if (shapes[i] instanceof MethodNode && shapes[i].pk == method_pk)
            return shapes[i];
    return null;
}

CanvasState.prototype.findOutputNode = function(pk) {
    var shapes = this.shapes;
    for(var i = 0; i < shapes.length; i++)
        if (shapes[i] instanceof OutputNode && shapes[i].pk == pk)
            return shapes[i];
    return null;
}
