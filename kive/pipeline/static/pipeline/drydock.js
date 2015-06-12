/**
 * drydock.js
 *   Implements HTML5 Canvas interface for assembling
 *   pipelines from datatype and method nodes.
 *   Based on the canvas interactivity example by Simon
 *   Sarris, HTML5 Unleashed (2014) Pearson Education Inc.
 */
var drydock = (function() {
    "use strict";
    var my = {};
    
    /**
     * HTML5 Canvas interface for assembling pipelines.
     * 
     * Builds pipelines from the input, method, and output nodes defined in
     * drydock_objects.js. Based on the canvas interactivity example by Simon
     * Sarris, HTML5 Unleashed (2014) Pearson Education Inc.
     * 
     * @param canvas: the canvas element to draw on
     * @param interval: the number of milliseconds between calls to draw(),
     *  or undefined if no automatic scheduling is needed.
     */
    my.CanvasState = function(canvas, interval) {
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
            var css = getComputedStyle(canvas, null);
            this.stylePaddingLeft = getStyle(css, 'padding-left');
            this.stylePaddingTop = getStyle(css, 'padding-top');
            this.styleBorderLeft = getStyle(css, 'border-left-width');
            this.styleBorderTop = getStyle(css, 'border-top-width');
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
    
        this.outputZone = new drydock_objects.OutputZone(this.width, this.height);
    
        // options
        this.selectionColor = '#7bf';
        this.selectionWidth = 2;
        
        // events
        var myState = this; // save reference to this particular CanvasState
        if (interval !== undefined) {
            setInterval(function() { myState.draw(); }, interval);
        }
    
        // Parameters on data-x
        this.can_edit = ($(canvas).data('editable') !== false);
    };

    function getStyle(css, name) {
        var value = css.getPropertyValue(name);
        if (value === '') {
            return 0;
        }
        return parseInt(value);
    }

    my.CanvasState.prototype.setScale = function(factor) {
        this.scale = factor;
        this.ctx.scale(factor, factor);
    };
    
    my.CanvasState.prototype.getMouseTarget = function(mx, my) {
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
    
    my.CanvasState.prototype.doDown = function(e) {
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
        
        // Clicking on an object that is already selected? Start dragging.
        if (!shift && this.selection.indexOf(mySel) > -1) {
            this.dragstart = { x: mx, y: my };
            this.dragging = true;
            this.valid = false; // activate canvas
            return;
        }
        
        if (mySel instanceof drydock_objects.Magnet) {
            if (mySel.isInput) {
                if (mySel.connected.length > 0) {
                    mySel = mySel.connected[0]; // select connector instead
                } else {
                    mySel = mySel.parent; // select magnet's parent instead
                }
            }
            else if (shift && this.selection.length !== 0 || ! this.can_edit){
                // out magnet that can't create a connector
                mySel = mySel.parent;
            }
        }
        if (mySel instanceof drydock_objects.Connector &&
                mySel.dest &&
                mySel.dest.parent instanceof drydock_objects.OutputNode) {
            // if the cable leads to an output node, then act as if
            // the output node itself was clicked...
            // not sure if this is the ideal behaviour...
            // maybe output nodes should just disappear when they are
            // disconnected? -JN
            mySel = mySel.dest.parent;
        }
        if (mySel instanceof drydock_objects.Magnet) {
            // The only way to get here is with an out magnet we want to create
            // a connector for.
            var conn = new drydock_objects.Connector(mySel);
            this.connectors.push(conn);
            mySel.connected.push(conn);
            this.selection = [ conn ];
            this.dragoffx = mx - conn.fromX;
            this.dragoffy = my - conn.fromY;
        }
        else if (mySel instanceof drydock_objects.Connector) {
            if (!shift || this.selection.length === 0) {
                this.selection = [ mySel ];
                if(this.can_edit){
                    this.dragoffx = this.dragoffy = 0;
                } else {
                    this.dragging = false;
                    return;
                }
            }
        }
        else {
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
        
            if (mySel instanceof drydock_objects.MethodNode) {
                $('#id_method_button').val('Revise Method');
            }
        }
        
        this.dragstart = { x: mx, y: my };
        this.dragging = true;
        this.valid = false; // activate canvas
        return;
    };
    
    my.CanvasState.prototype.doMove = function(e) {
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
            for (j = 0; j < this.selection.length; j++) {
                sel = this.selection[j];
                
                // update coordinates of this shape/connector
                sel.x += dx;
                sel.y += dy;
                
                // any changes made by the collision detection algorithm on this shape are now made "official"
                sel.x += sel.dx;
                sel.dx = 0;
                sel.y += sel.dy;
                sel.dy = 0;
                
                this.valid = false; // redraw

                // are we carrying a connector?
                if (sel instanceof drydock_objects.Connector && this.can_edit) {
                    // reset to allow mouse to disengage Connector from a magnet
                    
                    sel.x = mouse.x;
                    sel.y = mouse.y;
                    
                    if (sel.dest) {
                        sel.dest.connected = [];
                        sel.dest = null;
                    }

                    // get this connector's shape
                    var own_shape = sel.source.parent;

                    // check if connector has been dragged to an in-magnet
                    for (i = 0; i < shapes.length; i++) {
                        shape = shapes[i];
                    
                        // ignore Connectors, RawNodes, CDtNodes
                        // and disallow self-referential connections
                        if (typeof shape.in_magnets === 'undefined'  ||
                                shape.in_magnets.length === 0 ||
                                typeof own_shape !== 'undefined' &&
                                shape === own_shape) {
                            continue;
                        }

                        // shape is a Method, check its in-magnets
                        var in_magnets = shape.in_magnets,
                            connector_carrying_cdt;
                    
                        for (j = 0; j < in_magnets.length; j++) {
                            var in_magnet = in_magnets[j];

                            // retrieve CompoundDatatype of out-magnet
                            if (own_shape instanceof drydock_objects.RawNode) {
                                connector_carrying_cdt = null;
                            } else {
                                connector_carrying_cdt = sel.source.cdt;
                            }
                            // does this in-magnet accept this CompoundDatatype?
                            if (shape instanceof drydock_objects.MethodNode &&
                                connector_carrying_cdt == in_magnet.cdt) {
                                // light up magnet
                                in_magnet.fill = '#ff8';
                                in_magnet.acceptingConnector = true;
                                if (in_magnet.connected.length === 0 &&
                                        in_magnet.contains(sel.x, sel.y)) {
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
                    if (this.exec_order_is_ambiguous && sel instanceof drydock_objects.MethodNode) {
                        this.disambiguateExecutionOrder();
                    }
                }
            }
            
            this.dragstart = mouse;
        }
        
    };
    
    my.CanvasState.prototype.scaleToCanvas = function(maintain_aspect_ratio) {
        // general strategy: get the x and y coords of every shape, then get the max and mins of these sets.
        var x_ar = [], y_ar = [],
            margin = {
                x: Math.min(this.width  * 0.15, 100),
                y: Math.min(this.height * 0.15, 100)
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
            
        if (maintain_aspect_ratio) {
            if (scale.x < scale.y) {
                scale.y = scale.x;
            } else {
                scale.x = scale.y;
            }
        }
        
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
    
    my.CanvasState.prototype.centreCanvas = function() {
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
    
    my.CanvasState.prototype.autoLayout = function() {
        var x_spacing = 60,
            y_spacing = 130,
            z_drop = 20,// not implemented yet. intent is for nodes to cascade down into each other like a series of waterfalls!
            layer = [], layer_length, max_layer = this.exec_order[0].length,
            node, node_order = [], this_node, connected_nodes_in_order,
            magnets_in_order = [], layer_out_magnets, num_magnets,
            i, j, position;
        
        for (i = 1; i < this.exec_order.length; i++) {
            if (this.exec_order[i].length > max_layer) {
                max_layer = this.exec_order[i].length;
            }
        }
        
        // First two layers are relatively easy.
        // Layer 1 is exec_order[0]. (Potentially with more input nodes which will be found later)
        // Layer 0 is any input node leading to exec_order[0].
        magnets_in_order = [];
        layer = [];
        for (i = 0; i < this.exec_order[0].length; i++) {
            magnets_in_order = magnets_in_order.concat(this.exec_order[0][i].in_magnets);
        }
        for (i = 0; i < magnets_in_order.length; i++) {
            for (j = 0; j < magnets_in_order[i].connected.length; j++) {
                node = magnets_in_order[i].connected[j].source.parent;
                if (layer.indexOf(node) === -1) {
                    layer.push(node);
                }
            }
        }
        node_order.push(layer);
        node_order.push(this.exec_order[0]);
        
        var matrixIndexOf = function(matrix, value) {
            for (var i = 0; i < matrix.length; i++) {
                for (var j = 0; j < matrix[i].length; j++) {
                    if (matrix[i][j] === value) {
                        return [ i, j ];
                    }
                }
            }
            return false;
        };
        var addConnectedNodes = function(node, list) {
            // Follow a node's output cables to their connected nodes.
            // Insert these nodes in order into `list`.
            // Do not insert any duplicates into `list`.
            var connected_node;
            for (var i = 0; i < node.out_magnets.length; i++) {
                for (var j = 0; j < node.out_magnets[i].connected.length; j++) {
                    connected_node = node.out_magnets[i].connected[j].dest.parent;
                    if (list.indexOf(connected_node) === -1) {
                        list.push(connected_node);
                    }
                }
            }
        };
        var addConnectedNodesReverse = function(node, list) {
            // Reflexively insert input nodes into `list`.
            // Do not insert any duplicates into `list`.
            var connected_input;
            for (var i = 0; i < node.in_magnets.length; i++) {
                for (var j = 0; j < node.in_magnets[i].connected.length; j++) {
                    connected_input = node.in_magnets[i].connected[j].source.parent;
                    if ((connected_input instanceof drydock_objects.RawNode || 
                            connected_input instanceof drydock_objects.CdtNode) && 
                            matrixIndexOf(node_order, connected_input) === false) {
                        list.push(connected_input);
                    }
                }
            }
        };
        var insertIntoLayer = function(node, exec_order, list) {
            // Insert a node into a list in a "smart" way.
            // * Checks for duplicate entries
            // * If `node` is a method which is not the next method in exec_order, insertion is deferred
            // * If `node` -is- the next method in exec_order, insert all the method nodes that were deferred.
            var i, method_nodes,
                queue = my.CanvasState.method_node_queue = my.CanvasState.method_node_queue || []; // queue is a static variable that persists across function calls
            if (list.indexOf(node) === -1) {
                if (node instanceof drydock_objects.MethodNode && 
                        typeof exec_order !== 'undefined' &&
                        exec_order.indexOf(node) > -1) {
                    method_nodes = list.filter(function(node) { return node instanceof drydock_objects.MethodNode; });
                    if (exec_order.length <= method_nodes.length) {
                        console.error("Unexpected number of methods in method_nodes.");
                    }
                    if (exec_order[method_nodes.length] === node) {
                        // We've found the method node we're looking for.
                        list.push(node);
                        method_nodes.push(node);
                        
                        // Clear the queue. Make sure we maintain exec_order.
                        while (queue.length > 0) {
                            i = queue.indexOf(exec_order[method_nodes.length]);
                            list.push(queue[i]);
                            method_nodes.push(queue[i]);
                            queue.splice(i,1);
                        }
                    } else {
                        // Not the method node next in exec_order. Reserve it until we find the right node.
                        queue.push(node);
                    }
                }
                else if (node instanceof drydock_objects.OutputNode) {
                    // Output nodes are not relevant to execution order.
                    list.push(node);
                }
            }
            return list;
        };
        
        for (i = 0; i < this.exec_order.length; i++) {
            connected_nodes_in_order = [];
            layer = [];
            for (j = 0; j < this.exec_order[i].length; j++) {
                this_node = this.exec_order[i][j];
                addConnectedNodes(this_node, connected_nodes_in_order);// connected_nodes_in_order will be added to here
                addConnectedNodesReverse(this_node, node_order[node_order.length - 2]);// node_order[node_order.length - 2] will be added to here
            }
            for (j = 0; j < connected_nodes_in_order.length; j++) {
                insertIntoLayer(connected_nodes_in_order[j], this.exec_order[i+1], layer);// `layer` will be added to here
            }
            node_order.push(layer);
        }
        
        //x: x * 0.577350269 - y - i * spacing == 0
        //y: x * 0.577350269 + y - j * spacing == 0
        
        for (j = 0; j < node_order.length; j++) {
            layer_length = node_order[j].length;
            layer_out_magnets = [];
            node_order[j].center_x = node_order[j].center_x || 0;
            
            for (i = 0; i < layer_length; i++ ) {
                node = node_order[j][i];
                node.x = (y_spacing * j + x_spacing * (i - layer_length/2) + node_order[j].center_x) / 1.154700538;
                node.y = (y_spacing * j - x_spacing * (i - layer_length/2) - node_order[j].center_x) / 2;// + y_drop * j;
                node.dx = node.dy = 0;
                
                if (isNaN(node.x) || isNaN(node.y) ) {
                    console.error("Autolayout failed!", node.label, j, i, layer_length, node_order[j], node_order[j].center_x);
                }
                
                if (node.out_magnets.length > 0) {
                    node.draw(this.ctx);// needed to update magnet coords
                    layer_out_magnets = layer_out_magnets.concat(node.out_magnets);
                }
            }
            
            // added candy: the isometric X centre of the layer after this one will be aligned with the centre of the magnets leading to its nodes.
            if (j !== node_order.length - 1) {
                num_magnets = layer_out_magnets.length;
                var avg = averageCoordinates(layer_out_magnets);
                node_order[j+1].center_x = Geometry.isometricXCoord(avg.x, avg.y);
                if ( isNaN(node_order[j+1].center_x) ) {
                    console.error("Autolayout failed!", layer_out_magnets, num_magnets);
                }
            }
        }
        
        this.scaleToCanvas(true);// argument is to maintain aspect ratio
        this.centreCanvas();
    //    this.testExecutionOrder(); // should not have changed
        for (i = 0; i < this.shapes.length; i++) {
            this.detectCollisions(this.shapes[i]);
        }
        this.valid = false;
    };
    
    /**
     * Calculate the average x and y coordinates from an array of node objects.
     * 
     * @param nodes: an array of objects that all have x and y attributes.
     * @return an object with x and y attributes for the average values
     */
    function averageCoordinates(nodes) {
        var sum = nodes.reduce(function(a, b) {
            return {x: a.x+b.x, y: a.y+b.y};
        });
        return { x: sum.x/nodes.length, y: sum.y/nodes.length };
    }
    
    /**
     * Align selected nodes along the named axis.
     * 
     * @param axis: a string from ["x", "y", "iso_x", "iso_y", "iso_z"]
     */
    my.CanvasState.prototype.alignSelection = function(axis) {
        /* @todo
         * if nodes are too close together then they will collide and then get pushed back out.
         * when this "push back out" happens, it should happen -only- on the axis of alignment.
         */
        var sel = this.selection,
            coords = [],
            i, center, diff;
        var getCoord = {
            x:     function(o) { return o.y; },
            y:     function(o) { return o.x; },
            iso_z: function(o) { return o.x; },
            iso_y: function(o) { return Geometry.isometricXCoord(o.x, o.y); },
            iso_x: function(o) { return Geometry.isometricYCoord(o.x, o.y); }
        };
        var setCoord = {
            x:     function(o,c) { o.y = c; },
            y:     function(o,c) { o.x = c; },
            iso_z: function(o,c) { o.x = c; },
            iso_y: function(o,c) {
                diff = Geometry.iso2twodim(Geometry.isometricXCoord(o.x, o.y) - c, 0);
                o.x -= diff.x;
                o.y -= diff.y;
            },
            iso_x: function(o,c) {
                diff = Geometry.iso2twodim(0, Geometry.isometricYCoord(o.x, o.y) - c);
                o.x -= diff.x;
                o.y -= diff.y;
            }
        };
        if (sel instanceof Array && sel.length > 0 && getCoord.hasOwnProperty(axis)) {
            for (i = 0; i < sel.length; i++) {
                coords.push(getCoord[axis](sel[i]));
            }
            center = coords.reduce(function(a,b) { return a+b; }) / coords.length;
            for (i = 0; i < sel.length; i++) {
                setCoord[axis](sel[i], center);
                sel[i].dx = sel[i].dy = 0;
                this.detectCollisions(sel[i]);
            }
            this.valid = false;
        }
    };
    
    my.CanvasState.prototype.detectCollisions = function(myShape, bias) {
        var followups = [],
            vertices = myShape.getVertices(),
            scale_width = this.canvas.width / this.scale,
            scale_height = this.canvas.height / this.scale;
            
        // Bias defines how much to move myShape vs how much to move the shape it collided with.
        // 1 would be 100% myShape movement, 0 would be 100% other shape movement, and everything
        // else in-between is possible.
        if (bias === undefined || bias === null) bias = 0.75;
        
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
    };
    
    my.CanvasState.prototype.doUp = function(e) {
        this.valid = false;
        if (this.$dialog === undefined) {
            this.$dialog = $("#dialog_form");
        }
        var index,
            sel,
            i,
            j,
            connector,
            suffix,
            new_output_label,
            out_node,
            shape,
            in_magnet,
            dialog_height = this.$dialog[0].offsetHeight,
            dialog_width = this.$dialog[0].offsetWidth;
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
        
        // are we carrying a shape?
        if (!(this.selection[0] instanceof drydock_objects.Connector)) {
            for (i = 0; i < this.selection.length; i++) {
                sel = this.selection[i];
                if (this.outputZone.contains(sel.x, sel.y)) {
                    // Shape dragged into output zone
                    sel.x = this.outputZone.x - sel.w;
                }
            }
        }
        
        if (this.selection[0] instanceof drydock_objects.Connector) {
            connector = this.selection[0];
            
            if (!(connector.dest instanceof drydock_objects.Magnet)) {
                // connector not yet linked to anything
            
                if (this.outputZone.contains(connector.x, connector.y)) {
                    // Connector drawn into output zone
                    if (!(connector.source.parent instanceof drydock_objects.MethodNode)) {
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
                            if ( ! (shape instanceof drydock_objects.OutputNode)) {
                                continue;
                            }
                            if (shape.label == new_output_label) {
                                i = -1;
                                suffix++;
                                new_output_label = connector.source.label +'_'+ suffix;
                            }
                        }
                    
                        out_node = new drydock_objects.OutputNode(
                                connector.x,
                                connector.y,
                                new_output_label);
                        this.addShape(out_node);
                    
                        connector.dest = out_node.in_magnets[0];
                        connector.dest.connected = [ connector ];
                    
                        out_node.y = this.outputZone.y + this.outputZone.h + out_node.h/2 + out_node.r2;// push out of output zone
                        out_node.x = connector.x;
                        this.valid = false;
    
                        // spawn dialog for output label
                        this.$dialog
                            .data('node', out_node)
                            .show()
                            .css({
                                left: Math.min(connector.x, this.outputZone.x + this.outputZone.w/2 - dialog_width/2 ) + this.pos_x,
                                top:  Math.min(connector.y - dialog_height/2, this.canvas.height - dialog_height) + this.pos_y
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
            
            } else if (connector.dest instanceof drydock_objects.Magnet) {
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
    
    my.CanvasState.prototype.contextMenu = function(e) {
        var pos = this.getPos(e),
            mcm = $('#method_context_menu'),
            sel = this.selection,
            showMenu = function() {
                mcm.show().css({ top: e.pageY, left: e.pageX });
                $('li', mcm).show();
            };
    
        // Edit mode can popup the context menu to delete and edit nodes
        if(this.can_edit){
            if (sel.length == 1 && !(sel[0] instanceof drydock_objects.Connector) ) {
                showMenu();
                if (sel[0] instanceof drydock_objects.RawNode ||
                        sel[0] instanceof drydock_objects.CdtNode) {
                    $('.edit', mcm).hide();
                }
            } else if (sel.length > 1) {
                showMenu();
                $('.edit', mcm).hide();
            }
        } else {
            // Otherwise, we're read only, so only popup the context menu for outputs with datasets
            if (sel.length == 1) {
                if(sel[0] instanceof drydock_objects.OutputNode &&
                        sel[0].dataset_id !== undefined) {
                   // Context menu for pipeline outputs
                   showMenu();
                   $('.output_node', mcm).show();
                   $('.step_node', mcm).hide();
    
                } else if(sel[0] instanceof drydock_objects.MethodNode &&
                        sel[0].log_id !== undefined) {
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
    
    my.CanvasState.prototype.addShape = function(shape) {
        this.shapes.push(shape);
        if (shape instanceof drydock_objects.MethodNode) {
            this.testExecutionOrder();
        }
        this.valid = false;
        return shape;
    };
    
    /**
     * Calculate the total length of all the phases in an array.
     * 
     * @param phases: an array of objects that all have a length property.
     */
    function totalLength(phases) {
        return phases.reduce(function(a, b) { return a + b.length; }, 0); 
    }
    
    // Returns nothing, but sets CanvasState.exec_order and CanvasState.exec_order_is_ambiguous
    my.CanvasState.prototype.testExecutionOrder = function() {
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
            if (shapes[i] instanceof drydock_objects.MethodNode)
                n_methods++;
        }
    
        // esoteric syntax: label before a loop allows the statements "continue" and "break" to specify which loop they are continuing or breaking.
        // check number of methods in the phases array
        // sanity check... don't let this algorithm run away
        fill_phases_ar: while (n_methods > totalLength(phases) && L < 200) {
            phase = [];
            
            check_for_shape: for ( i=0; i < shapes.length; i++ ) {
                shape = shapes[i];
                if (!(shape instanceof drydock_objects.MethodNode)) {
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
                    if (shape.in_magnets[j].connected.length === 0) {
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
                    
                    if (!(parent instanceof drydock_objects.MethodNode)) {
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
            if (phase.length === 0) {
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
            this.checkAmbiguousExecutionOrder();
            
            if (this.exec_order_is_ambiguous) {
                this.disambiguateExecutionOrder();
            }
        } else {
            this.exec_order = false;
            this.exec_order_is_ambiguous = null;
        }
    };

    /**
     * Check if the execution order is ambiguous.
     * 
     * Reads this.exec_order, and sets this.exec_order_is_ambiguous.
     */
    my.CanvasState.prototype.checkAmbiguousExecutionOrder = function() {
        // get the maximum number of methods per phase
        // (.map counts the methods in each phase, while Math.max.apply 
        // finds the maximum and takes its input as an array rather than an
        // argument list)
        // comparison operation 1< will be true if there is more than 1 step per phase.
        this.exec_order_is_ambiguous = 1 < Math.max.apply(
                null, 
                this.exec_order.map(function(a) { return a.length; }));
    };
    
    my.CanvasState.prototype.disambiguateExecutionOrder = function() {
        for (var k=0; k < this.exec_order.length; k++ ) {
            // @note: nodes also have dx and dy properties which are !== 0 when collisions were detected.
            // I have not accounted for these properties in this method because they could shift around
            // on window resize, and it makes no sense for the pipeline to change on window resize.
            this.exec_order[k].sort(Geometry.isometricSort);
        }
    };
    
    my.CanvasState.prototype.clear = function() {
        // wipe canvas content clean before redrawing
        this.ctx.clearRect(0, 0, this.width / this.scale, this.height / this.scale);
        this.ctx.textAlign = 'center';
        this.ctx.font = '12pt Lato, sans-serif';
    };
    
    my.CanvasState.prototype.reset = function() {
        // remove all objects from canvas
        this.clear();
        // reset containers to reflect canvas
        this.shapes = [];
        this.connectors = [];
        this.exec_order = [];
        this.selection = [];
    };
    
    my.CanvasState.prototype.draw = function() {
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
            
            var draggingFromMethodOut = (
                    this.dragging &&
                    sel.length == 1 &&
                    sel[0] instanceof drydock_objects.Connector &&
                    sel[0].source.parent instanceof drydock_objects.MethodNode);
            
            // draw output end-zone -when- dragging a connector from a MethodNode
            if (draggingFromMethodOut && this.can_edit) {
                this.outputZone.draw(this.ctx);
            }
            
            // draw all shapes and magnets
            for (i = 0; i < shapes.length; i++) {
                shape = shapes[i];
                
                shapes[i].draw(ctx);
                
                // queue label to be drawn after
                if (this.force_show_exec_order === false ||
                        !(shapes[i] instanceof drydock_objects.MethodNode) ||
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
                ctx.globalAlpha = 0.5;
                for (i = 0; i < labels.length; i++) {
                    l = labels[i];
                    textWidth = ctx.measureText(l.label).width;
                    ctx.fillRect(l.x - textWidth/2 - 2, l.y - 11, textWidth + 4, 14);
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
    
    my.CanvasState.prototype.getPos = function(e) {
        // returns an object with x, y coordinates defined
        var element = this.canvas, offsetX = 0, offsetY = 0, mx, my;
    
        if (typeof element.offsetParent !== 'undefined') {
            do {
                offsetX += element.offsetLeft;
                offsetY += element.offsetTop;
            } while ((element = element.offsetParent));
        }
    
        offsetX += this.stylePaddingLeft + this.styleBorderLeft + this.htmlLeft;
        offsetY += this.stylePaddingTop + this.styleBorderTop + this.htmlTop;
    
        mx = e.pageX - offsetX;
        my = e.pageY - offsetY;
    
        return { x: mx, y: my };
    };
    
    my.CanvasState.prototype.deleteObject = function(objectToDelete) {
        // delete selected object
        // @param objectToDelete optionally specifies which object should be deleted.
        // Otherwise just go with the current selection.
        var mySel,
            sel,
            index = -1,
            i,
            j,
            k, // loop counters
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
            
            if (sel !== null && sel !== undefined) {
                if (sel instanceof drydock_objects.Connector) {
                    // remove selected Connector from list
                
                    // if a cable to an output node is severed, delete the node as well
                    if (sel.dest.parent instanceof drydock_objects.OutputNode) {
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
                else if (sel instanceof drydock_objects.MethodNode) {
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
                else if (sel instanceof drydock_objects.OutputNode) {
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
    
                            if (this_connector.dest !== undefined &&
                                    this_connector.dest instanceof drydock_objects.Magnet) {
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
    
    my.CanvasState.prototype.findMethodNode = function(method_pk) {
        var shapes = this.shapes;
        for(var i = 0; i < shapes.length; i++) {
            if (shapes[i] instanceof drydock_objects.MethodNode && shapes[i].pk == method_pk) {
                return shapes[i];
            }
        }
        return null;
    };
    
    my.CanvasState.prototype.findOutputNode = function(pk) {
        var shapes = this.shapes;
        for(var i = 0; i < shapes.length; i++) {
            if (shapes[i] instanceof drydock_objects.OutputNode && shapes[i].pk == pk) {
                return shapes[i];
            }
        }
        return null;
    };
    
    return my;
}(drydock));
