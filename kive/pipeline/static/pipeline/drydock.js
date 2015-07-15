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
        this.methods = []; // collection of methods to be drawn (subset of shapes)
        this.connectors = []; // collection of connectors between shapes
        this.dragging = false; // if mouse drag
    
        this.selection = []; // reference to active (selected) objects
        this.dragstart = { x: 0, y: 0 }; // where in the object we clicked
        this.dragoffx = 0;
        this.dragoffy = 0;
        
        this.exec_order = [];
        this.exec_order_is_ambiguous = null;
        this.exec_order_may_have_changed = false;
        
        this.collisions = 0;
    
        this.outputZone = new drydock_objects.OutputZone(this.width, this.height);
    
        // options
        this.selectionColor = '#7bf';
        this.selectionWidth = 2;
        
        // events
        var myState = this; // save reference to this particular CanvasState
        if (interval !== undefined) {
            setInterval(function() {
                if (myState.exec_order_may_have_changed) {
                    myState.disambiguateExecutionOrder();
                    myState.exec_order_may_have_changed = false;
                }
                if (!myState.valid) {
                    myState.draw();
                }
            }, interval);
        }
    
        // Parameters on data-x
        this.can_edit = ($(canvas).data('editable') !== false);
    };

    function getStyle(css, name) {
        var value = css.getPropertyValue(name);
        if (value === '') {
            return 0;
        }
        return parseInt(value, 10);
    }

    my.CanvasState.prototype.setScale = function(factor) {
        this.scale = factor;
        this.valid = false;
    };
    
    my.CanvasState.prototype.getMouseTarget = function(mx, my) {
        var shapes = this.shapes,
            connectors = this.connectors;
    
        // did we click on a shape?
        // check shapes in reverse order
        for (var i = shapes.length - 1; i >= 0; i--) {
            if (shapes[i].contains(mx, my)) {
                // each shape checks to see if its own magnets were clicked
                return shapes[i].getMouseTarget(mx, my, true);
            }
        }
        
        // did we click on a Connector?
        // check this -after- checking shapes, as the algorithm is slower.
        for (i = connectors.length - 1; i >= 0; i--) {
            if (connectors[i].contains(mx, my, 5)) {
                return connectors[i];
            }
        }
        
        return false;
    };
    
    my.CanvasState.prototype.doDown = function(e) {
        var pos = this.getPos(e),
            mx = pos.x, my = pos.y,
            mySel = this.getMouseTarget(mx, my);
        
        if (mySel === false) {
            if (!e.shiftKey) {
                // nothing clicked
                this.selection = [];
                this.valid = false;
                $('#id_method_button').val('Add Method');
            }
            return false;
        }
        
        // Check if object is already selected (or if shift key is held)
        if (e.shiftKey || this.selection.indexOf(mySel) == -1) {
            mySel.doDown(this, e);
        }
        this.dragstart = pos;
        this.dragging = true;
        this.valid = false; // activate canvas
    };
    
    my.CanvasState.prototype.doMove = function(e) {
        /*
         event handler for mouse motion over canvas
         */
        var mouse = this.getPos(e),
            methods = this.methods,
            dx, dy,
            i, j, k,
            method, 
            sel, 
            source_shape, 
            in_magnet;
        
        if (this.dragging) {
            this.valid = false; // redraw

            // are we carrying a shape or Connector?
            for (j = 0; (sel = this.selection[j]); j++) {
                // are we carrying a connector?
                if (sel instanceof drydock_objects.Connector && this.can_edit) {
                    // reset to allow mouse to disengage Connector from a magnet
                    sel.x = mouse.x;
                    sel.y = mouse.y;

                    // get this connector's shape
                    source_shape = sel.source.parent;
                    
                    if (sel.dest !== undefined && sel.dest !== null) {
                        sel.dest.connected = [];
                        sel.dest = null;
                    }

                    // check if connector has been dragged to an in-magnet
                    for (i = 0; (method = methods[i]); i++) {
                        // disallow self-referential connections
                        if (source_shape !== undefined && method === source_shape) {
                            continue;
                        }

                        for (k = 0; (in_magnet = method.in_magnets[k]); k++) {
                            // light up magnet
                            if (in_magnet.connected.length === 0 && sel.source.cdt === in_magnet.cdt) {
                                in_magnet.acceptingConnector = true;
                            }
                        }

                        in_magnet = method.getMouseTarget(mouse.x, mouse.y);

                        // does this in-magnet accept this CompoundDatatype?
                        // cdt is null if own_shape is a RawNode
                        if (in_magnet && sel.source.cdt === in_magnet.cdt) {
                            in_magnet.tryAcceptConnector(sel);
                        }
                    }
                } else {
                    // carrying a shape

                    // update coordinates of this shape/connector
                    // any changes made by the collision detection algorithm on this shape are now made "official"
                    dx = mouse.x - this.dragstart.x;
                    dy = mouse.y - this.dragstart.y;
                    sel.x += dx + sel.dx;
                    sel.y += dy + sel.dy;
                    sel.dy = sel.dx = 0;

                    // if execution order is ambiguous, the tiebreaker is the y-position.
                    // dragging a method node needs to calculate this in real-time.
                    this.exec_order_may_have_changed |= this.exec_order_is_ambiguous && sel.affects_exec_order;
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
            scale = {
                x: (this.width  - margin.x * 2) / pipeline_width,
                y: (this.height - margin.y * 2) / pipeline_height
            };
            
        if (maintain_aspect_ratio) {
            scale.y = scale.x = Math.min(scale.x, scale.y);
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
    
    /*
     * Helper functions for autoLayout.
     */
    function matrixIndexOf(matrix, value) {
        for (var i = 0, j; i < matrix.length; i++) {
            for (j = 0; j < matrix[i].length; j++) {
                if (matrix[i][j] === value) {
                    return [ i, j ];
                }
            }
        }
        return false;
    }
    function addConnectedNodes(node, list) {
        // Follow a node's output cables to their connected nodes.
        // Insert these nodes in order into `list`.
        // Do not insert any duplicates into `list`.
        var connected_node;
        for (var i = 0, j; i < node.out_magnets.length; i++) {
            for (j = 0; j < node.out_magnets[i].connected.length; j++) {
                connected_node = node.out_magnets[i].connected[j].dest.parent;
                if (list.indexOf(connected_node) === -1) {
                    list.push(connected_node);
                }
            }
        }
    }
    function addConnectedNodesReverse(node, list, node_order) {
        // Reflexively insert input nodes into `list`.
        // Do not insert any duplicates into `list`.
        var connected_input;
        for (var i = 0, j; i < node.in_magnets.length; i++) {
            for (j = 0; j < node.in_magnets[i].connected.length; j++) {
                connected_input = node.in_magnets[i].connected[j].source.parent;
                if ((connected_input instanceof drydock_objects.RawNode || 
                        connected_input instanceof drydock_objects.CdtNode) && 
                        matrixIndexOf(node_order, connected_input) === false) {
                    list.push(connected_input);
                }
            }
        }
    }
    function insertIntoLayer(node, exec_order, list) {
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
    }
    my.CanvasState.prototype.autoLayout = function() {
        if (!this.exec_order) {
            return false;
        }
        
        var x_spacing = 60,
            y_spacing = 130,
            z_drop = 20,// not implemented yet. intent is for nodes to cascade down into each other like a series of waterfalls!
            layer = [],
            max_layer = this.exec_order[0].length,
            node,
            node_order = [],
            this_node,
            connected_nodes_in_order,
            magnets_in_order = [],
            i, j,
            state = this;
        
        for (i = 1; i < this.exec_order.length; i++) {
            if (this.exec_order[i].length > max_layer) {
                max_layer = this.exec_order[i].length;
            }
        }
        
        // First two layers are relatively easy.
        // Layer 1 is exec_order[0]. (Potentially with more input nodes which will be found later)
        // Layer 0 is any input node leading to exec_order[0].
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
        
        for (i = 0; i < this.exec_order.length; i++) {
            connected_nodes_in_order = [];
            layer = [];
            for (j = 0; (this_node = this.exec_order[i][j]); j++) {
                addConnectedNodes(this_node, connected_nodes_in_order);// connected_nodes_in_order will be added to here
                addConnectedNodesReverse(this_node, node_order[node_order.length - 2], node_order);// node_order[node_order.length - 2] will be added to here
            }
            for (j = 0; j < connected_nodes_in_order.length; j++) {
                insertIntoLayer(connected_nodes_in_order[j], this.exec_order[i+1], layer);// `layer` will be added to here
            }
            node_order.push(layer);
        }
        
        //x: x * 0.577350269 - y - i * spacing == 0
        //y: x * 0.577350269 + y - j * spacing == 0
        
        $(this.canvas).fadeOut({ complete: function() {
            var layer_length,
                layer_out_magnets,
                num_magnets;
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
                        node.draw(state.ctx);// needed to update magnet coords
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
            state.scaleToCanvas(true);// "true" to maintain aspect ratio
            state.centreCanvas();
            for (i = 0; i < state.shapes.length; i++) {
                state.detectCollisions(state.shapes[i]);
            }
            state.valid = false;
            $(this).fadeIn();
        }});
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
            shapes_plus = this.shapes.concat(this.outputZone),
            scale_width = this.canvas.width / this.scale,
            scale_height = this.canvas.height / this.scale,
            i, j,
            shape,
            vertex,
            my_x, my_y,
            sh_x, sh_y,
            dx, dy,
            step = 5,
            dh,
            angle,
            implemented_bias,
            Dx, Dy;
            
        // Bias defines how much to move myShape vs how much to move the shape it collided with.
        // 1 would be 100% myShape movement, 0 would be 100% other shape movement, and everything
        // else in-between is possible.
        if (bias === undefined || bias === null) bias = 0.75;
        // output zone is not allowed to move
        if (myShape === this.outputZone) implemented_bias = 0;
        
        for (i = 0; (shape = shapes_plus[i]); i++) {
            // Objects are passed by reference in JS, so this comparison is really comparing references.
            // Identical objects at different memory addresses will not pass this condition.
            if (shape === myShape) continue;

            if (implemented_bias === undefined) {
                // output zone is not allowed to move
                // returns 1 if shape is output zone. otherwise go with pre-set bias.
                implemented_bias = shape === this.outputZone || bias;
            }
            
            for (j = 0; (vertex = vertices[j]); j++) {
                while (shape.contains(vertex.x, vertex.y)) {
                    // If a collision is detected and we start moving myShape, we have to re-check all previous shapes as well.
                    // We do this by resetting the counter.
                    // We also have to check for collisions on the other shape.
                    if (i > -1) {
                        i = -1;
                        if (followups.indexOf(shape) === -1) {
                            followups.push(shape);
                        }
                    }

                    // Drawing a line between the two objects' centres, move the centre 
                    // of mySel to extend this line while keeping the same angle.
                    my_x = myShape.x + myShape.dx;
                    my_y = myShape.y + myShape.dy;
                    sh_x = shape.x + shape.dx;
                    sh_y = shape.y + shape.dy;
                    dx = my_x - sh_x;
                    dy = my_y - sh_y;
                    dh = (dx < 0 ? -1 : 1) * (Math.sqrt(dx*dx + dy*dy) + step);// add however many additional pixels you want to move
                    angle = dx ? Math.atan(dy / dx) : Math.PI/2;
                    Dx = Math.cos(angle) * dh - dx;
                    Dy = Math.sin(angle) * dh - dy;
                    
                    myShape.dx += Dx * implemented_bias;
                    shape.dx   -= Dx * (1 - implemented_bias);
                    my_x = myShape.x + myShape.dx;
                    sh_x = shape.x + shape.dx;
                    
                    // do not allow shape to exceed canvas boundaries
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
                    
                    myShape.dy += Dy * implemented_bias;
                    shape.dy -= Dy * (1 - implemented_bias);
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
        this.$dialog = this.$dialog || $("#dialog_form");
        $(this.canvas).css("cursor", "auto");

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
            dialog_height = this.$dialog.height(),
            dialog_width = this.$dialog.width();
        
        // Collision detection!
        // Note: algorithm now includes collisions with OutputZone.
        for (i = 0; this.dragging && i < this.selection.length; i++) {
            if (typeof this.selection[i].getVertices == 'function') {
                this.detectCollisions(this.selection[i]);
            }
        }
        
        this.dragging = false;
        
        if (this.selection[0] instanceof drydock_objects.Connector) {
            connector = this.selection[0];

            if (connector.dest instanceof drydock_objects.Magnet) {
                // connector has been linked to an in-magnet
                // update source magnet
                if (connector.source.connected.indexOf(connector) < 0) {
                    connector.source.connected.push(connector);
                }
    
                // update destination magnet
                if (connector.dest.connected.indexOf(connector) < 0) {
                    connector.dest.connected.push(connector);
                }
            } else {
                // connector not yet linked to anything
            
                if (this.outputZone.contains(connector.x, connector.y)) {
                    // Connector drawn into output zone
                    if (!(connector.source.parent instanceof drydock_objects.MethodNode)) {
                        // disallow Connectors from data node directly to end-zone
                        index = this.connectors.indexOf(connector);
                        this.connectors.splice(index, 1);
                        this.selection = [];
                    } else {
                        // valid Connector, assign non-null value
                        new_output_label = this.uniqueNodeName(connector.source.label, drydock_objects.OutputNode);
                        out_node = connector.spawnOutputNode(new_output_label);
                        this.detectCollisions(out_node);
                        this.addShape(out_node);
    
                        // spawn dialog for output label
                        this.$dialog
                            .data('node', out_node)
                            .show()
                            .css({
                                left: out_node.x + out_node.dx - dialog_width/2,
                                top:  out_node.y + out_node.dy
                            })
                        .find('#output_name')
                            .val(new_output_label)
                            .select(); // default value;
                    }
                } else {
                    // Connector not linked to anything - delete
                    connector.deleteFrom(this);
                    this.selection = [];
                }
            } 
        }
            
        // see if this has changed the execution order
        this.testExecutionOrder();
    
        // turn off all in-magnets
        for (i = 0; i < this.shapes.length; i++) {
            this.shapes[i].unlightMagnets();
        }
    };
    
    my.CanvasState.prototype.uniqueNodeName = function(desired_name, object_class) {
        var suffix = 0,
            name = desired_name,
            shape;
        for (var i = 0; i< this.shapes.length; i++) {
            shape = this.shapes[i];
            if (object_class && !(shape instanceof object_class)) {
                continue;
            }
            if (shape.label == name) {
                i = -1;
                suffix++;
                name = desired_name +'_'+ suffix;
            }
        }
        return name;
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
        if (this.can_edit && sel[0] instanceof drydock_objects.Node) {
            showMenu();
            if (sel.length > 1 || 
                    sel[0] instanceof drydock_objects.RawNode ||
                    sel[0] instanceof drydock_objects.CdtNode) {
                $('.edit', mcm).hide();
            }
        } else if (sel.length == 1) {
            // Otherwise, we're read only, so only popup the context menu for outputs with datasets
            if (sel[0] instanceof drydock_objects.OutputNode &&
                sel[0].dataset_id) {

               // Context menu for pipeline outputs
               showMenu();
               $('.step_node', mcm).hide();

            }
            else if (sel[0] instanceof drydock_objects.MethodNode &&
                     sel[0].log_id) {

               // Context menu for pipeline steps
               showMenu();
               $('.output_node', mcm).hide();
            }
        }
        this.doUp(e);
        e.preventDefault();
    };
    
    my.CanvasState.prototype.addShape = function(shape) {
        this.shapes.push(shape);
        if (shape instanceof drydock_objects.MethodNode) {
            this.methods.push(shape);
            this.testExecutionOrder();
        }
        this.valid = false;
        return shape;
    };
    
    function migrateConnectors(from_node, to_node) {
        var migrateInputs  = migrateFnUsing('inputs',  'in_magnets',  'dest',   'push'),
            migrateOutputs = migrateFnUsing('outputs', 'out_magnets', 'source', 'unshift');
        
        $.each(from_node.inputs, migrateInputs);
        $.each(from_node.outputs, migrateOutputs);
        
        // inner helper function does the bulk of the work 
        // 4 arguments tell it which properties to use
        // modifies from_node and to_node in parent's scope
        function migrateFnUsing(xputs_prop, magnets_prop, terminal_prop, shift_dir) {
            return function() {
                var old_xput = this,
                    old_didx_s1 = old_xput.dataset_idx - 1,
                    old_xput_connections = from_node[magnets_prop][old_didx_s1].connected,
                    new_xput = to_node[xputs_prop][old_didx_s1],
                    xputs_are_matching_cdts = new_xput &&
                        new_xput.structure !== null && old_xput.structure !== null &&
                        new_xput.structure.compounddatatype == old_xput.structure.compounddatatype,
                    xputs_are_raw = new_xput &&
                        new_xput.structure === null && old_xput.structure === null,
                    connector;// temp variable

                if (xputs_are_raw || xputs_are_matching_cdts) {
                    // re-attach all Connectors
                    while (old_xput_connections.length > 0) {
                        connector = old_xput_connections.pop();
                        connector[terminal_prop] = to_node[magnets_prop][old_didx_s1];
                        connector[terminal_prop].connected[shift_dir](connector);
                    }
                }
                /*
                This code block may be useful if we want to provide 
                more specific info about the mismatch? Variable must
                be returned by migrateConnectors.
                else {}
                */
            };
        }
    }
    
    my.CanvasState.prototype.replaceMethod = function(old_method, new_method) {
        var was_fully_connected = old_method.isFullyConnected();
        new_method.x = old_method.x;
        new_method.y = old_method.y;
        this.addShape(new_method);
        migrateConnectors(old_method, new_method);
        this.deleteObject(old_method);
        // this will detect a mismatch if any new xputs have been added
        return was_fully_connected !== new_method.isFullyConnected();
    };
    
    my.CanvasState.prototype.findNodeByLabel = function(label) {
        var found;
        for(var i = 0; i < this.shapes.length; i++) {
            if (this.shapes[i].label == label) {
                if (found !== undefined) {
                    throw new Error("Duplicate label: '" + label + "'.");
                }
                found = this.shapes[i];
            }
        }
        return found;
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
    
        var methods = this.methods,
            method,
            phases = [],
            phase,
            L = 0,
            i, j, k,
            parent, 
            okay_to_add,
            found;
    
        // esoteric syntax: label before a loop allows "continue" and "break" to specify which loop they are continuing or breaking.
        // check number of methods in the phases array
        // sanity check... don't let this algorithm run away
        fill_phases_ar: while (methods.length > totalLength(phases) && L < 200) {
            phase = [];
            
            check_for_shape: for ( i=0; (method = methods[i]); i++ ) {
                for ( j = 0; j < phases.length; j++ ) {
                    // search for parent in phases array
                    if (phases[j].indexOf(method) > -1) {
                        continue check_for_shape;
                    }
                }
        
                okay_to_add = true;
                for ( j = 0; j < method.in_magnets.length; j++ ) {
                    
                    // check if pipeline is incomplete
                    if (method.in_magnets[j].connected.length === 0) {
                        // can't go any further in this case
                        phases = false;
                        break fill_phases_ar;
                    }

                    parent = method.in_magnets[j].connected[0].source.parent;
                    if (methods.indexOf(parent) === -1) continue;
                    
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
            
                if (okay_to_add === true) phase.push(method);
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
            this.exec_order_may_have_changed |= this.exec_order_is_ambiguous;
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
    
    my.CanvasState.prototype.getSteps = function() {
        if ( ! this.exec_order) {
            this.testExecutionOrder();
        }
        var steps = [];
        $.each(this.exec_order, function() {
            steps = steps.concat(this);
        });
        return steps;
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
        this.methods = [];
        this.connectors = [];
        this.exec_order = [];
        this.selection = [];
    };
    
    my.CanvasState.prototype.draw = function() {
        /*
        Render pipeline objects to Canvas.
         */
        var ctx = this.ctx,
            shapes = this.shapes,
            connectors = this.connectors,
            sel = this.selection,
            labels = [],
            flat_exec_order = [],
            i, l, L, textWidth, shape;
        ctx.save();
        this.clear();
        ctx.scale(this.scale, this.scale);

        for( i = 0; i < this.exec_order.length; i++) {
            //"flatten" 2d array into 1d by concatenation.
            flat_exec_order = flat_exec_order.concat(this.exec_order[i]);
        }
        
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
        for (i = 0; (shape = shapes[i]); i++) {
            shape.draw(ctx);
            
            // queue label to be drawn after
            if (this.force_show_exec_order === false ||
                    !(shape instanceof drydock_objects.MethodNode) ||
                    this.force_show_exec_order === undefined &&
                    !this.exec_order_is_ambiguous) {
                labels.push(shape.getLabel());
            } else {
                // add information about execution order
                L = shape.getLabel();
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
            for (i = 0; (l = labels[i]); i++) {
                textWidth = ctx.measureText(l.label).width;
                ctx.fillRect(l.x - textWidth/2 - 2, l.y - 11, textWidth + 4, 14);
            }
            ctx.fillStyle = '#000';
            ctx.globalAlpha = 1.0;
            for (i = 0; (l = labels[i]); i++) {
                ctx.fillText(l.label, l.x, l.y);
            }
        }
        
        ctx.restore();
        this.valid = true;
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
        var sel, i;
        
        if (typeof objectToDelete !== 'undefined') {
            sel = [ objectToDelete ];
        } else {
            sel = this.selection;
        }
        
        for (i = 0; i < sel.length; i++) {
            if (typeof sel[i] == 'object' && typeof sel[i].deleteFrom == 'function') {
                sel[i].deleteFrom(this);
            }
        }
        // see if this has changed the execution order
        this.testExecutionOrder();
        this.selection = [];
        this.valid = false; // re-draw canvas to make Connector disappear
    };
    
    my.CanvasState.prototype.findMethodNode = function(method_pk) {
        var methods = this.methods;
        for(var i = 0; i < methods.length; i++) {
            if (methods[i].pk == method_pk) {
                return methods[i];
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
}());
