/**
 * drydock.js
 *   Implements HTML5 Canvas interface for assembling
 *   pipelines from datatype and method nodes.
 *   Based on the canvas interactivity example by Simon
 *   Sarris, HTML5 Unleashed (2014) Pearson Education Inc.
 */
"use strict";
import { Geometry } from "./geometry";
import { Nodes } from "./drydock_objects";
import { Point, Rectangle } from "./ShapeTypes";
declare var $: any;

class CanvasState {
    /**
     * HTML5 Canvas interface for assembling pipelines.
     * 
     * Builds pipelines from the input, method, and output nodes defined in
     * Nodes.js. Based on the canvas interactivity example by Simon
     * Sarris, HTML5 Unleashed (2014) Pearson Education Inc.
     * 
     * @param canvas: the canvas element to draw on
     * @param interval: the number of milliseconds between calls to draw(),
     *  or undefined if no automatic scheduling is needed.
     */
     
    // fixes issues with mouse coordinates
    stylePaddingLeft = 0;
    stylePaddingTop = 0;
    styleBorderLeft = 0;
    styleBorderTop = 0;

    scale = 1;
    enable_labels = true;

    valid = false; // if false, canvas will redraw everything
    shapes = []; // collection of shapes to be drawn
    methods = []; // collection of methods to be drawn (subset of shapes)
    inputs = []; // collection of inputs to be drawn (subset of shapes)
    outputs = []; // collection of outputs to be drawn (subset of shapes)
    connectors = []; // collection of connectors between shapes
    dragging = false; // if mouse drag

    selection = []; // reference to active (selected) objects
    dragstart = { x: 0, y: 0 }; // where in the object we clicked
    dragoffx = 0;
    dragoffy = 0;

    exec_order = [];
    exec_order_is_ambiguous:number = null;
    exec_order_may_have_changed = 0;
    input_order_may_have_changed = 0;
    force_show_exec_order: number;
    has_unsaved_changes = 0;

    collisions = 0;

    // options
    selectionColor = '#7bf';
    selectionWidth = 2;

    width: number;
    height: number;
    pos_x: number;
    pos_y: number;
    ctx: CanvasRenderingContext2D;
    htmlTop:number;
    htmlLeft: number;
    outputZone: Nodes.OutputZone;
    can_edit: boolean;

    mouse_highlight: Nodes.Magnet;
    $dialog: any;

    static method_node_queue: Nodes.MethodNode[] = [];
    
    constructor(private canvas, interval) {
        /*
        keeps track of canvas state (mouse drag, etc.)
         */
    
        // initialize "class"
        this.width = this.canvas.width;
        this.height = this.canvas.height;
        this.pos_y = this.canvas.offsetTop;
        this.pos_x = this.canvas.offsetLeft;
        this.ctx = this.canvas.getContext('2d');

        function getStyle(css, name): number {
            var value = css.getPropertyValue(name);
            if (value === '') {
                return 0;
            }
            return parseInt(value, 10);
        }
        if (window.getComputedStyle) {
            var css = getComputedStyle(canvas, null);
            this.stylePaddingLeft = getStyle(css, 'padding-left');
            this.stylePaddingTop  = getStyle(css, 'padding-top');
            this.styleBorderLeft  = getStyle(css, 'border-left-width');
            this.styleBorderTop   = getStyle(css, 'border-top-width');
        }
    
        // adjust for fixed-position bars at top or left of page
        var html:any = document.body.parentNode;
        this.htmlTop = html.offsetTop;
        this.htmlLeft = html.offsetLeft;
    
        this.outputZone = new Nodes.OutputZone(this.width, this.height);
        
        // events
        var interval_fn = () => {
            if (this.exec_order_may_have_changed) {
                this.disambiguateExecutionOrder();
                this.exec_order_may_have_changed = 0;
            }
            if (this.input_order_may_have_changed) {
                this.inputs.sort(Geometry.isometricSort);
                this.input_order_may_have_changed = 0;
            }
            if (!this.valid) {
                this.draw();
            }
        };
        interval_fn();
        if (interval !== undefined) {
            setInterval(interval_fn, interval);
        }
    
        // Parameters on data-x
        this.can_edit = $(canvas).data('editable') !== false;
    }

    setScale(factor: number): void {
        this.scale = factor;
        this.valid = false;
    }
    
    getMouseTarget (mx: number, my: number): Nodes.Node|Nodes.Magnet|Nodes.Connector {
        var shapes = this.shapes,
            connectors = this.connectors;
    
        // did we click on a shape?
        // check shapes in reverse order
        for (let i = shapes.length - 1; i >= 0; i--) {
            if (shapes[i].contains(mx, my)) {
                // each shape checks to see if its own magnets were clicked
                return shapes[i].getMouseTarget(mx, my, true);
            }
        }
        
        // did we click on a Connector?
        // check this -after- checking shapes, as the algorithm is slower.
        for (let i = connectors.length - 1; i >= 0; i--) {
            if (connectors[i].contains(mx, my, 5)) {
                return connectors[i];
            }
        }
        
        return null;
    }
    
    doDown (e) {
        var pos = this.getPos(e),
            mx = pos.x, my = pos.y,
            mySel = this.getMouseTarget(mx, my);
        
        if (mySel === null) {
            if (!e.shiftKey) {
                // nothing clicked
                this.selection = [];
                this.valid = false;
                let method_button = document.getElementById('id_method_button');
                if (method_button) {
                    method_button.setAttribute('value', 'Add Method');
                }
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
    }
    
    doMove (e): void {
        /*
         event handler for mouse motion over canvas
         */
        var mouse = this.getPos(e);
        
        if (this.dragging) {
            this.valid = false; // redraw

            // are we carrying a shape or Connector?
            for (let sel of this.selection) {
                // are we carrying a connector?
                if (sel instanceof Nodes.Connector) {
                    if (this.can_edit) {
                        // reset to allow mouse to disengage Connector from a magnet
                        sel.x = mouse.x;
                        sel.y = mouse.y;

                        // get this connector's shape
                        let source_shape = sel.source.parent;

                        if (sel.dest !== undefined && sel.dest !== null) {
                            sel.dest.connected = [];
                            sel.dest = null;
                        }

                        // check if connector has been dragged to an in-magnet
                        for (let shape of this.shapes) {
                            // disallow self-referential connections
                            if (source_shape !== undefined && shape === source_shape) {
                                continue;
                            }

                            for (let in_magnet of shape.in_magnets) {
                                // light up magnet
                                if (in_magnet.connected.length === 0 && sel.source.cdt === in_magnet.cdt) {
                                    in_magnet.acceptingConnector = true;
                                }
                            }

                            let in_magnet = shape.getMouseTarget(mouse.x, mouse.y);

                            // does this in-magnet accept this CompoundDatatype?
                            // cdt is null if own_shape is a RawNode
                            if (in_magnet && sel.source.cdt === in_magnet.cdt) {
                                in_magnet.tryAcceptConnector(sel);
                            }
                        }
                    }
                } else {
                    // carrying a shape

                    // update coordinates of this shape/connector
                    // any changes made by the collision detection algorithm on this shape are now made "official"
                    let dx = mouse.x - this.dragstart.x;
                    let dy = mouse.y - this.dragstart.y;
                    sel.x += dx + sel.dx;
                    sel.y += dy + sel.dy;
                    sel.dy = sel.dx = 0;

                    // if execution order is ambiguous, the tiebreaker is the y-position.
                    // dragging a method node needs to calculate this in real-time.
                    this.exec_order_may_have_changed |= this.exec_order_is_ambiguous && sel.affects_exec_order;
                    this.input_order_may_have_changed |= +(sel instanceof Nodes.CdtNode || sel instanceof Nodes.RawNode);
                }
            }
            
            this.dragstart = mouse;
        }
        
        let was_highlighted = this.mouse_highlight instanceof Nodes.Magnet;
        this.mouse_highlight = null;
        for (let shape of this.shapes) {
            let magnet = shape.getMouseTarget(mouse.x, mouse.y);
            if (magnet instanceof Nodes.Magnet && 
                    magnet.connected.length === 0) {
                if (magnet !== this.mouse_highlight) {
                    this.mouse_highlight = magnet;
                    this.valid = false;
                }
                break;
            }
        }
        if (was_highlighted && !this.mouse_highlight) {
            this.valid = false;
        }
    }
    
    scaleToCanvas (maintain_aspect_ratio?: boolean): void {
        // general strategy: get the x and y coords of every shape, then get the max and mins of these sets.
        var x_ar = [],
            y_ar = [],
            margin = {
                x: Math.min(this.width  * 0.15, 100),
                y: Math.min(this.height * 0.15, 100)
            };

        for (let shape of this.shapes) {
            x_ar.push(shape.x);
            y_ar.push(shape.y);
        }
        
        var xmin = Math.min(... x_ar),
            ymin = Math.min(... y_ar),
            pipeline_width = Math.max(... x_ar) - xmin,
            pipeline_height = Math.max(... y_ar) - ymin,
            scale: Point = {
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
        for (let shape of this.shapes) {
            shape.x = (shape.x - xmin) * scale.x + margin.x;
            shape.y = (shape.y - ymin) * scale.y + margin.y;
        }
        
        this.valid = false;
    }
    
    centreCanvas(): void {
        var x_ar = [],
            y_ar = [];
        
        for (let shape of this.shapes) {
            x_ar.push(shape.x);
            y_ar.push(shape.y);
        }
        
        var xmove = ( this.width  - Math.max(... x_ar) - Math.min(... x_ar) ) / 2;
        var ymove = ( this.height - Math.max(... y_ar) - Math.min(... y_ar) ) / 2;

        for (let shape of this.shapes) {
            shape.x += xmove;
            shape.y += ymove;
        }
        
        this.valid = false;
    };




    /*
     * Helper functions for CanvasState.autoLayout.
     */
    private static insertIntoLayer(node: Nodes.Node, exec_order: Nodes.MethodNode[], list: Nodes.Node[]): Nodes.Node[] {
        // Insert a node into a list in a "smart" way.
        // * Checks for duplicate entries
        // * If `node` is a method which is not the next method in exec_order, insertion is deferred
        // * If `node` -is- the next method in exec_order, insert all the method nodes that were deferred.
        var queue = CanvasState.method_node_queue; // queue is a static variable that persists across function calls
        if (list.indexOf(node) === -1) {
            if (node instanceof Nodes.MethodNode &&
                typeof exec_order !== 'undefined' &&
                exec_order.indexOf(node) > -1) {
                let method_list = list.filter(node => node instanceof Nodes.MethodNode);
                if (exec_order.length <= method_list.length) {
                    console.error("Unexpected number of methods in method_nodes.");
                }
                if (exec_order[method_list.length] === node) {
                    // We've found the method node we're looking for.
                    list.push(node);
                    method_list.push(node);

                    // Clear the queue. Make sure we maintain exec_order.
                    while (queue.length) {
                        let i = queue.indexOf(exec_order[method_list.length]);
                        list.push(queue[i]);
                        method_list.push(queue[i]);
                        queue.splice(i, 1);
                    }
                } else {
                    // Not the method node next in exec_order. Reserve it until we find the right node.
                    queue.push(node);
                }
            }
            else if (node instanceof Nodes.OutputNode) {
                // Output nodes are not relevant to execution order.
                list.push(node);
            }
        }
        return list;
    }

    /**
     * Calculate the total length of all the phases in an array.
     *
     * @param ar: an array of objects that all have a length property.
     */
    private static matrixTotalLength(ar) {
        return ar.reduce((a, b) => a + b.length, 0);
    }
    private static matrixIndexOf<T>(matrix: T[][], value: T): number[] {
        for (let i=0; i < matrix.length; i++) {
            for (let j=0; j < matrix[i].length; j++) {
                if (matrix[i][j] === value) {
                    return [ i, j ];
                }
            }
        }
        return null;
    }
    private static addConnectedNodes(node: Nodes.Node, list: Nodes.Node[]): void {
        // Follow a node's output cables to their connected nodes.
        // Insert these nodes in order into `list`.
        // Do not insert any duplicates into `list`.
        for (let out_magnet of node.out_magnets) {
            for (let connected of out_magnet.connected) {
                if (list.indexOf(connected.dest.parent) === -1) {
                    list.push(connected.dest.parent);
                }
            }
        }
    }
    private static addConnectedNodesReverse(node: Nodes.Node, list: Nodes.Node[], node_order: Nodes.Node[][]): void {
        // Reflexively insert input nodes into `list`.
        // Do not insert any duplicates into `list`.
        for (let in_magnet of node.out_magnets) {
            for (let connected of in_magnet.connected) {
                let connected_input = connected.source.parent;
                if ((connected_input instanceof Nodes.RawNode ||
                    connected_input instanceof Nodes.CdtNode) &&
                    CanvasState.matrixIndexOf(node_order, connected_input) == null
                ) {
                    list.push(connected_input);
                }
            }
        }
    }
    autoLayout(): void {
        if (!this.exec_order) {
            return;
        }
        
        var x_spacing = 60,
            y_spacing = 130,
            // z_drop = 20,// not implemented yet. intent is for nodes to cascade down into each other like a series of waterfalls!
            layer = [],
            max_layer = this.exec_order[0].length,
            magnets_in_order = [],
            original_input_order = this.inputs.map(x => x);
        
        for (let i = 1; i < this.exec_order.length; i++) {
            if (this.exec_order[i].length > max_layer) {
                max_layer = this.exec_order[i].length;
            }
        }
        
        // First two layers are relatively easy.
        // Layer 1 is exec_order[0]. (Potentially with more input nodes which will be found later)
        // Layer 0 is any input node leading to exec_order[0].
        for (let layer1node of this.exec_order[0]) {
            magnets_in_order = magnets_in_order.concat(layer1node.in_magnets);
        }
        for (let magnet of magnets_in_order) {
            for (let connected of magnet.connected) {
                let node = connected.source.parent;
                if (layer.indexOf(node) === -1) {
                    layer.push(node);
                }
            }
        }

        interface NodeOrderArray<T> extends Array<T> {
            center_x?: number
        }
        let node_order:NodeOrderArray<any> = [ layer ];
        node_order.push(this.exec_order[0]);
        
        for (let i in this.exec_order) {
            let connected_nodes_in_order = [];
            for (let this_node of this.exec_order[i]) {
                CanvasState.addConnectedNodes(this_node, connected_nodes_in_order);// connected_nodes_in_order will be added to here
                CanvasState.addConnectedNodesReverse(this_node, node_order[node_order.length - 2], node_order);// node_order[node_order.length - 2] will be added to here
            }
            let layer = [];
            for (let connected_node of connected_nodes_in_order) {
                CanvasState.insertIntoLayer(connected_node, this.exec_order[i+1], layer);// `layer` will be added to here
            }
            node_order.push(layer);
        }
        
        //x: x * 0.577350269 - y - i * spacing == 0
        //y: x * 0.577350269 + y - j * spacing == 0
        
        $(this.canvas).fadeOut({ complete: () => {
            for (let j = 0; j < node_order.length; j++) {
                let layer_length = node_order[j].length;
                let layer_out_magnets = [];
                node_order[j].center_x = node_order[j].center_x || 0;
            
                for (let i of layer_length) {
                    let node = node_order[j][i];


                    // static isoTo2D(iso_x:number, iso_y:number): Point {
                    //     // inverse of [ isometricXCoord, isometricYCoord ]
                    //     return {
                    //         x: (iso_y + iso_x) / 1.154700538,
                    //         y: (iso_y - iso_x) / 2
                    //     };
                    // };

                    // node.x = (y_spacing * j + x_spacing * (i - layer_length/2) + node_order[j].center_x) / 1.154700538;
                    // node.y = (y_spacing * j - x_spacing * (i - layer_length/2) - node_order[j].center_x) / 2;// + y_drop * j;
                    let node_coords = Geometry.isoTo2D(
                        x_spacing * (i - layer_length/2) + node_order[j].center_x,
                        y_spacing * j
                    );
                    node.x = node_coords.x;
                    node.y = node_coords.y;
                    node.dx = node.dy = 0;
                
                    if (isNaN(node.x) || isNaN(node.y) ) {
                        console.error("Autolayout failed!");
                    }
                
                    if (node.out_magnets.length > 0) {
                        node.draw(this.ctx);// needed to update magnet coords
                        layer_out_magnets = layer_out_magnets.concat(node.out_magnets);
                    }
                }
            
                // the isometric X centre of the layer after this one will be aligned with the centre of the magnets leading to its nodes.
                if (j !== node_order.length - 1) {
                    let num_magnets = layer_out_magnets.length;
                    var avg = Geometry.averagePoint(layer_out_magnets);
                    node_order[j+1].center_x = Geometry.isometricXCoord(avg.x, avg.y);
                    if ( isNaN(node_order[j+1].center_x) ) {
                        console.error("Autolayout failed!", layer_out_magnets, num_magnets);
                    }
                }
            }

            // part ii: maintain input order
            // put all inputs on the y-position of the first node_order
            let first_layer_iso_y = Geometry.isometricYCoord(node_order[0][0].x, node_order[0][0].y);
            for (let i = 0; i < original_input_order.length; i++) {
                let new_dims = Geometry.isoTo2D(x_spacing * (i - original_input_order.length / 2), first_layer_iso_y);
                original_input_order[i].x = new_dims.x;
                original_input_order[i].y = new_dims.y;
            }

            this.scaleToCanvas(true);// "true" to maintain aspect ratio
            this.centreCanvas();
            for (let shape of this.shapes) {
                this.detectCollisions(shape);
            }
            this.valid = false;
            $(this.canvas).fadeIn();
        }});
    };
    
    /**
     * Align selected nodes along the named axis.
     * 
     * @param axis: a string from ["x", "y", "iso_x", "iso_y", "iso_z"]
     */
    alignSelection (axis:"x"|"y"|"iso_x"|"iso_y"|"iso_z"): void {
        /* @todo
         * if nodes are too close together then they will collide and then get pushed back out.
         * when this "push back out" happens, it should happen -only- on the axis of alignment.
         */
        var sel = this.selection;
        var getCoord = {
            x:     o => o.y,
            y:     o => o.x,
            iso_z: o => o.x,
            iso_y: o => Geometry.isometricXCoord(o.x, o.y),
            iso_x: o => Geometry.isometricYCoord(o.x, o.y)
        };
        var setCoord = {
            x:     (o,c) => { o.y = c; },
            y:     (o,c) => { o.x = c; },
            iso_z: (o,c) => { o.x = c; },
            iso_y: (o,c) => {
                let diff = Geometry.isoTo2D(Geometry.isometricXCoord(o.x, o.y) - c, 0);
                o.x -= diff.x;
                o.y -= diff.y;
            },
            iso_x: (o,c) => {
                let diff = Geometry.isoTo2D(0, Geometry.isometricYCoord(o.x, o.y) - c);
                o.x -= diff.x;
                o.y -= diff.y;
            }
        };
        if (sel instanceof Array && sel.length > 0 && getCoord.hasOwnProperty(axis)) {
            let coords = [];
            for (let sel_ of sel) {
                coords.push(getCoord[axis](sel_));
            }
            let center = coords.reduce((a,b) => a+b) / coords.length;
            for (let sel_ of sel) {
                setCoord[axis](sel_, center);
                sel_.dx = sel_.dy = 0;
                this.detectCollisions(sel_);
            }
            this.valid = false;
        }
    }
    
    private static pushShapesApart(shape1: Nodes.Node|Nodes.OutputZone, shape2: Nodes.Node|Nodes.OutputZone, bias: number, bounds: Rectangle): void {
        let step = 5;
        // Drawing a line between the two objects' centres, move the centre 
        // of mySel to extend this line while keeping the same angle.
        let my_x = shape1.x + shape1.dx;
        let my_y = shape1.y + shape1.dy;
        let sh_x = shape2.x + shape2.dx;
        let sh_y = shape2.y + shape2.dy;
        let dx = my_x - sh_x;
        let dy = my_y - sh_y;
        let dh = (dx < 0 ? -1 : 1) * (Math.sqrt(dx*dx + dy*dy) + step);// add however many additional pixels you want to move
        let angle = dx ? Math.atan(dy / dx) : Math.PI/2;
        let Dx = Math.cos(angle) * dh - dx;
        let Dy = Math.sin(angle) * dh - dy;

        shape1.dx += Dx * bias;
        shape2.dx   -= Dx * (1 - bias);
        my_x = shape1.x + shape1.dx;
        sh_x = shape2.x + shape2.dx;

        // do not allow shape to exceed canvas boundaries
        if (my_x > bounds.width) {
            sh_x -= my_x - bounds.width;
            my_x = bounds.width;
        }
        if (my_x < 0) {
            sh_x -= my_x;
            my_x = 0;
        }
        if (sh_x > bounds.width) {
            my_x -= sh_x - bounds.width;
            sh_x = bounds.width;
        }
        if (sh_x < 0) {
            my_x -= sh_x;
            sh_x = 0;
        }

        shape1.dx = my_x - shape1.x;
        shape2.dx = sh_x - shape2.x;

        shape1.dy += Dy * bias;
        shape2.dy -= Dy * (1 - bias);
        my_y = shape1.y + shape1.dy;
        sh_y = shape2.y + shape2.dy;

        if (my_y > bounds.height) {
            sh_y -= my_y - bounds.height;
            my_y = bounds.height;
        }
        if (my_y < 0) {
            sh_y -= my_y;
            my_y = 0;
        }
        if (shape2.y > bounds.height) {
            my_y += sh_y - bounds.height;
            sh_y = bounds.height;
        }
        if (sh_y < 0) {
            my_y -= sh_y;
            sh_y = 0;
        }

        shape1.dy = my_y - shape1.y;
        shape2.dy = sh_y - shape2.y;
    }
    detectCollisions (myShape: Nodes.Node|Nodes.OutputZone, bias?: number) {
        var followups = [],
            vertices = myShape.getVertices(),
            shapes_plus = this.shapes.concat(this.outputZone),
            canvas_bounds: Rectangle = {
                x: 0,
                y: 0,
                width: this.canvas.width / this.scale,
                height: this.canvas.height / this.scale
            },
            implemented_bias;
            
        // Bias defines how much to move myShape vs how much to move the shape it collided with.
        // 1 would be 100% myShape movement, 0 would be 100% other shape movement, and everything
        // else in-between is possible.
        if (bias === undefined || bias === null) bias = 0.75;
        // output zone is not allowed to move
        if (myShape === this.outputZone) implemented_bias = 0;
        
        for (let i = 0, shape; (shape = shapes_plus[i]); i++) {
            // Objects are passed by reference in JS, so this comparison is really comparing references.
            // Identical objects at different memory addresses will not pass this condition.
            if (shape === myShape) continue;

            if (implemented_bias === undefined) {
                // output zone is not allowed to move
                // returns 1 if shape is output zone. otherwise go with pre-set bias.
                implemented_bias = shape === this.outputZone || bias;
            }
            
            for (let j = 0, vertex; (vertex = vertices[j]); j++) {
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
                    
                    CanvasState.pushShapesApart(myShape, shape, implemented_bias, canvas_bounds);
            
                    vertices = myShape.getVertices();
                    vertex = vertices[j];
                }
            }
        }
        
        for (let followup of followups) {
            this.detectCollisions(followup, bias);
        }
    }
    
    doUp (): void {
        this.valid = false;
        this.$dialog = this.$dialog || $('#dialog_form');
        $(this.canvas).css("cursor", "auto");
        
        // Collision detection!
        // Note: algorithm now includes collisions with OutputZone.
        if (this.dragging) {
            for (let sel of this.selection) {
                if (typeof sel.getVertices == 'function') {
                    this.detectCollisions(sel);
                }
            }
        }
        
        this.dragging = false;
        
        if (this.selection[0] instanceof Nodes.Connector) {
            let connector = this.selection[0];

            if (connector.dest instanceof Nodes.Magnet) {
                // connector has been linked to an in-magnet
                // update source magnet
                if (connector.source.connected.indexOf(connector) < 0) {
                    connector.source.connected.push(connector);
                }
    
                // update destination magnet
                if (connector.dest.connected.indexOf(connector) < 0) {
                    connector.dest.connected.push(connector);
                }

                this.has_unsaved_changes = 1;
            } else {
                // connector not yet linked to anything
            
                if (this.outputZone.contains(connector.x, connector.y)) {
                    // Connector drawn into output zone
                    if (!(connector.source.parent instanceof Nodes.MethodNode)) {
                        // disallow Connectors from data node directly to end-zone
                        let index = this.connectors.indexOf(connector);
                        this.connectors.splice(index, 1);
                        this.selection = [];
                    } else {
                        // valid Connector, assign non-null value
                        let new_output_label = this.uniqueNodeName(connector.source.label, Nodes.OutputNode);
                        let out_node = connector.spawnOutputNode(new_output_label);
                        this.detectCollisions(out_node);
                        this.addShape(out_node);
    
                        // spawn dialog for output label
                        this.$dialog
                            .data('node', out_node)
                            .show()
                            .css({
                                left: out_node.x + out_node.dx - this.$dialog.width()/2,
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
            
        // see if this has changed the execution order or input order
        this.testExecutionOrder();
    
        // turn off all in-magnets
        for (let shape of this.shapes) {
            shape.unlightMagnets();
        }
    }
    
    private uniqueNodeName (desired_name: string, object_class): string {
        var suffix = 0,
            name = desired_name;
        for (let i = 0, shape; (shape = this.shapes[i]); i++) {
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
    }
    
    contextMenu (e) {
        var mcm = $('#method_context_menu'),
            sel = this.selection,
            showMenu = () => {
                mcm.show().css({ top: e.pageY, left: e.pageX });
                $('li', mcm).show();
            };
    
        // Edit mode can popup the context menu to delete and edit nodes
        if (this.can_edit && sel[0] instanceof Nodes.Node) {
            showMenu();
            if (sel.length > 1 || 
                    sel[0] instanceof Nodes.RawNode ||
                    sel[0] instanceof Nodes.CdtNode) {
                $('.edit', mcm).hide();
            }
        } else if (sel.length == 1) {
            // Otherwise, we're read only, so only popup the context menu for outputs with datasets
            if ((sel[0] instanceof Nodes.OutputNode ||
                 sel[0] instanceof Nodes.CdtNode ||
                 sel[0] instanceof Nodes.RawNode) &&
                sel[0].dataset_id) {

               // Context menu for pipeline outputs
               showMenu();
               $('.step_node', mcm).hide();
            }
            else if (sel[0] instanceof Nodes.MethodNode &&
                     sel[0].log_id) {

               // Context menu for pipeline steps
               showMenu();
               $('.dataset_node', mcm).hide();
            }
        }
        this.doUp();
        e.preventDefault();
    }
    
    addShape(shape: Nodes.Node): Nodes.Node {
        this.shapes.push(shape);
        if (shape instanceof Nodes.MethodNode) {
            this.methods.push(shape);
            this.testExecutionOrder();
        } else if (shape instanceof Nodes.CdtNode || shape instanceof Nodes.RawNode) {
            this.inputs.push(shape);
            this.inputs.sort(Geometry.isometricSort);
        } else if (shape instanceof Nodes.OutputNode) {
            this.outputs.push(shape);
        }
        shape.has_unsaved_changes = 1;
        this.valid = false;
        this.has_unsaved_changes = 1;
        return shape;
    };
    
    private static migrateConnectors(from_node: Nodes.MethodNode, to_node: Nodes.MethodNode): void {
        var migrateInputs  = migrateFnUsing('inputs',  'in_magnets',  'dest',   'push'),
            migrateOutputs = migrateFnUsing('outputs', 'out_magnets', 'source', 'unshift');

        from_node.inputs.forEach(migrateInputs);
        from_node.outputs.forEach(migrateOutputs);
        
        // inner helper function does the bulk of the work 
        // 4 arguments tell it which properties to use
        // modifies from_node and to_node in parent's scope
        function migrateFnUsing(xputs_prop, magnets_prop, terminal_prop, shift_dir) {
            return old_xput => {
                var old_didx_s1 = old_xput.dataset_idx - 1,
                    old_xput_connections = from_node[magnets_prop][old_didx_s1].connected,
                    new_xput = to_node[xputs_prop][old_didx_s1],
                    xputs_are_matching_cdts = new_xput &&
                        new_xput.structure !== null && old_xput.structure !== null &&
                        new_xput.structure.compounddatatype == old_xput.structure.compounddatatype,
                    xputs_are_raw = new_xput &&
                        new_xput.structure === null && old_xput.structure === null;

                if (xputs_are_raw || xputs_are_matching_cdts) {
                    // re-attach all Connectors
                    while (old_xput_connections.length > 0) {
                        let connector = old_xput_connections.pop();
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
    
    replaceMethod (old_method: Nodes.MethodNode, new_method: Nodes.MethodNode): boolean {
        var was_fully_connected = old_method.isFullyConnected();
        new_method.x = old_method.x;
        new_method.y = old_method.y;
        this.addShape(new_method);
        CanvasState.migrateConnectors(old_method, new_method);
        this.deleteObject(old_method);
        // this will detect a mismatch if any new xputs have been added
        return was_fully_connected !== new_method.isFullyConnected();
    }
    
    findNodeByLabel (label: string): Nodes.Node {
        var found;
        for (let shape of this.shapes) {
            if (shape.label == label) {
                if (found !== undefined) {
                    throw new Error("Duplicate label: '" + label + "'.");
                }
                found = shape;
            }
        }
        return found;
    }

    private static phaseOrderMethods(methods: Nodes.MethodNode[]): Nodes.MethodNode[][] {
        /*
         gather method nodes which have no method-node parents
         set as phase 0
         gather method nodes which are children of phase 1 methods
         eliminate method nodes which have parents not in an existing phase (0..n)
         set as phase 1
         etc...
         */

        var phases = [];

        // esoteric syntax: label before a loop allows "continue" and "break" to specify which loop they are continuing or breaking.
        // check number of methods in the phases array
        fill_phases_array: for (
            var L = 0;
            methods.length > CanvasState.matrixTotalLength(phases) &&
            L < 200;
            L++
        ) {
            let phase = [];

            for ( let method of methods ) {
                if (CanvasState.matrixIndexOf(phases, method) != null) {
                    continue;
                }

                let okay_to_add = true;
                for ( let in_magnet of method.in_magnets ) {

                    // check if pipeline is incomplete
                    if (in_magnet.connected.length === 0) {
                        // can't go any further in this case
                        phases = null;
                        break fill_phases_array;
                    }

                    let parent = in_magnet.connected[0].source.parent;
                    if (methods.indexOf(parent) === -1) {
                        continue;
                    }

                    // if parent node has not been put in order yet,
                    // then this node cannot be added.
                    if (null == CanvasState.matrixIndexOf(phases, parent)) {
                        okay_to_add = false;
                        break;
                    }
                }

                if (okay_to_add === true) {
                    phase.push(method);
                }
            }

            // check if pipeline is incomplete
            if (phase.length === 0) {
                // can't go any further in this case
                phases = null;
                break;
            }

            phases.push(phase);
            L++;
        }

        // sanity check... don't let this algorithm run away
        if (L >= 200) {
            console.log('DEBUG: Runaway topological sort algorithm');
            phases = null;
        }

        return phases;
    }
    
    // Returns nothing, but sets CanvasState.exec_order and CanvasState.exec_order_is_ambiguous
    testExecutionOrder (): void {
        this.exec_order = CanvasState.phaseOrderMethods(this.methods);
        
        if (this.exec_order) {
            this.checkAmbiguousExecutionOrder();
            this.exec_order_may_have_changed |= this.exec_order_is_ambiguous;
        } else {
            // Pipeline is incomplete
            this.exec_order_is_ambiguous = null;
        }
    }

    /**
     * Check if the execution order is ambiguous.
     * 
     * Reads this.exec_order, and sets this.exec_order_is_ambiguous.
     */
    checkAmbiguousExecutionOrder() {
        // get the maximum number of methods per phase
        // (.map counts the methods in each phase, while Math.max.apply 
        // finds the maximum and takes its input as an array rather than an
        // argument list)
        // comparison operation 1< will be true if there is more than 1 step per phase.
        this.exec_order_is_ambiguous = +(1 < Math.max(
            ... this.exec_order.map(a => a.length)
        ));
    }
    
    disambiguateExecutionOrder() {
        this.methods = [];
        for (let exec_step of this.exec_order ) {
            // @note: nodes also have dx and dy properties which are !== 0 when collisions were detected.
            // I have not accounted for these properties in this method because they could shift around
            // on window resize, and it makes no sense for the pipeline to change on window resize.
            exec_step.sort(Geometry.isometricSort);
            this.methods = this.methods.concat(exec_step);
        }
    }
    
    getSteps() {
        if ( ! this.exec_order) {
            this.testExecutionOrder();
        }
        return this.exec_order.reduce((a,b) => a.concat(b), []);
    }
    
    clear(): void {
        // wipe canvas content clean before redrawing
        this.ctx.clearRect(0, 0, this.width / this.scale, this.height / this.scale);
        this.ctx.textAlign = 'center';
        this.ctx.font = '12pt Lato, sans-serif';
    }
    
    reset(): void {
        // remove all objects from canvas
        this.clear();
        // reset containers to reflect canvas
        this.shapes = [];
        this.methods = [];
        this.inputs = [];
        this.outputs = [];
        this.connectors = [];
        this.exec_order = [];
        this.selection = [];
    }
    
    draw(): void {
        /*
        Render pipeline objects to Canvas.
         */
        var ctx = this.ctx,
            sel = this.selection;
        ctx.save();
        this.clear();
        ctx.scale(this.scale, this.scale);
        
        var draggingFromMethodOut = (
                this.dragging &&
                sel.length == 1 &&
                sel[0] instanceof Nodes.Connector &&
                sel[0].source.parent instanceof Nodes.MethodNode);
        
        // draw output end-zone -when- dragging a connector from a MethodNode
        if (draggingFromMethodOut && this.can_edit) {
            this.outputZone.draw(this.ctx);
        }
        
        // draw all shapes and magnets
        for (let shape of this.shapes) {
            shape.draw(ctx);
        }

        // draw all connectors
        ctx.globalAlpha = 0.75;
        for (let connector of this.connectors) {
            connector.draw(ctx);
        }
        ctx.globalAlpha = 1.0;

        // draw selection ring
        if (sel.length > 0) {
            ctx.strokeStyle = this.selectionColor;
            ctx.lineWidth = this.selectionWidth * 2;
            ctx.font = '9pt Lato, sans-serif';
            ctx.textBaseline = 'middle';
            ctx.textAlign = 'center';
            for (let sel_ of sel) {
                sel_.highlight(ctx, this.dragging);
            }
        }
        
        // draw all update signals above shapes and connectors
        for (let method of this.methods) {
            if (method.update_signal) {
                method.update_signal.draw(ctx);
            }
        }

        if (this.enable_labels) {
            let labels = this.generateLabels();

            // draw all labels
            ctx.textAlign = 'center';
            ctx.textBaseline = 'alphabetic';
            ctx.font = '10pt Lato, sans-serif';

            // to minimize canvas state changes, loop twice.
            // canvas state changes are computationally expensive.
            ctx.fillStyle = '#fff';
            ctx.globalAlpha = 0.7;
            for (let label of labels) {
                let textWidth = ctx.measureText(label.label).width;
                ctx.fillRect(label.x - textWidth/2 - 2, label.y - 11, textWidth + 4, 14);
            }
            ctx.fillStyle = '#000';
            ctx.globalAlpha = 1.0;
            for (let label of labels) {
                ctx.fillText(label.label, label.x, label.y);
            }
        }

        if (this.mouse_highlight) {
            // Highlight (label) the object (usually a magnet);
            this.mouse_highlight.highlight(ctx);
        }
        
        this.ctx.fillStyle = "#fff";
        this.ctx.globalCompositeOperation = 'destination-over';
        this.ctx.fillRect(0, 0, this.width / this.scale, this.height / this.scale);
        this.ctx.globalCompositeOperation = 'source-over';
        
        ctx.restore();
        this.valid = true;
    }

    private labelFns = {
        inputs: function(l, _i) {
            return "i" + (_i + 1) + l.suffix + ": " + l.label;
        },
        _methods: function(l, _i) {
            return "s" + (_i + 1) + l.suffix + ': ' + l.label;
        },
        outputs: function(l) {
            return (l.suffix ? l.suffix + ' ' : '') + l.label;
        },
        methods: null
    };
    generateLabels () {
        // check if method node order is not needed
        if (!this.force_show_exec_order && !this.exec_order_is_ambiguous) {
            this.labelFns.methods = this.labelFns.outputs;// treatment is identical to outputs (unordered)
        } else {
            this.labelFns.methods = this.labelFns._methods;
        }
        this.labelFns.inputs = this.labelFns.outputs;// treatment is identical to outputs (unordered)

        // prepare all labels
        let labels = [];
        for (let nodeType of [ "methods", "inputs", "outputs" ]) {
            for (let i = 0, node; (node = this[nodeType][i]); i++) {
                let L = node.getLabel();
                L.label = this.labelFns[nodeType](L, i);
                labels.push(L);
            }
        }

        return labels;
    }
    
    getPos (e): Point {
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
    
    deleteObject (objectToDelete?) {
        // delete selected object
        // @param objectToDelete optionally specifies which object should be deleted.
        // Otherwise just go with the current selection.
        var sel;
        
        if (typeof objectToDelete !== 'undefined') {
            sel = [ objectToDelete ];
        } else {
            sel = this.selection;
        }
        
        for (let sel_ of sel) {
            if (typeof sel_ == 'object' && typeof sel_.deleteFrom == 'function') {
                sel_.deleteFrom(this);
            }
        }
        // see if this has changed the execution order
        this.testExecutionOrder();
        this.selection = [];
        this.valid = false; // re-draw canvas to make Connector disappear
        this.has_unsaved_changes = 1;
    }
    
    findMethodNode (method_pk: number): Nodes.MethodNode {
        for (let method of this.methods) {
            if (method.pk == method_pk) {
                return method;
            }
        }
        return null;
    };
    
    findOutputNode (pk: number): Nodes.OutputNode {
        for (let output of this.outputs) {
            if (output.pk == pk) {
                return output;
            }
        }
        return null;
    };

    findInputNode (input_index) {
        for (let input of this.inputs) {
            if (input.input_index == input_index) {
                return input;
            }
        }
        return null;
    };

}