/**
 * drydock.js
 *   Implements HTML5 Canvas interface for assembling
 *   pipelines from datatype and method nodes.
 */
"use strict";
import {
    CanvasObject, CNode, MethodNode, CdtNode, RawNode, OutputNode, OutputZone, Magnet, Connector,
    CanvasWrapper
} from "./drydock_objects";
import { Geometry, Point, Rectangle } from "./geometry";
import 'jquery';

export const REDRAW_INTERVAL = 50; // ms

export class CanvasState {
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
    shapes: CNode[] = []; // collection of shapes to be drawn
    methods: MethodNode[] = []; // collection of methods to be drawn (subset of shapes)
    inputs: (CdtNode|RawNode)[] = []; // collection of inputs to be drawn (subset of shapes)
    outputs: OutputNode[] = []; // collection of outputs to be drawn (subset of shapes)
    connectors: Connector[] = []; // collection of connectors between shapes
    dragging = false; // if mouse drag

    selection: (CNode|Connector)[] = []; // reference to active (selected) objects
    dragstart = { x: 0, y: 0 }; // where in the object we clicked
    dragoffx = 0;
    dragoffy = 0;

    exec_order: MethodNode[][] = [];
    exec_order_is_ambiguous: boolean;
    exec_order_may_have_changed: boolean;
    input_order_may_have_changed: boolean;
    force_show_exec_order: boolean;
    has_unsaved_changes = false;

    collisions = 0;

    // options
    selectionColor = '#7bf';
    selectionWidth = 2;
    
    width: number;
    height: number;
    old_width: number;
    old_height: number;
    pos_x: number;
    pos_y: number;
    ctx: CanvasRenderingContext2D;
    htmlTop:number;
    htmlLeft: number;
    outputZone: OutputZone;

    mouse_highlight: Magnet;
    $dialog: any;

    static method_node_queue: MethodNode[] = [];
    
    constructor(public canvas: HTMLCanvasElement, public can_edit: boolean, interval?: number) {
        /*
        keeps track of canvas state (mouse drag, etc.)
         */
        this.old_width  = this.width  = this.canvas.width;
        this.old_height = this.height = this.canvas.height;
        this.pos_y  = this.canvas.offsetTop;
        this.pos_x  = this.canvas.offsetLeft;
        this.ctx    = this.canvas.getContext('2d');

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
    
        this.outputZone = new OutputZone(this.width, this.height);
        
        // events
        var interval_fn = () => {
            if (this.exec_order_may_have_changed) {
                this.disambiguateExecutionOrder();
                this.exec_order_may_have_changed = false;
            }
            if (this.input_order_may_have_changed) {
                this.inputs.sort(Geometry.isometricSort);
                this.input_order_may_have_changed = false;
            }
            if (!this.valid) {
                this.draw();
            }
        };
        interval_fn();
        if (interval !== undefined) {
            setInterval(interval_fn, interval);
        }
    }

    setScale(factor: number): void {
        this.scale = factor;
        this.valid = false;
    }
    
    getMouseTarget (mx: number, my: number): CanvasObject {
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
    
    /**
     * If a node or connector was clicked, select it.
     * If nothing was clicked, de-select everything.
     *
     * Shift key enables the following behaviour:
     * - If a previously selected node was clicked, de-select it.
     * - If a non-selected node was clicked, select it.
     * - If nothing was clicked, leave selection as-is (do nothing).
     *
     * @param e The mouse event object.
     */
    doDown (e: MouseEvent) {
        var pos = this.getPos(e),
            mySel = this.getMouseTarget(pos.x, pos.y),
            selection = this.selection;
    
        if (mySel === null) {
            if (!e.shiftKey) {
                // nothing clicked
                this.selection = [];
                this.valid = false;
            }
        } else {
            // Check if object is already selected (or if shift key is held)
            if (e.shiftKey ||
                    CanvasState.isMagnet(mySel) ||
                    CanvasState.isSelectable(mySel) &&
                    selection.indexOf(mySel) === -1) {
                mySel.doDown(this, e);
            }
            this.dragstart = pos;
            this.dragging = true;
            this.valid = false; // activate canvas
        }
    }
    
    private moveConnector(connector: Connector, mouse: Point) {
        // reset to allow mouse to disengage Connector from a magnet
        connector.x = mouse.x;
        connector.y = mouse.y;
    
        // get this connector's shape
        let source_shape = connector.source.parent;
    
        let old_dest = connector.dest; // keep track for purposes of change detection
    
        if (connector.dest !== undefined && connector.dest !== null) {
            connector.dest.connected = [];
            connector.dest = null;
        }
    
        // check if connector has been dragged to an in-magnet
        for (let shape of this.shapes) {
            // disallow self-referential connections
            if (source_shape !== undefined && shape === source_shape) {
                continue;
            }
        
            for (let in_magnet of shape.in_magnets) {
                // light up magnet
                if (in_magnet.connected.length === 0 &&
                    (connector.source.cdt === in_magnet.cdt ||
                    // OutputNodes don't care about datatype.
                    CanvasState.isOutputNode(shape))
                ) {
                    in_magnet.acceptingConnector = true;
                }
            }
        
            let in_magnet = shape.getMouseTarget(mouse.x, mouse.y);
        
            // does this in-magnet accept this CompoundDatatype?
            // cdt is null if own_shape is a RawNode
            if (CanvasState.isMagnet(in_magnet) &&
                (
                    connector.source.cdt === in_magnet.cdt ||
                    // OutputNodes don't care about datatype.
                    CanvasState.isOutputNode(shape)
                )
            ) {
                in_magnet.tryAcceptConnector(connector)
            }
        }
    
        if (connector.dest !== old_dest) {
            this.dispatchChangeEvent({connected: [connector]});
        }
    }
    
    private moveShape(shape: CNode, mouse: Point) {
        // update coordinates of this shape/connector
        // any changes made by the collision detection algorithm on this shape are now made "official"
        let dx = mouse.x - this.dragstart.x;
        let dy = mouse.y - this.dragstart.y;
        shape.x += dx + shape.dx;
        shape.y += dy + shape.dy;
        shape.dy = shape.dx = 0;
    
        // if execution order is ambiguous, the tiebreaker is the y-position.
        // dragging a method node needs to calculate this in real-time.
        this.exec_order_may_have_changed = this.exec_order_may_have_changed ||
            this.exec_order_is_ambiguous && shape.affects_exec_order;
        this.input_order_may_have_changed = this.input_order_may_have_changed ||
            CanvasState.isInputNode(shape);
    }
    
    doMove (e: MouseEvent): void {
        /*
         event handler for mouse motion over canvas
         */
        var mouse = this.getPos(e);
        
        if (this.dragging) {
            this.valid = false; // redraw

            // are we carrying a shape or Connector?
            for (let sel of this.selection) {
                // are we carrying a connector?
                if (CanvasState.isConnector(sel)) {
                    if (this.can_edit) {
                        this.moveConnector(sel, mouse);
                    }
                }
                // carrying a shape
                else {
                    this.moveShape(sel, mouse);
                }
            }
            
            this.dragstart = mouse;
        }
        
        // let was_highlighted = this.mouse_highlight !== null;
        // this.mouse_highlight = null;
        // for (let shape of this.shapes) {
        //     let magnet = shape.getMouseTarget(mouse.x, mouse.y);
        //     if (CanvasState.isMagnet(magnet) && magnet.connected.length === 0) {
        //         if (magnet !== this.mouse_highlight) {
        //             this.mouse_highlight = magnet;
        //             this.valid = false;
        //         }
        //         break;
        //     }
        // }
        // if (was_highlighted && !this.mouse_highlight) {
        //     this.valid = false;
        // }
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
    private static insertIntoLayer(node: CNode, exec_order: MethodNode[], list:CNode[]): CNode[] {
        // Insert a node into a list in a "smart" way.
        // * Checks for duplicate entries
        // * If `node` is a method which is not the next method in exec_order, insertion is deferred
        // * If `node` -is- the next method in exec_order, insert all the method nodes that were deferred.
        var queue = CanvasState.method_node_queue; // queue is a static variable that persists across function calls
        if (list.indexOf(node) === -1) {
            if (CanvasState.isMethodNode(node) &&
                exec_order.indexOf(<MethodNode>node) > -1) {
                let method_list = list.filter(node => node.isMethodNode());
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
                    queue.push(<MethodNode>node);
                }
            }
            else if (node.isOutputNode()) {
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
    private static addConnectedNodesOut(node: CNode, list: CNode[]): void {
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
    private static addConnectedInputNodesIn(node: CNode, list: CNode[], node_order: CNode[][]): void {
        // Reflexively insert input nodes into `list`.
        // Do not insert any duplicates into `list`.
        for (let in_magnet of node.in_magnets) {
            let connected_input = in_magnet.connected[0].source.parent;
            if (connected_input.isInputNode() &&
                CanvasState.matrixIndexOf(node_order, connected_input) === null
            ) {
                list.push(connected_input);
            }
        }
    }
    protected static exposeHelperFnsForTesting() {
        return {
            matrixTotalLength:        CanvasState.matrixTotalLength,
            matrixIndexOf:            CanvasState.matrixIndexOf,
            addConnectedNodesOut:     CanvasState.addConnectedNodesOut,
            addConnectedInputNodesIn: CanvasState.addConnectedInputNodesIn,
            insertIntoLayer:          CanvasState.insertIntoLayer
        };
    }

    autoLayout(): void {
        if (!this.exec_order) {
            return;
        }

        interface NodeOrderArray<T> extends Array<T> {
            center_x?: number
        }
        
        let x_spacing = 60,
            y_spacing = 130,
            // z_drop = 20,// not implemented yet. intent is for nodes to cascade down into each other like a series of waterfalls!
            l1_inmagnets_in_order = this.exec_order[0].reduce(
                (prev, curr) => prev.concat(curr.in_magnets), []
            ),
            // First two layers are relatively easy.
            // Layer 1 is exec_order[0]. (Potentially with more input nodes which will be found later)
            node_order:NodeOrderArray<any> = [ [], this.exec_order[0] ],
            original_input_order = this.inputs.map(x => x);

        // Layer 0 is any input node leading to exec_order[0].
        for (let magnet of l1_inmagnets_in_order) {
            let node = magnet.connected[0].source.parent;
            if (node_order[0].indexOf(node) === -1) {
                node_order[0].push(node);
            }
        }

        for (let i = 0; i < this.exec_order.length; i++) {
            let connected_nodes_in_order = [],
                layer = [];
            for (let node of this.exec_order[i]) {
                // all nodes connected to out magnets will be added to connected_nodes_in_order
                CanvasState.addConnectedNodesOut(node, connected_nodes_in_order);
                // node_order[node_order.length - 2] will be added to node_order
                CanvasState.addConnectedInputNodesIn(node, node_order[node_order.length - 2], node_order);
            }
            for (let connected_node of connected_nodes_in_order) {
                layer = CanvasState.insertIntoLayer(connected_node, this.exec_order[i+1], layer);// `layer` will be added to here
            }
            node_order.push(layer);
        }

        $(this.canvas).fadeOut({ complete: () => {
            for (let j = 0; j < node_order.length; j++) {
                let layer_length = node_order[j].length;
                let layer = node_order[j];
                layer.center_x = layer.center_x || 0;
            
                for (let i = 0; i < layer_length; i++) {
                    layer[i].setCoordsFromIso(
                        x_spacing * (i - layer_length/2) + layer.center_x,
                        y_spacing * j
                    );
                }

                let layer_out_magnets = layer.reduce(
                    (prev, curr) => prev.concat(curr.out_magnets), []
                );

                // the isometric X centre of the layer after this one will be aligned with the centre of the magnets leading to its nodes.
                if (j !== node_order.length - 1) {
                    let avg = Geometry.averagePoint(layer_out_magnets);
                    node_order[j+1].center_x = Geometry.isometricXCoord(avg.x, avg.y);
                    if ( isNaN(node_order[j+1].center_x) ) {
                        console.error("Autolayout failed!");
                        // console.error({
                        //     "ordered nodes": node_order,
                        //     layer: j,
                        //     "outmagnets of layer": layer_out_magnets
                        // });
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
    
    
    private static getCoord = {
        x:     o => o.y,
        y:     o => o.x,
        iso_x: o => Geometry.isometricYCoord(o.x, o.y),
        iso_y: o => Geometry.isometricXCoord(o.x, o.y)
    };
    private static setCoord = {
        x:     (o, c) => { o.y = c; },
        y:     (o, c) => { o.x = c; },
        iso_x: (o, c) => {
            let diff = Geometry.isoTo2D(
                0,
                Geometry.isometricYCoord(o.x, o.y) - c
            );
            o.x -= diff.x;
            o.y -= diff.y;
        },
        iso_y: (o, c) => {
            let diff = Geometry.isoTo2D(
                Geometry.isometricXCoord(o.x, o.y) - c,
                0
            );
            o.x -= diff.x;
            o.y -= diff.y;
        }
    };
    /**
     * Align selected nodes along the named axis.
     * 
     * @param axis: a string from ["x", "y", "iso_x", "iso_y", "iso_z"]
     */
    alignSelection (axis: string): void {
        /* @todo
         * if nodes are too close together then they will collide and then get pushed back out.
         * when this "push back out" happens, it should happen -only- on the axis of alignment.
         */
        var sel = this.selection;
        if (Array.isArray(sel) && sel.length && CanvasState.getCoord.hasOwnProperty(axis)) {
            let center = sel.map(
                    node => CanvasState.getCoord[axis](node)
                ).reduce(
                    (a, b) => a + b
                ) / sel.length;
            
            for (let sel_ of sel) {
                if (CanvasState.isNode(sel_)) {
                    CanvasState.setCoord[axis](sel_, center);
                    sel_.dx = sel_.dy = 0;
                    this.detectCollisions(sel_);
                }
            }
            this.valid = false;
        }
    }
    
    private static pushShapesApart(shape1: CNode|OutputZone, shape2: CNode|OutputZone, bias: number, bounds: Rectangle): void {
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
    
    detectCollisions (myShape: CNode|OutputZone, bias?: number) {
        var followups = [],
            vertices = myShape.getVertices(),
            shapes_plus: (CNode|OutputZone)[] = [].concat(this.shapes, this.outputZone),
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
    
    detectAllCollisions(fromBasePosition: boolean = false, bias: number = 0.5): void {
        for (let shape of this.shapes) {
            if (fromBasePosition) {
                shape.dx = shape.dy = 0;
            }
            this.detectCollisions(shape, bias);
        }
    }
    
    doUp (e?: MouseEvent): void {
        this.valid = false;
        $(this.canvas).css("cursor", "auto");
        
        // Collision detection!
        // Note: algorithm now includes collisions with OutputZone.
        if (this.dragging) {
            for (let sel of this.selection) {
                if (CanvasState.isNode(sel)) {
                    this.detectCollisions(sel);
                }
            }
        }
        
        this.dragging = false;
        
        let sel = this.selection[0];
        if (CanvasState.isConnector(sel)) {
            let connector = sel;

            if (CanvasState.isMagnet(connector.dest)) {
                // connector has been linked to an in-magnet
                // update source magnet
                if (connector.source.connected.indexOf(connector) < 0) {
                    connector.source.connected.push(connector);
                }
    
                // update destination magnet
                if (connector.dest.connected.indexOf(connector) < 0) {
                    connector.dest.connected.push(connector);
                }

                this.has_unsaved_changes = true;
            } else {
                // connector not yet linked to anything
            
                if (this.outputZone.contains(connector.x, connector.y)) {
                    // Connector drawn into output zone
                    if (!(connector.source.parent.isMethodNode())) {
                        // disallow Connectors from data node directly to end-zone
                        let index = this.connectors.indexOf(connector);
                        this.connectors.splice(index, 1);
                        this.selection = [];
                    } else {
                        // valid Connector, assign non-null value
                        let new_output_label = this.uniqueNodeName(connector.source.label, OutputNode);
                        let out_node = connector.spawnOutputNode(new_output_label);
                        this.detectCollisions(out_node);
                        this.addShape(out_node, true);
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
            // @todo: BUG: CdtNodes and RawNodes may share a name under this scheme
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
    
    completeMethodInputs(method): void {
        let emptyMagnets = method.in_magnets.filter(el => el.connected.length === 0);
        for (let magnet of emptyMagnets) {
            let ctor;
            let args: (string|number)[] = [ method.x - 80, method.y - 80 ];
            if (magnet.cdt !== null && magnet.cdt !== undefined) {
                ctor = CdtNode;
                args.unshift(magnet.cdt);
            } else {
                ctor = RawNode;
            }
            args.push(this.uniqueNodeName(magnet.label, ctor));
            let shape = new ctor(...args);
            this.addShape(shape);
            var connector = new Connector(shape.out_magnets[0]);
            connector.dest = magnet;
            connector.source = shape.out_magnets[0];
            shape.out_magnets[0].connected = [ connector ];
            magnet.connected = [ connector ];
            this.connectors.push(connector);
            this.valid = false;
        }
    }
    
    addShape(shape: CNode, open_dialog: boolean = false): CNode {
        this.shapes.push(shape);
        if (CanvasState.isMethodNode(shape)) {
            this.methods.push(shape);
            this.testExecutionOrder();
        } else if (CanvasState.isInputNode(shape)) {
            this.inputs.push(shape);
            this.inputs.sort(Geometry.isometricSort);
        } else if (CanvasState.isOutputNode(shape)) {
            this.outputs.push(shape);
        }
        shape.has_unsaved_changes = true;
        this.valid = false;
        this.has_unsaved_changes = true;
        if (open_dialog) {
            this.dispatchNewOutputEvent({ added: [ shape ], open_dialog });
        }
        this.dispatchChangeEvent({ added: [ shape ] });
        return shape;
    };
    
    private static migrateConnectors(from_node: MethodNode, to_node: MethodNode): void {
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
    
    replaceMethod (old_method: MethodNode, new_method: MethodNode): boolean {
        var was_fully_connected = old_method.isFullyConnected();
        new_method.x = old_method.x;
        new_method.y = old_method.y;
        this.addShape(new_method);
        CanvasState.migrateConnectors(old_method, new_method);
        this.deleteObject(old_method);
        // this will detect a mismatch if any new xputs have been added
        return was_fully_connected !== new_method.isFullyConnected();
    }
    
    findNodeByLabel (label: string): CNode {
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

    private static phaseOrderMethods(methods: MethodNode[]): MethodNode[][] {
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
            this.exec_order_may_have_changed = this.exec_order_may_have_changed || this.exec_order_is_ambiguous;
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
        this.exec_order_is_ambiguous = (1 < Math.max(
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
        if ( ! this.exec_order) {
            return this.getMethodNodes(); // unordered list
        } else {
            return this.exec_order.reduce((a,b) => a.concat(b), []); // ordered list
        }
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
        
        this.dispatchChangeEvent({ reset: true });
    }
    
    private dispatchNewOutputEvent(data: any) {
        this.canvas.dispatchEvent(
            new CustomEvent('CanvasStateNewOutput', { detail: data })
        );
    }
    
    private dispatchChangeEvent(data: any) {
        this.canvas.dispatchEvent(
            new CustomEvent('CanvasStateChange', { detail: data })
        );
    }
    
    /**
     * Render pipeline objects to Canvas.
     */
    draw(): void {
        var ctx = this.ctx,
            sel = this.selection;
        ctx.save();
        this.clear();
        ctx.scale(this.scale, this.scale);
        
        let sel0 = sel[0];
        var draggingFromMethodOut = (
                this.dragging &&
                sel.length == 1 &&
                CanvasState.isConnector(sel0) &&
                sel0.source.parent.isMethodNode() );
        
        // draw output end-zone -when- dragging a connector from a MethodNode
        if (draggingFromMethodOut && this.can_edit) {
            this.outputZone.draw(this.ctx);
        }
    
        // draw selection ring
        if (sel.length > 0) {
            ctx.strokeStyle = this.selectionColor;
            ctx.lineWidth = this.selectionWidth * 2;
            ctx.font = '9pt Lato, sans-serif';
            ctx.textBaseline = 'middle';
            ctx.textAlign = 'center';
            for (let sel_ of sel.filter(CanvasState.isNode)) {
                sel_.highlight(ctx);
            }
        }
        
        // draw all shapes and magnets
        // @todo: render these in hidden canvases and import them instead of redrawing from scratch
        for (let shape of this.shapes) {
            shape.draw(ctx);
        }
        
        // draw all connectors
        ctx.globalAlpha = 0.75;
        for (let connector of this.connectors) {
            connector.draw(ctx);
        }
        ctx.globalAlpha = 1.0;
    
        // draw labels on connectors of selected nodes
    
        // Attempting to make this more efficient by batching all labels into one.
        // Drawing text is the most resource-intensive operation here.
        // This code uses makeLabel() instead of drawLabel(), which this replaces.
        // @todo: draw these using a separate SVG layer? CSS3 transforms are hardware-accelerated, and canvas is slow for text
        let connectorLabels = [];
        let canvasWrapper = new CanvasWrapper(null, ctx);
        for (let connector of this.connectors) {
            if (    connector.source && sel.indexOf(connector.source.parent) > -1 ||
                    connector.dest   && sel.indexOf(  connector.dest.parent) > -1
                ) {
                let label;
                if (label = connector.buildLabel(ctx)) {
                    connectorLabels.push(label);
                }
            }
        }
        ctx.save();
        ctx.font = '10pt Lato, sans-serif';
        ctx.textBaseline = 'middle';
        ctx.globalAlpha = 1;
        ctx.textAlign = 'center';
        for (let i = 0; i < connectorLabels.length; i++) {
            let label = connectorLabels[i];
            let prevLabel = connectorLabels[i - 1] || { centre: { x: 0, y: 0 }, rotate: 0 };
            ctx.rotate(-prevLabel.rotate);
            ctx.translate(label.centre.x - prevLabel.centre.x, label.centre.y - prevLabel.centre.y);
            ctx.rotate(label.rotate);
            ctx.fillStyle = '#aaa';
            canvasWrapper.fillRect(label.rect);
            ctx.fillStyle = "white";
            ctx.fillText(label.label, 0, 0);
        }
        ctx.restore();
    
        // draw label if connector itself is selected (and connected)
        ctx.strokeStyle = this.selectionColor;
        ctx.lineWidth = this.selectionWidth * 2;
        ctx.fillStyle = this.selectionColor;
        for (let connector of sel.filter(CanvasState.isConnector)) {
            (<Connector> connector).highlight(ctx);
            if ((<Connector> connector).dest) {
                (<Connector> connector).drawLabel(ctx);
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
        
        // if (this.mouse_highlight) {
        //     // Highlight (label) the object (usually a magnet);
        //     this.mouse_highlight.highlight(ctx);
        // }
        
        ctx.restore();
        this.valid = true;
    }

    private labelFns = {
        inputs: function(l, _i) {
            return "i" + (_i + 1) + (l.suffix || '') + ": " + l.label;
        },
        methods: null,
        methods_with_order: function(l, _i) {
            return "s" + (_i + 1) + (l.suffix || '') + ': ' + l.label;
        },
        methods_without_order: function(l, _i) {
            return (l.suffix ? l.suffix + ' ' : '') + l.label;
        },
        outputs: function(l) {
            return (l.suffix ? l.suffix + ' ' : '') + l.label;
        }
    };
    generateLabels () {
        // check if method node order is not needed
        let showXO = this.force_show_exec_order;
        let showXOUndef = showXO === undefined;
        
        if (showXO === true ||
                showXOUndef && this.exec_order_is_ambiguous) {
            this.labelFns.methods = this.labelFns.methods_with_order;
        } else if (showXO === false ||
                showXOUndef && !this.exec_order_is_ambiguous) {
            this.labelFns.methods = this.labelFns.methods_without_order;
        }

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
        var element = <HTMLElement> this.canvas,
            offsetX = 0,
            offsetY = 0;
    
        if (typeof element.offsetParent !== 'undefined') {
            do {
                offsetX += element.offsetLeft;
                offsetY += element.offsetTop;
            } while (element = <HTMLElement> element.offsetParent);
        }
    
        offsetX += this.stylePaddingLeft + this.styleBorderLeft + this.htmlLeft;
        offsetY += this.stylePaddingTop + this.styleBorderTop + this.htmlTop;
    
        let mx = (e.pageX - offsetX) / this.scale;
        let my = (e.pageY - offsetY) / this.scale;
    
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
        this.has_unsaved_changes = true;
        
        this.dispatchChangeEvent({ removed: sel });
    }
    
    findMethodNode (method_pk: number): MethodNode {
        for (let method of this.methods) {
            if (method.pk == method_pk) {
                return method;
            }
        }
        return null;
    }
    
    findOutputNode (pk: number): OutputNode {
        for (let output of this.outputs) {
            if (output.pk == pk) {
                return output;
            }
        }
        return null;
    }

    findInputNode (input_index) {
        for (let input of this.inputs) {
            if (input.input_index == input_index) {
                return input;
            }
        }
        return null;
    }

    getOutputNodes(): OutputNode[] {
        return this.outputs;
    }

    getInputNodes(): (CdtNode|RawNode)[] {
        return this.inputs;
    }

    getMethodNodes(): MethodNode[] {
        return this.methods;
    }

    /**
     * Absolute coordinates of the node's top edge, center.
     */
    getAbsoluteCoordsOfNode(node: any): {x: number, y: number} {
        let canvas = this.canvas;
        if (CanvasState.isMethodNode(node)) {
            let inputs_width = node.n_inputs * 4 + 7;
            let outputs_width = node.n_outputs * 4 + 24;
            return {
                x: node.x + node.dx + 0.8660254 * Math.min(Math.max(outputs_width - inputs_width, 0), 45) + canvas.offsetLeft - 9,
                y: node.y + node.dy - inputs_width + canvas.offsetTop - 29
            };
        } else if (CanvasState.isInputNode(node) || CanvasState.isOutputNode(node)) {
            return {
                x: node.x + node.dx + canvas.offsetLeft,
                y: node.y + node.dy + canvas.offsetTop - node.h/2 - node.offset
            };
        }
    }
    
    static isUniqueName(list: CNode[], name: string) {
      for (let shape of list) {
        if (shape.label === name) {
          return false;
        }
      }
      return true;
    }

    static isNode(node: any): node is CNode {
        return node && node.isNode && node.isNode();
    }
    static isCdtNode(node: any): node is CdtNode  {
        return node && node.isCdtNode && node.isCdtNode();
    }
    static isRawNode(node: any): node is RawNode {
        return node && node.isRawNode && node.isRawNode();
    }
    static isMethodNode(node: any): node is MethodNode {
        return node && node.isMethodNode && node.isMethodNode();
    }
    static isOutputNode(node: any): node is OutputNode {
        return node && node.isOutputNode && node.isOutputNode();
    }
    static isInputNode(node: any): node is CdtNode|RawNode {
        return node && (CanvasState.isCdtNode(node) || CanvasState.isRawNode(node));
    }
    static isDataNode(node: any): node is CdtNode|RawNode|OutputNode {
        return node && (CanvasState.isInputNode(node) || CanvasState.isOutputNode(node));
    }
    static isSelectable(obj: any): obj is CNode|Connector {
        return obj && (CanvasState.isNode(obj) || CanvasState.isConnector(obj));
    }
    static isConnector(obj: any): obj is Connector {
        return obj && obj.isConnector && obj.isConnector();
    }
    static isMagnet(obj: any): obj is Magnet {
        return obj && obj.isMagnet && obj.isMagnet();
    }
    static isOutputZone(obj: any): obj is OutputZone {
        return obj && obj.isOutputZone && obj.isOutputZone();
    }
    
    /**
     * Converts checkIntegrity into a simple yes-or-no with errors ignored.
     * Useful to passively and continuously validate pipeline.
     * @returns {boolean}
     */
    isComplete(): boolean {
        try {
            this.assertIntegrity();
        } catch(e) {
            // Enable to watch pipeline integrity in real-time:
            // console.log(e);
            return false;
        }
        return true;
    }
    
    assertIntegrity() {
        let expected_num_shapes = this.inputs.length + this.outputs.length + this.methods.length;
        
        if (this.shapes.length === 0) {
            throw "Pipeline is empty";
        }
        if (this.inputs.length === 0) {
            throw "Pipeline has no inputs";
        }
        if (this.methods.length === 0) {
            throw "Pipeline has no methods";
        }
        if (this.outputs.length === 0) {
            throw "Pipeline has no outputs";
        }
        
        if (expected_num_shapes > this.shapes.length) {
            throw 'Pipeline has more nodes than it knows about!'
        } else if (expected_num_shapes < this.shapes.length) {
            throw 'Pipeline has fewer nodes than it knows about!'
        }
        
        for (let shape of this.inputs) {
            // <any> type so that TS compiler doesn't detect a tautology here.
            if (!CanvasState.isInputNode(<any> shape)) {
                throw 'Invalid input detected: ' + shape.label;
            }
            
            // all CDtNodes or RawNodes (inputs) should feed into a MethodNode and have only one magnet
            if (shape.out_magnets.length !== 1) {
                throw 'Invalid amount of magnets for input node ' + shape.label;
            }
    
            // is this magnet connected?
            if (shape.out_magnets[0].connected.length === 0) {
                throw 'Disconnected input node ' + shape.label;
            }
        }
        
        for (let shape of this.outputs) {
            // <any> type so that TS compiler doesn't detect a tautology here.
            if (!CanvasState.isOutputNode(<any> shape)) {
                throw 'Invalid output detected: ' + shape.label;
            }
            
            // all outputs should come from a MethodNode and have only one magnet
            if (shape.in_magnets.length !== 1) {
                throw 'Invalid amount of magnets for output node ' + shape.label;
            }
        
            // is this magnet connected?
            if (shape.in_magnets[0].connected.length === 0) {
                throw 'Disconnected output node ' + shape.label;
            }
        }
        
        for (let shape of this.methods) {
            // <any> type so that TS compiler doesn't detect a tautology here.
            if (!CanvasState.isMethodNode(<any> shape)) {
                throw 'Invalid output detected: ' + shape.label;
            }
            let empty_out_magnets_list = shape.out_magnets
                .filter(el => el.connected.length === 0)
                .map(el => el.label)
                .join(', ');
            if (empty_out_magnets_list.length) {
                throw 'Step ' + shape.label + ' has unused outputs ' + empty_out_magnets_list;
            }
            let empty_in_magnets_list = shape.in_magnets
                .filter(el => el.connected.length === 0);
            if (empty_in_magnets_list.length) {
                throw 'Step ' + shape.label + ' has unused inputs';
            }
        }
    }

}