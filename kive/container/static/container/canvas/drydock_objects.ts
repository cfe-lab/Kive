/**
 * drydock_objects.js
 *   JS prototypes that are used to populate canvasState
 *   (see drydock.js)
 */
"use strict";
import { Point, Ellipse, Rectangle, Geometry } from "./geometry";
import { Circle, TextParams } from "./ShapeTypes";
import { Bezier } from "./bezier";
import { CanvasState } from "./drydock";

export class CanvasWrapper {

    /**
     * A helper class to easily draw primitive shapes on the canvas.
     */
    constructor(canvas: HTMLCanvasElement, private ctx?: CanvasRenderingContext2D) {
        this.ctx = ctx || canvas.getContext('2d');
    }

    /**
     * Draw a circle.
     * 
     * @param circle.x: the x position of the circle centre
     * @param circle.y: the y position of the circle centre
     * @param circle.r: the radius of the circle
     */
    drawCircle(circle: Circle) {
        this.ctx.beginPath();
        this.ctx.arc(circle.x, circle.y, circle.r, 0, 2 * Math.PI);
        this.ctx.closePath();
        this.ctx.fill();
    }

    /**
     * Draw an ellipse.
     * 
     * @param ellipse.x: the x position of the ellipse centre
     * @param ellipse.y: the y position of the ellipse centre
     * @param ellipse.rx: the radius of the ellipse along the x axis
     * @param ellipse.ry: the radius of the ellipse along the y axis
     */
    drawEllipse(ellipse: Ellipse) {
        this.ctx.save(); // save state
        this.ctx.translate(ellipse.x - ellipse.rx, ellipse.y - ellipse.ry);
        this.ctx.scale(ellipse.rx, ellipse.ry);
        this.drawCircle({x: 1, y: 1, r: 1});
        this.ctx.restore(); // restore to original state
    }

    /**
     * Stroke an ellipse.
     * 
     * @param ellipse.x: the x position of the ellipse centre
     * @param ellipse.y: the y position of the ellipse centre
     * @param ellipse.rx: the radius of the ellipse along the x axis
     * @param ellipse.ry: the radius of the ellipse along the y axis
     */
    strokeEllipse(ellipse: Ellipse) {
        this.ctx.save(); // save state
        this.ctx.translate(ellipse.x - ellipse.rx, ellipse.y - ellipse.ry);
        this.ctx.scale(ellipse.rx, ellipse.ry);
        this.ctx.beginPath();
        this.ctx.arc(1, 1, 1, 0, 2 * Math.PI);
        this.ctx.closePath();
        this.ctx.restore(); // restore to original state
        this.ctx.stroke();
    }

    /**
     * Draw text with a standard font, colour, and background rectangle.
     * 
     * The background rectangle is drawn around the text, so the anchor point
     * is inside the rectangle.
     * 
     * @param args.x: the x position of the anchor point
     * @param args.y: the y position of the middle of the line of text
     * @param args.text: the text to draw
     * @param args.dir: 1 for anchor point on the left, -1 for the right(default),
     *  or 0 for the centre
     */
    drawText(args: TextParams) {
        var dir = args.dir === 1 ? 1 : args.dir === 0 ? 0 : -1,
            rectArgs: Rectangle = { x: args.x, y: args.y, width: 0, height: 0 },
            textFill = "black",
            margin = 2;
        this.ctx.save();
        this.ctx.globalAlpha = 0.7;
        switch (args.style) {
        case 'node':
            this.ctx.font = '10pt Lato, sans-serif';
            this.ctx.textBaseline = 'alphabetic';
            rectArgs.height = 14;
            rectArgs.y -= 11;
            break;
        case 'connector':
            this.ctx.font = '10pt Lato, sans-serif';
            this.ctx.textBaseline = 'middle';
            rectArgs.height = 14;
            rectArgs.y -= 7;
            rectArgs.r = 6;
            margin = 5;
            textFill = "white";
            this.ctx.globalAlpha = 1;
            break;
        case 'outputZone':
            this.ctx.font = 'bold 10pt Lato, sans-serif';
            this.ctx.textBaseline = 'alphabetic';
            textFill = '#aaa';
            // rectArgs = undefined;
            break;
        case 'in-magnet':
            this.ctx.font = '9pt Lato, sans-serif';
            this.ctx.textBaseline = 'middle';
            this.ctx.strokeStyle = '#fff';
            this.ctx.fillStyle = '#666';
            this.ctx.lineWidth = 1;
            this.ctx.globalAlpha = 1;
            rectArgs.height = 17;
            rectArgs.y -= 8.5;
            rectArgs.r = 3;
            margin = 3;
            dir = -1;
            textFill = "white";
            rectArgs.stroke = true;
            break;
        case 'out-magnet':
            this.ctx.font = '9pt Lato, sans-serif';
            this.ctx.textBaseline = 'middle';
            this.ctx.strokeStyle = '#fff';
            this.ctx.fillStyle = '#666';
            this.ctx.lineWidth = 1;
            this.ctx.globalAlpha = 1;
            rectArgs.height = 17;
            rectArgs.y -= 8.5;
            rectArgs.r = 3;
            margin = 3;
            dir = 1;
            textFill = "white";
            rectArgs.stroke = true;
            break;
        case 'callout':
            this.ctx.font = '11pt Lato, sans-serif';
            this.ctx.textBaseline = 'middle';
            this.ctx.strokeStyle = '#fff';
            this.ctx.fillStyle = '#666';
            this.ctx.lineWidth = 1;
            this.ctx.globalAlpha = 1;
            rectArgs.height = 24;
            rectArgs.y -= 12;
            rectArgs.r = 8;
            margin = 7;
            dir = 1;
            textFill = "white";
            rectArgs.stroke = true;
            break;
        default:
            this.ctx.font = '9pt Lato, sans-serif';
            this.ctx.textBaseline = 'middle';
            rectArgs.height = 15;
            rectArgs.y -= 7.5;
        }
        this.ctx.textAlign = dir === 1 ? 'left' : dir === 0 ? 'center' : 'right';

        // make a backing box so the label is on the fill colour
        rectArgs.width = 2 * margin + this.ctx.measureText(args.text).width;
        if (dir === 0) {
            rectArgs.x -= rectArgs.width / 2;
        }
        else {
            rectArgs.x -= dir * margin;
            rectArgs.width *= dir;
        }
        this.fillRect(rectArgs);
        if (rectArgs.stroke) {
            this.strokeRect(rectArgs);
        }
        if (args.style === 'in-magnet' || args.style === 'out-magnet') {
            // draw triangle pointer
            this.ctx.beginPath();
            if (args.style === 'in-magnet') {
                this.ctx.moveTo(rectArgs.x - 1,     args.y - 3);
                this.ctx.lineTo(rectArgs.x + 3, args.y);
                this.ctx.lineTo(rectArgs.x - 1,     args.y + 3);
            } else {
                this.ctx.moveTo(rectArgs.x + 1,     args.y - 3);
                this.ctx.lineTo(rectArgs.x - 3, args.y);
                this.ctx.lineTo(rectArgs.x + 1,     args.y + 3);
            }
            this.ctx.closePath();
            this.ctx.fill();
        } else if (args.style === 'callout') {
            // draw triangle pointer
            this.ctx.beginPath();
            this.ctx.moveTo(rectArgs.x + 1, args.y - 5);
            this.ctx.lineTo(rectArgs.x - 5, args.y);
            this.ctx.lineTo(rectArgs.x + 1, args.y + 5);
            this.ctx.closePath();
            this.ctx.fill();
        }

        this.ctx.globalAlpha = 1;
        this.ctx.fillStyle = textFill;
        this.ctx.fillText(args.text, args.x, args.y);
        this.ctx.restore();
    };

    /**
     * Draw a rectangle or rounded rectangle.
     * 
     * @param rectangle.x: the left edge of the rectangle
     * @param rectangle.y: the top edge of the rectangle
     * @param rectangle.width: the width of the rectangle
     * @param rectangle.height: the height of the rectangle
     * @param rectangle.r: the radius of the corners, or undefined for a regular
     *  rectangle
     */
    fillRect(rectangle: Rectangle) {
        if (rectangle.r === undefined) {
            this.ctx.fillRect(rectangle.x, rectangle.y, rectangle.width, rectangle.height);
        } else {
            this.buildRect(rectangle);
            this.ctx.fill();
        }
    };
    strokeRect(rectangle: Rectangle) {
        if (rectangle.r === undefined) {
            this.ctx.strokeRect(rectangle.x, rectangle.y, rectangle.width, rectangle.height);
        } else {
            this.buildRect(rectangle);
            this.ctx.stroke();
        }
    };
    buildRect(rectangle: Rectangle) {
        this.ctx.beginPath();
        // middle of top edge
        this.ctx.moveTo(rectangle.x + rectangle.width / 2, rectangle.y);
        // to middle of right edge
        this.ctx.arcTo(
                rectangle.x + rectangle.width, rectangle.y,
                rectangle.x + rectangle.width, rectangle.y + rectangle.height / 2,
                rectangle.r);
        // to middle of bottom edge
        this.ctx.arcTo(
                rectangle.x + rectangle.width, rectangle.y + rectangle.height,
                rectangle.x + rectangle.width / 2, rectangle.y + rectangle.height,
                rectangle.r);
        // to middle of left edge
        this.ctx.arcTo(
                rectangle.x, rectangle.y + rectangle.height,
                rectangle.x, rectangle.y + rectangle.height / 2,
                rectangle.r);
        // to middle of top edge
        this.ctx.arcTo(
                rectangle.x, rectangle.y,
                rectangle.x + rectangle.width / 2, rectangle.y,
                rectangle.r);
        this.ctx.closePath();
    };
}

export interface CanvasObject {
    draw(ctx: CanvasRenderingContext2D): void;
    doDown(cs: CanvasState, e: Event): void;
    contains(x: number, y: number, pad?: number): boolean;
}

export interface CNode extends CanvasObject {
    highlight(ctx: CanvasRenderingContext2D): void;
    getVertices(): Point[];
    getLabel(): NodeLabel;
    deleteFrom(cs: CanvasState): void;
    unlightMagnets(): void;
    setMagnetPosition(): void;
    getMouseTarget(x: number, y: number, skip_check?: boolean): BaseNode|CNode|Magnet;
    x: number;
    y: number;
    dx: number;
    dy: number;
    fill: string;
    w: number;
    h: number;
    label: string;
    in_magnets: Magnet[];
    out_magnets: Magnet[];
    affects_exec_order: boolean;
    has_unsaved_changes: boolean;
    status?: string;

    /* @todo: investigate where these came from */
    dataset_id?: any;
    run_id?: any;
}

export interface INodeUpdateSignalMap {
    "no update available": INodeUpdateSignalDefinition;
    "updated": INodeUpdateSignalDefinition;
    "updated with issues": INodeUpdateSignalDefinition;
    "unavailable": INodeUpdateSignalDefinition;
    "update in progress": INodeUpdateSignalDefinition;
}
interface INodeUpdateSignalDefinition {
    color: string;
    icon: HTMLImageElement;
}

export const STATUS_COLOR_MAP = {
    CLEAR: 'green',
    FAILED: 'red',
    RUNNING: 'orange',
    READY: 'orange',
    WAITING: 'yellow'
};

function removeFromArray(array: Array<any>, obj: any) {
    array.splice(array.indexOf(obj), 1);
}
function deleteFromTemplate(cs: CanvasState) {
    for (let out_magnet of this.out_magnets) {
        for (let connector of out_magnet.connected) {
            removeFromArray(cs.connectors, connector);
            if (connector.dest !== undefined &&
                    connector.dest instanceof Magnet) {
                // in-magnets can accept only one Connector
                connector.dest.connected = [];
            }
        }
        out_magnet.connected = [];
    }
    removeFromArray(cs.shapes, this);
    removeFromArray(cs.inputs, this);
}

/**
 * A base class for all nodes. (RawNode, CdtNode, MethodNode, OutputNode)
 * Doesn't do much yet.
 */
abstract class BaseNode {
    affects_exec_order = false;
    in_magnets: Magnet[];
    out_magnets: Magnet[];
    x: number;
    y: number;
    dx: number;
    dy: number;
    w: number;
    h: number;
    has_unsaved_changes: boolean;
    status?: string; // for view_run

    constructor() { }

    contains(x: number, y: number) {
        // to be replaced by child classes
        return false;
    }
    getVertices() {
        return [];
    }
    unlightMagnets() {
        for (let magnet of this.in_magnets) {
            magnet.fill = '#fff';
            magnet.acceptingConnector = false;
        }
    }
    getMouseTarget(mx: number, my: number, skip_check?: boolean): BaseNode|CNode|Magnet {
        if (skip_check || this.contains(mx, my)) {
            // are we clicking on a magnet?
            for (let magnet of this.out_magnets.concat(this.in_magnets)) {
                if (magnet.contains(mx, my)) {
                    return magnet;
                }
            }

            // otherwise return the shape object.
            return this;
        }
        return null;
    }
    doDown(canvasState, e) {
        var i = canvasState.shapes.indexOf(this),
            sel = canvasState.selection,
            sel_stack_ix = sel.indexOf(this),
            pos = canvasState.getPos(e);

        // this shape is now on top.
        canvasState.shapes.push(canvasState.shapes.splice(i, 1)[0]);

        // moving the shape
        canvasState.dragoffx = pos.x - this.x;
        canvasState.dragoffy = pos.y - this.y;

        if (e.shiftKey && sel_stack_ix > -1) {
            sel.splice(sel_stack_ix, 1);
        } else if (e.shiftKey) {
            sel.push(this);
        } else {
            canvasState.selection = [ this ];
        }
    }

    isConnectedTo(node: CNode) {
        var magnetConnectors = magnet => magnet.connected;
        // may contain duplicates, but that's ok
        var connectors = [].concat(
            ...this.in_magnets.map(magnetConnectors),
            ...this.out_magnets.map(magnetConnectors),
            ...node.in_magnets.map(magnetConnectors),
            ...node.out_magnets.map(magnetConnectors)
        );

        for (let connector of connectors) {
            if ((connector.source.parent === this && connector.dest.parent === node) ||
                (connector.source.parent === node && connector.dest.parent === this))
                return true;
        }
        return false;
    }

    setCoordsFromIso(x: number, y: number) {
        let coords = Geometry.isoTo2D(x, y);
        this.x = coords.x;
        this.y = coords.y;
        this.dx = this.dy = 0;
        this.setMagnetPosition();
    }
    setMagnetPosition() {
        // placeholder - child classes must set
    }
    draw(ctx: CanvasRenderingContext2D) {
        // placeholder - child classes must set
    }

    isNode() {
        return true;
    }
}

/**
 * A base class for both cylindrical nodes: RawNode and OutputNode.
 */
class CylinderNode extends BaseNode {
    /*
    BaseNode rendered as a cylinder.
     */
    dx = 0; // display offset to avoid collisions, relative to its "true" coordinates
    dy = 0;
    r = 20; // x-radius (ellipse)
    r2 = this.r / 2; // y-radius (ellipse)
    w = this.r; // for compatibility
    h = 25; // height of cylinder
    offset = 18; // distance of label from center
    fill = "grey";
    found_fill: string;
    found_md5: boolean;
    magnetOffset: Point = { x: -12, y: -this.h / 2 };
    in_magnets = [];
    out_magnets = [];
    highlightStroke: string;

    constructor(public x: number, public y: number, public label: string) {
        super();
    }

    private getTopEllipse(): Ellipse {
        return {
            x: this.x + this.dx,
            y: this.y + this.dy - this.h / 2,
            rx: this.r,
            ry: this.r2
        };
    }
    private getBottomEllipse(): Ellipse {
        return {
            x: this.x + this.dx,
            y: this.y + this.dy + this.h / 2,
            rx: this.r,
            ry: this.r2
        };
    }
    private getStack(): Rectangle {
        return {
            x: this.x + this.dx - this.r,
            y: this.y + this.dy - this.h / 2,
            width: this.r * 2,
            height: this.h
        };
    }
    draw(ctx: CanvasRenderingContext2D) {
        var canvas = new CanvasWrapper(undefined, ctx),
            top_ellipse = this.getTopEllipse(),
            bottom_ellipse = this.getBottomEllipse(),
            stack = this.getStack();

        // draw bottom ellipse
        ctx.fillStyle = this.found_md5 ? this.found_fill : this.fill;
        canvas.drawEllipse(bottom_ellipse);
        // draw stack
        canvas.fillRect(stack);
        // draw top ellipse
        canvas.drawEllipse(top_ellipse);
        // some shading
        ctx.fillStyle = '#fff';
        ctx.globalAlpha = 0.35;
        canvas.drawEllipse(top_ellipse);
        ctx.globalAlpha = 1.0;
        // draw magnet
        this.setMagnetPosition();
        var magnet = this.in_magnets[0] || this.out_magnets[0];
        magnet.draw(ctx);
    }
    setMagnetPosition() {
        var magnet = this.in_magnets[0] || this.out_magnets[0];
        magnet.x = this.x + this.dx + this.magnetOffset.x;
        magnet.y = this.y + this.dy + this.magnetOffset.y;
    }
    highlight(ctx: CanvasRenderingContext2D) {
        var canvas = new CanvasWrapper(undefined, ctx);
        canvas.strokeEllipse(this.getBottomEllipse());
        canvas.strokeRect(this.getStack());
        canvas.strokeEllipse(this.getTopEllipse());
    }
    contains (mx: number, my: number): boolean {
        // node is comprised of a rectangle and two ellipses
        return Geometry.inRectangle(mx, my, this.getStack()) ||
            Geometry.inEllipse(mx, my, this.getBottomEllipse()) ||
            Geometry.inEllipse(mx, my, this.getTopEllipse());
    }
    getVertices(): Point[] {
        var cx = this.x + this.dx,
            cy = this.y + this.dy;

        var x1 = cx + this.r,
            x2 = cx - this.r,
            y1 = cy + this.h / 2,
            y2 = y1 - this.h;

        // Include centre to collide with small objects completely inside border.
        return [
            { x: cx, y: cy },
            { x: x1, y: y1 },
            { x: x2, y: y1 },
            { x: x1, y: y2 },
            { x: x2, y: y2 },
            { x: cx, y: cy + this.h / 2 + this.r2 },
            { x: cx, y: cy - this.h / 2 - this.r2 }
        ];
    }
    getLabel() {
        return new NodeLabel(
                this.label,
                this.x + this.dx,
                this.y + this.dy - this.h / 2 - this.offset,
                this.has_unsaved_changes ? '*' : '');
    }
}

export class RawNode extends CylinderNode implements CNode {
    /*
    BaseNode representing an unstructured (raw) datatype.
     */
    fill = "#8D8";
    found_fill = "blue";
    inset = 10; // distance of magnet from center
    dataset_id: number;

    /* for view_run */
    md5: string;
    run_id: string;

    constructor(public x, public y, public label = "", public input_index?) {
        super(x, y, label);
        this.magnetOffset = {x: 10, y: this.r2 / 2};
        // Input node always has one magnet
        this.out_magnets = [
            new Magnet(this, 5, 2, "white", null, this.label, null, true)
        ];
    };

    isInputNode() {
        return true;
    }
    isRawNode() {
        return true;
    }

    deleteFrom = deleteFromTemplate;
}

export class CdtNode extends BaseNode implements CNode {
    dx = 0; // display offset to avoid collisions, relative to its "true" coordinates
    dy = 0;
    w = 45;
    h = 28;
    fill = "#88D";
    found_fill = "blue";
    inset = 13;
    offset = 15;
    in_magnets = [];
    out_magnets = [];

    /* for view_run */
    md5: string;
    found_md5: boolean;
    dataset_id: number;
    run_id: string;

    /*
    BaseNode represents a Compound Datatype (CSV structured data).
    Rendered as a square shape.
     */
    constructor(public pk, public x, public y, public label = "", public input_index?) {
        super();
        this.out_magnets = [
            new Magnet(this, 5, 2, "white", this.pk, this.label, null, true, pk)
        ];
    };

    draw(ctx: CanvasRenderingContext2D): void {
        let cx = this.x + this.dx;
        let cy = this.y + this.dy;
        let w2 = this.w / 2;
        let h2 = this.h / 2;
        let prism_cap = cy - h2;
        let prism_base = cy + h2;

        ctx.lineJoin = 'bevel';
        ctx.fillStyle = this.found_md5 ? this.found_fill : this.fill;

        // draw base
        ctx.beginPath();
        ctx.moveTo(cx - w2, prism_base);
        ctx.lineTo(cx, prism_base + w2 / 2);
        ctx.lineTo(cx + w2, prism_base);
        ctx.lineTo(cx + w2, prism_base - this.h);
        ctx.lineTo(cx - w2, prism_base - this.h);
        ctx.closePath();
        ctx.fill();

        // draw top
        ctx.beginPath();
        ctx.moveTo(cx - w2, prism_cap);
        ctx.lineTo(cx, prism_cap + w2 / 2);
        ctx.lineTo(cx + w2, prism_cap);
        ctx.lineTo(cx, prism_cap - w2 / 2);
        ctx.closePath();
        ctx.fill();

        // some shading
        ctx.fillStyle = '#fff';
        ctx.globalAlpha = 0.35;
        ctx.fill();
        ctx.globalAlpha = 1.0;

        // draw magnet
        this.setMagnetPosition();
        this.out_magnets[0].draw(ctx);
    }
    setMagnetPosition() {
        var out_magnet = this.out_magnets[0];
        out_magnet.x = this.x + this.dx + this.inset;
        out_magnet.y = this.y + this.dy + this.w / 8;
    }
    getVertices(): Point[] {
        let cx = this.x + this.dx;
        let cy = this.y + this.dy;
        let w2 = this.w / 2;
        let h2 = this.h / 2;
        let prism_cap = cy - h2;
        let prism_base = cy + h2;

        return [
            { x: cx,      y: cy },
            { x: cx - w2, y: prism_base },
            { x: cx,      y: prism_base + w2 / 2 },
            { x: cx + w2, y: prism_base },
            { x: cx + w2, y: prism_cap },
            { x: cx,      y: prism_cap - w2 / 2 },
            { x: cx - w2, y: prism_cap }
        ];
    }
    highlight(ctx): void {
        var cx = this.x + this.dx,
            cy = this.y + this.dy;

        ctx.lineJoin = 'bevel';

        var w2 = this.w / 2,
            h2 = this.h / 2,
            butt = cy + h2,
            cap = cy - h2;

        ctx.beginPath();
        ctx.moveTo(cx - w2, butt);
        ctx.lineTo(cx,      butt + w2 / 2);
        ctx.lineTo(cx + w2, butt);
        ctx.lineTo(cx + w2, cap);
        ctx.lineTo(cx,      cap - w2 / 2);
        ctx.lineTo(cx - w2, cap);
        ctx.closePath();
        ctx.stroke();
    }
    contains (mx, my) {
        /*
        Are mouse coordinates within the perimeter of this node?
         */
        var dx = Math.abs(this.x + this.dx - mx),
            dy = Math.abs(this.y + this.dy - my);

        // mouse coords are within the 4 diagonal lines.
        // can be checked with 1 expression because the 4 lines are mirror images of each other
        return this.h / 2 + this.w / 4 - dy > dx / 2 &&
        // then check the horizontal boundaries on the sides of the hexagon
            dx < this.w / 2;
    }
    getLabel() {
        return new NodeLabel(
                this.label,
                this.x + this.dx,
                this.y + this.dy - this.h / 2 - this.offset,
                this.has_unsaved_changes ? '*' : '');
    }

    isInputNode() {
        return true;
    }
    isCdtNode() {
        return true;
    }

    deleteFrom = deleteFromTemplate;
}

/**
 * A whistle-shaped object with anchors for all the inputs and outputs.
 *
 * The top and bottom sides expand to fit the number of inputs or outputs.
 * @param pk: the method key
 * @param family: the method family key
 * @param x, y: position of the method on the screen
 * @param fill: colour to draw the method with
 * @param label: string value
 * @param inputs: an array of input details
 *  [ {dataset_idx: 'dataset index', dataset_name: 'name', structure: null or
 *  {compounddatatype: pk} }]
 * @param outputs: an array of output details, same structure
 * @param status: describes progress during a run, possible values are the
 *  keys in STATUS_COLOR_MAP
 */
export class MethodNode extends BaseNode implements CNode {

    dx = 0; // display offset to avoid collisions, relative to its "true" coordinates
    dy = 0;
    inset = 10; // distance from left or right edge to center of hole
    offset = 10; // space between bottom of node and label
    spacing = 20; // separation between pins
    stack = 20;
    scoop = 45;
    in_magnets: Magnet[] = [];
    out_magnets: Magnet[] = [];
    affects_exec_order = true;
    n_inputs: number;
    n_outputs: number;
    h: number;

    /* @todo: figure out what these are */
    new_dependencies;
    new_code_resource_revision;
    log_id;
    run_id;

    private input_plane_len;
    private vertices: Point[];
    private prevX: number;
    private prevY: number;
    private update_signal: NodeUpdateSignal;

    constructor (
            public x,
            public y,
            public fill,
            public label,
            public inputs,
            public outputs,
            public status? // Members for instances of methods in runs
        ) {
        super();
        this.x = x || 0;
        this.y = y || 0;
        this.fill = fill || '#999';
        this.label = label || '';
        this.n_inputs = Object.keys(inputs).length;
        this.n_outputs = Object.keys(outputs).length;
        this.h = Math.max(this.n_inputs, this.n_outputs) * this.spacing;
        if (this.n_inputs === 0) {
            throw "No inputs passed to MethodNode."
        }
        if (this.n_outputs === 0) {
            throw "No outputs passed to MethodNode."
        }

        for (let input of this.inputs) {
            this.addInput(input);
        }
        for (let output of this.outputs) {
            this.addOutput(output);
        }
    }

    private addInput(input, r = 5, attract = 5, magnet_fill = '#fff') {
        this.addXput(input, this.in_magnets, false, r, attract, magnet_fill);
    }
    private addOutput(output_name, r = 5, attract = 5, magnet_fill = '#fff') {
        this.addXput(
            {dataset_name: output_name},
            this.out_magnets,
            true,
            r,
            attract,
            magnet_fill);
    }
    private addXput(input, magnet_array, is_output, r = 5, attract = 5, magnet_fill = '#fff') {
        var cdt_pk = null;
        magnet_array.push(new Magnet(
            this, r, attract, magnet_fill,
            cdt_pk, input.dataset_name, null, is_output, null,
            false
        ));
    }

    buildBodyPath(ctx) {
        var vertices = this.getVertices();
        ctx.beginPath();

        // body
        ctx.moveTo( vertices[4].x, vertices[4].y );
        ctx.lineTo( vertices[5].x, vertices[5].y );
        ctx.lineTo( vertices[6].x, vertices[6].y );
        ctx.bezierCurveTo(
                vertices[10].x, vertices[10].y,
                vertices[10].x, vertices[10].y,
                vertices[1].x, vertices[1].y );
        ctx.lineTo( vertices[2].x, vertices[2].y );
        ctx.lineTo( vertices[3].x, vertices[3].y );
        ctx.bezierCurveTo(
                vertices[8].x, vertices[8].y,
                vertices[8].x, vertices[8].y,
                vertices[4].x, vertices[4].y );
        ctx.closePath();
    }

    draw(ctx: CanvasRenderingContext2D) {
        ctx.fillStyle = this.fill;
        var vxs = this.getVertices();

        // body
        this.buildBodyPath(ctx);
        ctx.fill();

        // input plane (shading)
        ctx.beginPath();
        ctx.moveTo( vxs[0].x, vxs[0].y );
        ctx.lineTo( vxs[1].x, vxs[1].y );
        ctx.lineTo( vxs[2].x, vxs[2].y );
        ctx.lineTo( vxs[3].x, vxs[3].y );
        ctx.fillStyle = '#fff';
        ctx.globalAlpha = 0.35;
        ctx.fill();

        // top bend (shading)
        ctx.beginPath();
        ctx.moveTo( vxs[6].x, vxs[6].y );
        ctx.lineTo( vxs[7].x, vxs[7].y );
        ctx.bezierCurveTo( vxs[9].x,  vxs[9].y,  vxs[9].x,  vxs[9].y,  vxs[0].x, vxs[0].y );
        ctx.lineTo( vxs[1].x, vxs[1].y );
        ctx.bezierCurveTo( vxs[10].x, vxs[10].y, vxs[10].x, vxs[10].y, vxs[6].x, vxs[6].y );
        ctx.globalAlpha = 0.12;
        ctx.fill();

        ctx.fillStyle = this.fill;
        ctx.globalAlpha = 1.0;

        // draw magnets
        this.setMagnetPosition();
        for (let magnet of this.in_magnets.concat(this.out_magnets)) {
            magnet.draw(ctx);
        }

        // update signal
        if (this.update_signal) {
            this.update_signal.x = vxs[6].x - this.update_signal.r;
            this.update_signal.y = vxs[2].y + this.update_signal.r;
            this.update_signal.draw(ctx);
        }
    }

    setMagnetPosition() {
        let magnet, pos;
        let cx = this.x + this.dx,
            cy = this.y + this.dy,
            cos30 = Math.sqrt(3) / 2,
            magnet_margin = 6,
            c2c = this.in_magnets[0].r * 2 + magnet_margin,
            y_inputs = cy - this.stack,
            x_outputs = cx + this.scoop * cos30,
            y_outputs = cy + this.scoop * 0.5;
        for (let i = 0, len = this.in_magnets.length; i < len; i++) {
            magnet = this.in_magnets[i];
            pos = (i - len / 2 + 0.5) * c2c;
            magnet.x = cx + pos * cos30;
            magnet.y = y_inputs - pos / 2;
        }
        for (let i = 0, len = this.out_magnets.length; i < len; i++) {
            magnet = this.out_magnets[i];
            pos = (i - len / 2 + 0.5) * c2c;
            magnet.x = x_outputs + pos * cos30;
            magnet.y = y_outputs - pos / 2;
        }
    }

    highlight (ctx: CanvasRenderingContext2D) {
        // highlight this node shape
        this.buildBodyPath(ctx);
        ctx.stroke();
    };

    contains (mx, my) {
        var vertices = this.getVertices();
        return Geometry.inPolygon(mx, my,
            [ 1, 2, 3, 8, 4, 5, 6, 10 ].map(i => vertices[i])
        );
    }

    getVertices () {
        var cx = this.x + this.dx,
            cy = this.y + this.dy;
        if (this.vertices === undefined ||
                cx !== this.prevX ||
                cy !== this.prevY) {
            // experimental draw
            var cos30 = Math.sqrt(3) / 2,
             // sin30 = 0.5 (this is trivial)
                magnet_radius = this.in_magnets[0].r,
                magnet_margin = 6,
                dmc = magnet_radius + magnet_margin, // distance from magnet centre to edge
                c2c = dmc + magnet_radius, // centre 2 centre of adjacent magnets
                cosdmc = cos30 * dmc,
                ipy = cy - this.stack,
                input_plane_len  = (this.n_inputs * c2c + magnet_margin) / 2,
                cosipl = cos30 * input_plane_len;

            var opx = cx + this.scoop * cos30,
                opy = cy + this.scoop * 0.5,
                output_plane_len = (this.n_outputs * c2c + magnet_margin) / 2,
                cosopl = cos30 * output_plane_len; // half of the length of the parallelogram ("half hypoteneuse")

            var vertices = [
                { x: cx + cosdmc - cosipl, y: ipy + (dmc + input_plane_len) / 2 },
                { x: cx + cosdmc + cosipl, y: ipy + (dmc - input_plane_len) / 2 },
                { x: cx - cosdmc + cosipl, y: ipy - (dmc + input_plane_len) / 2 },
                { x: cx - cosdmc - cosipl, y: ipy - (dmc - input_plane_len) / 2 },
                { x: opx - cosopl, y: opy + dmc + output_plane_len / 2 },
                { x: opx + cosopl, y: opy + dmc - output_plane_len / 2 },
                { x: opx + cosopl, y: opy - dmc - output_plane_len / 2 },
                { x: opx - cosopl, y: opy - dmc + output_plane_len / 2 }
            ];

            if (this.in_magnets.length > this.out_magnets.length) {
                vertices.push(
                    { x: cx - cosdmc - cosopl, y: cy + (dmc + output_plane_len) / 2 },
                    { x: cx + cosdmc - cosopl, y: cy - (dmc - output_plane_len) / 2 },
                    { x: cx + cosdmc + cosopl, y: cy - (dmc + output_plane_len) / 2 }
                );
            } else {
                vertices.push(
                    { x: cx - cosdmc - cosipl, y: cy + cosdmc - (dmc - input_plane_len) / 2 },
                    { x: cx + cosdmc - cosipl, y: cy - cosdmc + (dmc + input_plane_len) / 2 },
                    { x: cx + cosdmc + cosipl, y: cy - cosdmc + (dmc - input_plane_len) / 2 }
                );
            }

            this.prevX = cx;
            this.prevY = cy;
            this.vertices = vertices;
            this.input_plane_len = input_plane_len;
        }

        return this.vertices;
    };

    getLabel() {
        return new NodeLabel(
                this.label,
                this.x + this.dx + this.scoop / 4,
                this.y + this.dy - this.stack - this.input_plane_len / 2 - this.offset,
                this.has_unsaved_changes ? '*' : '');
    };

    doDown (cs, e) {
        super.doDown(cs, e);
    };

    updateSignal (status) {
        if (status !== undefined) {
            if (this.update_signal instanceof NodeUpdateSignal) {
                this.update_signal.setStatus(status);
            } else {
                this.update_signal = new NodeUpdateSignal(this, status);
            }
        } else {
            return this.update_signal;
        }
    };

    isFullyConnected () {
        var is_fully_connected: boolean;
        for (let magnet of this.in_magnets) {
            is_fully_connected = magnet.connected.length !== 0;
            if (!is_fully_connected) break;
        }
        if (is_fully_connected) for (let magnet of this.out_magnets) {
            is_fully_connected = magnet.connected.length !== 0;
            if (!is_fully_connected) break;
        }
        return is_fully_connected;
    };

    isMethodNode() {
        return true;
    }

    deleteFrom (cs) {
        var magnets = this.in_magnets.concat(this.out_magnets);
        // delete Connectors
        for (let magnet of magnets) {
            // this loop done in reverse so that deletions do not re-index the array
            for (let i = magnet.connected.length - 1; i >= 0; i--) {
                magnet.connected[i].deleteFrom(cs);
            }
        }
        // remove MethodNode from list and any attached Connectors
        removeFromArray(cs.shapes, this);
        removeFromArray(cs.methods, this);
    };
}

export class Magnet implements CanvasObject {
    /*
    A Magnet is the attachment point for a BaseNode (shape) given a
    Connector.  It is always contained within a shape.
    x and y coordinates will be set by parent object draw().
     */

    x = null;
    y = null;
    connected = [];  // hold references to Connectors
    acceptingConnector = false; // true if a connector is being dragged
    isInput: boolean;

    constructor(
            public parent,
            public r = 5,
            public attract = 5,
            public fill = '#fff',
            public cdt?,
            public label = '',
            public offset = 5,
            public isOutput = false,
            public pk = null,
            public toDelete = false) {
        this.isInput = !this.isOutput;
    }
    draw (ctx: CanvasRenderingContext2D) {
        // magnet coords are set by containing shape
        var canvas = new CanvasWrapper(undefined, ctx);

        ctx.fillStyle = '#fff';
        canvas.drawCircle(this);
        if (this.acceptingConnector) {
            ctx.fillStyle = '#ff0';
            canvas.drawCircle({ x: this.x, y: this.y, r: this.r - 1.5 });
            this.highlight(ctx);
        } else if (this.toDelete) {
            ctx.fillStyle = '#000';
            canvas.drawCircle({ x: this.x, y: this.y, r: this.r - 1.5 });
        }
    }
    highlight (ctx: CanvasRenderingContext2D) {
        ctx.fillStyle = '#fff';
        new CanvasWrapper(undefined, ctx).drawText({
            x: this.x + (this.r + this.offset) * +(this.isOutput || -1),
            y: this.y,
            text: this.label,
            dir: 0,
            style: this.isOutput ? "out-magnet" : "in-magnet"
        });
    }
    contains (mx, my) {
        var dx = this.x - mx;
        var dy = this.y - my;
        return Math.sqrt(dx * dx + dy * dy) <= this.r + this.attract;
    }
    doDown (cs, e) {
        if (this.isInput) {
            if (this.connected.length) {
                this.connected[0].doDown(cs, e); // select connector instead
            } else {
                this.parent.doDown(cs, e); // select magnet's parent instead
            }
        } else if (e.shiftKey && cs.selection.length !== 0 || !cs.can_edit) {
            // out magnet that can't create a connector
            this.parent.doDown(cs, e);
        } else {
            // The only way to get here is with an out magnet
            // we want to create a connector for.
            var pos = cs.getPos(e),
                conn = new Connector(this);
            cs.connectors.push(conn);
            this.connected.push(conn);
            cs.selection = [ conn ];
            cs.dragoffx = pos.x - conn.fromX;
            cs.dragoffy = pos.y - conn.fromY;
        }
    }

    tryAcceptConnector (conn: Connector): boolean {
        if (!this.connected.length &&
                this.contains(conn.x, conn.y)) {
            // jump to magnet
            conn.x = this.x;
            conn.y = this.y;
            this.connected = [ conn ];
            conn.dest = this;
            this.acceptingConnector = false;
            // OutputNodes don't care about datatype.
            if (CanvasState.isOutputNode(this.parent)) {
                this.cdt = conn.source.cdt;
            }
            return true;
        }
        return false;
    }

    isMagnet() {
        return true;
    }
}

export class Connector implements CanvasObject {
    /*
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

    dest: Magnet = null;
    source: Magnet;
    fromX: number;
    fromY: number;
    x: number;
    y: number;
    private ctrl1: Point;
    private ctrl2: Point;
    private prevX: number;
    private prevY: number;
    private dx: number;
    private dy: number;
    private label_width;
    private measured_text: string;

    constructor(out_magnet: Magnet) {
        this.source = out_magnet;
        this.x = this.fromX = out_magnet.x; // for compatibility with shape-based functions
        this.y = this.fromY = out_magnet.y;
    }
    draw (ctx: CanvasRenderingContext2D) {
        /*
         Draw a line to represent a Connector originating from a Magnet.
         */
        var canvas = new CanvasWrapper(undefined, ctx);
        this.calculateCurve();
        ctx.strokeStyle = '#abc';
        ctx.lineWidth = 6;
        ctx.lineCap = 'round';

        if (this.dest === null) {
            // if connector doesn't have a destination yet,
            // give it the label of the source magnet it's coming from

            // save the canvas state to start applying transformations
            ctx.save();
            ctx.fillStyle = '#aaa';
            canvas.drawText({
                x: this.x + 2,
                y: this.y + 5,
                text: this.source.label,
                dir: 1,
                style: "connector"});
            ctx.restore();
            ctx.canvas.style.cursor = "none";
        }
        else {
            // Recolour this path if the statuses of the source and dest are meaningful
            var src = this.source.parent,
                cable_stat;

            if (typeof src.status === 'string') {
                cable_stat = "RUNNING";

                // Upper cable is done!
                if (src.status === 'CLEAR' && typeof this.dest.parent.status === 'string' ) {
                    // Whatever, everything else is fine!
                    cable_stat = "CLEAR";
                }
                else if (src.status === 'FAILED') {// Source is borked
                    // so is any cable that pokes out of it...
                    cable_stat = "FAILED";
                }
            }

            ctx.strokeStyle = STATUS_COLOR_MAP[cable_stat] || ctx.strokeStyle;
        }

        ctx.beginPath();
        ctx.moveTo(this.fromX, this.fromY);
        ctx.bezierCurveTo(
            this.ctrl1.x, this.ctrl1.y,
            this.ctrl2.x, this.ctrl2.y,
            this.x, this.y);
        ctx.stroke();
    }
    highlight (ctx: CanvasRenderingContext2D) {
        /*
         Highlight this Connector by drawing another line along
         its length. Colour and line width set by canvasState.
         */
        this.calculateCurve();
        ctx.beginPath();
        ctx.moveTo(this.fromX, this.fromY);
        ctx.bezierCurveTo(this.ctrl1.x, this.ctrl1.y, this.ctrl2.x, this.ctrl2.y, this.x, this.y);
        ctx.stroke();
    }

    // make an object in the format of bezier lib
    calculateCurve(): Point[] {
        if (this.dest !== null && this.dest !== undefined) {
            // move with the attached shape
            this.x = this.dest.x;
            this.y = this.dest.y;
        }
        if (this.ctrl1 === undefined ||
                this.fromX !== this.source.x ||
                this.fromY !== this.source.y ||
                this.x !== this.prevX ||
                this.y !== this.prevY) {
            // Either the curve has never been calculated or an end moved.
            this.fromX = this.source.x;
            this.fromY = this.source.y;
            this.prevX = this.x;
            this.prevY = this.y;

            this.dx = this.x - this.fromX;
            this.dy = this.y - this.fromY;

            this.ctrl1 = {
                /*
                 - Comes from origin at a 30-degree angle
                 - cos(30) = sqrt(3)/2  &  sin(30) = 0.5
                 - Distance of ctrl from origin is 70% of dx
                 - Minimum dx is 50, so minimum distance of ctrl is 35.
                 */
                x: this.fromX + Math.max(this.dx, 50) * Math.sqrt(3) / 2 * 0.7,
                y: this.fromY + Math.max(this.dx, 50) / 2 * 0.7
            };
            this.ctrl2 = {
                /*
                 - Vertical offset is 2/3 of dy, or 2/5 of -dy, whichever is positive
                 - Minimum vertical offset is 50
                 - Horizontal offset is 10% of dx
                 */
                x: this.x - this.dx / 10,
                y: this.y - Math.max( (this.dy > 0 ? 1 : -0.6) * this.dy, 50) / 1.5
            };
        }
        return [
            { x: this.fromX, y: this.fromY },
            this.ctrl1, this.ctrl2,
            { x: this.x, y: this.y }
        ];
    }

    drawLabel (ctx: CanvasRenderingContext2D) {
        let builtLabel;
        if (builtLabel = this.buildLabel(ctx)) {
            // save the canvas state to start applying transformations
            ctx.save();
            // set the bezier midpoint as the origin
            ctx.translate(builtLabel.centre.x, builtLabel.centre.y);
            ctx.rotate(builtLabel.rotate);
            (new CanvasWrapper(undefined, ctx)).drawText({
                x: 0, y: 0, dir: 0,
                text: builtLabel.label,
                style: "connector"
            });
            ctx.restore();
        }
    }

    buildLabel (ctx: CanvasRenderingContext2D) {
        var jsb = this.calculateCurve();
        var label = this.source.label;

        if (this.source.label !== this.dest.label) {
            label += "->" + this.dest.label;
        }

        if (label !== this.measured_text) {
            this.measured_text = label;
            this.label_width = ctx.measureText(this.measured_text).width + 12;
        }

        this.dx = this.x - this.fromX;
        this.dy = this.y - this.fromY;

        // only draw if it's long enough to contain the text
        if ( this.dx * this.dx + this.dy * this.dy > this.label_width * this.label_width * 2) {
            // determine the angle of the bezier at the midpoint
            var midpoint = Bezier.nearestPointOnCurve({
                x: this.fromX + this.dx / 2,
                y: this.fromY + this.dy / 2
            }, jsb);

            return {
                // set the bezier midpoint as the origin
                centre: midpoint.point,
                rotate: Bezier.gradientAtPoint(jsb, midpoint.location),
                rect: {
                    x: -this.label_width / 2,
                    y: -7,
                    r: 6,
                    width: this.label_width,
                    height: 14
                },
                label
            };
        }
    }
    // debug = function(ctx) {
    //     var jsb = this.calculateCurve(),
    //         midpoint = jsBezier.nearestPointOnCurve({ x: this.fromX + this.dx/2, y: this.fromY + this.dy/2 }, jsb),
    //         midpointAngle = jsBezier.gradientAtPoint(jsb, midpoint.location),
    //         wrong_midpoint = jsBezier.pointOnCurve(jsb, 0.5);
    //
    //     ctx.fillStyle = '#000';
    //     ctx.beginPath();
    //     ctx.arc(this.ctrl1.x, this.ctrl1.y, 5, 0, 2 * Math.PI, true);
    //     ctx.arc(this.ctrl2.x, this.ctrl2.y, 5, 0, 2 * Math.PI, true);
    //     ctx.fill();
    //
    //     ctx.fillStyle = '#ff0';
    //     ctx.beginPath();
    //     ctx.arc(wrong_midpoint.x, wrong_midpoint.y, 5, 0, 2 * Math.PI, true);
    //     ctx.fill();
    //
    //     var atan_bez = function(pts) {
    //         var lin_dy = pts[1].y - pts[0].y,
    //             lin_dx = pts[1].x - pts[0].x;
    //         return Math.atan(lin_dy / lin_dx);
    //     };
    //
    //     var quadPt = [];
    //     ctx.fillStyle = '#600';
    //     for (var i=0; i+1 < jsb.length; i++) {
    //         var quadMid = {
    //             x: (jsb[i+1].x - jsb[i].x) * midpoint.location + jsb[i].x,
    //             y: (jsb[i+1].y - jsb[i].y) * midpoint.location + jsb[i].y
    //         };
    //         quadPt.push(quadMid);
    //         ctx.beginPath();
    //         ctx.arc(quadMid.x, quadMid.y, 5, 0, 2 * Math.PI, true);
    //         ctx.fill();
    //     }
    //     console.group('quadratic tangents');
    //     for (i=0; i < quadPt.length - 1; i++)
    //         console.log(atan_bez(quadPt.slice(i, i+2)) * 180/Math.PI);
    //     console.groupEnd();
    //
    //     var linPt = [];
    //     ctx.fillStyle = '#A00';
    //     for (i=0; i+1 < quadPt.length; i++) {
    //         var linMid = {
    //             x: (quadPt[i+1].x - quadPt[i].x) * midpoint.location + quadPt[i].x,
    //             y: (quadPt[i+1].y - quadPt[i].y) * midpoint.location + quadPt[i].y
    //         };
    //         linPt.push(linMid);
    //         ctx.beginPath();
    //         ctx.arc(linMid.x, linMid.y, 5, 0, 2 * Math.PI, true);
    //         ctx.fill();
    //     }
    //
    //     var pt = [];
    //     ctx.fillStyle = '#F00';
    //     for (i=0; i+1 < linPt.length; i++) {
    //         var mid = {
    //             x: (linPt[i+1].x - linPt[i].x) * midpoint.location + linPt[i].x,
    //             y: (linPt[i+1].y - linPt[i].y) * midpoint.location + linPt[i].y
    //         };
    //         pt.push(mid);
    //         ctx.beginPath();
    //         ctx.arc(mid.x, mid.y, 5, 0, 2 * Math.PI, true);
    //         ctx.fill();
    //     }
    //
    //     var final_tangent = atan_bez(linPt);
    //
    //     console.group('linPt');
    //     console.log('linPt[0]: '+ linPt[0].x + ', ' + linPt[0].y);
    //     console.log('linPt[1]: '+ linPt[1].x + ', ' + linPt[1].y);
    //     console.groupEnd();
    //
    //     console.group('connector debug');
    //     console.log('midpoint location: '+ midpoint.location);
    //     console.log('midpoint coord: '+ midpoint.point.x + ', ' + midpoint.point.y);
    //     console.log('jsbez angle: '+ ( midpointAngle * 180/Math.PI) );
    //     console.log('calculated angle: ' + (final_tangent * 180/Math.PI) );
    //     console.groupEnd();
    //
    //     ctx.fillStyle = '#0f0';
    //     ctx.beginPath();
    //     ctx.arc(midpoint.point.x, midpoint.point.y, 3, 0, 2 * Math.PI, true);
    //     ctx.fill();
    // };

    contains (mx, my, pad = 5) {
        // Uses library jsBezier to accomplish certain tasks.
        // Since precise bezier distance is expensive to compute, we start by
        // running a faster algorithm to see if mx,my is outside the rectangle
        // given by the beginning, end, and control points (plus padding).

        this.calculateCurve();
        var ys = [ this.y, this.fromY, this.ctrl1.y, this.ctrl2.y ],
            xs = [ this.x, this.fromX, this.ctrl1.x, this.ctrl2.x ],
            bottom = Math.max.apply(null, ys),
            top = Math.min.apply(null, ys),
            right = Math.max.apply(null, xs),
            left = Math.min.apply(null, xs);

        if (mx > left - pad && mx < right + pad &&
            my > top - pad && my < bottom + pad) {
            // expensive route: run bezier distance algorithm
            return pad >
                Bezier.distanceFromCurve(
                    { x: mx, y: my },
                    this.calculateCurve()
                ).distance;
        }
        // mx,my is outside the rectangle, don't bother computing the bezier distance
        else return false;
    };

    spawnOutputNode(new_output_label: string) {
        var out_node = new OutputNode(this.x, this.y, new_output_label);
        this.dest = out_node.in_magnets[0];
        this.dest.cdt = this.source.cdt;
        this.dest.connected = [ this ];
        return out_node;
    }

    doDown(cs, e) {
        if (!e.shiftKey || cs.selection.length === 0) {
            cs.selection = [ this ];
            if (cs.can_edit) {
                cs.dragoffx = cs.dragoffy = 0;
            } else {
                cs.dragging = false;
                cs.selection = [];
            }
        }
    }

    isConnector() {
        return true;
    }

    deleteFrom = function(cs) {
        // remove selected Connector from list
        var index;

        // if a cable to an output node is severed, delete the node as well
        if (this.dest) {
            if (CanvasState.isOutputNode(this.dest.parent)) {
                index = cs.shapes.indexOf(this.dest.parent);
                if (index > -1) cs.shapes.splice(index, 1);
                index = cs.outputs.indexOf(this.dest.parent);
                if (index > -1) cs.outputs.splice(index, 1);
            } else {
                // remove connector from destination in-magnet
                index = this.dest.connected.indexOf(this);
                if (index > -1) this.dest.connected.splice(index, 1);
            }
        }

        // remove connector from source out-magnet
        index = this.source.connected.indexOf(this);
        if (index > -1) this.source.connected.splice(index, 1);

        // remove Connector from master list
        index = cs.connectors.indexOf(this);
        if (index > -1) cs.connectors.splice(index, 1);
    };
}

class NodeLabel {
    constructor(public label = '', public x = 0, public y = 0, public suffix = '') { }
}
class NodeUpdateSignal {
    x: number;
    y: number;
    r: number = 10;
    status: string;
    status_opts: INodeUpdateSignalMap = (function() {
        let pngprefix = "data:image/png;base64,";
        let imgs: {
            check?: HTMLImageElement;
            question?: HTMLImageElement;
            x?: HTMLImageElement;
            refresh?: HTMLImageElement;
        } = { };
        let icon64 = {
            /* tslint:disable:max-line-length */
            check: /*inline update-check:*/"iVBORw0KGgoAAAANSUhEUgAAABAAAAAQBAMAAADt3eJSAAAABGdBTUEAALGPC/xhBQAAAAFzUkdCAK7OHOkAAAAtUExURUxpcf///////////////////////////////////////////////////////3EAnbYAAAAOdFJOUwAQcO9QQCCfj8+/MN+vrAj6JgAAAGFJREFUCNdjYEAFTgIQmuVdAYSRDGOse5wAppnePYEI7Ht3gYFBvJCBYd4boCa9Vwwc754Chf3eKcS9awAyeN+ZnnsJUsho9+7da7CWuHfvFMAMjnevIIYwzjOD2iwCNh4AiZcdZAU+g5sAAAAASUVORK5CYII=",
            question: /*inline update-question:*/"iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAABGdBTUEAALGPC/xhBQAAAAFzUkdCAK7OHOkAAAAzUExURUxpcf////////////////////////////////////////////////////////////////Hv/K4AAAAQdFJOUwCfgFBgvyDvMECvzxCPcN9FpKciAAAAaElEQVQY01WO6xbAEAyDy1C1W97/addlzPH9IZFGRZxy7sAeD+kkBdH6acNAC43mtyTH5blAw5+Mk0CmoVCe2zAsMmD/CKn5ba1T8+c0A9FlKFO/hSYLtaw6qW6LoWOHwQ20tSK3vsMDBq4EkJmvtUUAAAAASUVORK5CYII=",
            x: /*inline update-x:*/"iVBORw0KGgoAAAANSUhEUgAAABAAAAAQBAMAAADt3eJSAAAABGdBTUEAALGPC/xhBQAAAAFzUkdCAK7OHOkAAAAbUExURUxpcf///////////////////////////////+WJFuQAAAAIdFJOUwCfEM+AcI+vhS7NcwAAAFlJREFUCNdjYIABJlcFIBmiwMDSYcTAwNghwMDa0azAINHhwAAkDBg7GhlAwo0SQBkGkFBHO1gfYwdIPYQBlgFJNUIEmiGKJTqMwNrhBsKtYEoDWRqoAHcFAAWbFYtiUTTsAAAAAElFTkSuQmCC",
            refresh: /*inline update-refresh:*/"iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAABGdBTUEAALGPC/xhBQAAAAFzUkdCAK7OHOkAAAAzUExURUxpcf////////////////////////////////////////////////////////////////Hv/K4AAAAQdFJOUwCfQL/fj1AQ73BgIDDPgK825B7lAAAAg0lEQVQY001PWxLEIAxCjY+obXP/0y6mjl0+nBARECBmEDMJAxvZNjJKIQ92IDaBm8PFi3JxaEBtZmE97BzsAZTCuhbxpkSB5OeLWoZHnDhEpTh+fLpdjfgrRNdilnJxC+b0t2frizOlVY8x6XEq/7MEsPZVXw1nrrpX7RQamkSSeuEf5C8HP4rTRNYAAAAASUVORK5CYII="
            /* tslint:enable:max-line-length */
        };

        for (let i in icon64) {
            imgs[i] = new Image();
            imgs[i].src = pngprefix + icon64[i];
        }

        return {
            "no update available": {
                color: "#666",
                icon: imgs.check
            },
            "updated": {
                color: "#0b0",
                icon: imgs.check
            },
            "updated with issues": {
                color: "#aa0",
                icon: imgs.question
            },
            "unavailable": {
                color: "#a22",
                icon: imgs.x
            },
            "update in progress": {
                color: "#aa0",
                icon: imgs.refresh
            }
        };
    }());

    constructor(public node: BaseNode, status) {
        this.setStatus(status);
        this.x = node.x + node.w / 2;
        this.y = node.y - node.h / 2;
    };
    draw (ctx: CanvasRenderingContext2D) {
        var canvas = new CanvasWrapper(undefined, ctx);
        var status_opt = this.status_opts[this.status];
        ctx.fillStyle = status_opt.color;
        canvas.drawCircle(this);
        ctx.drawImage(status_opt.icon, this.x - this.r + 2, this.y - this.r + 2);
    };
    setStatus (status: string) {
        if (this.status_opts.hasOwnProperty(status)) {
            this.status = status;
        } else {
            this.status = undefined;
        }
    };
}

export class OutputZone implements CanvasObject {
    dy = 0;
    dx = 0;
    x: number;
    y = 1;
    w: number;
    h: number;

    constructor(cw, ch, public inset = 15) {
        this.alignWithCanvas(cw, ch);
    }

    alignWithCanvas (cw, ch) {
        this.x = cw * 0.995;
        this.w = cw * 0.175;
        this.h = this.w;

        while (this.h + this.y > ch) {
            this.h /= 1.5;
        }
    }

    draw(ctx: CanvasRenderingContext2D) {
        // draw output zone
        ctx.strokeStyle = "#aaa";
        ctx.setLineDash([5]);
        ctx.lineWidth = 1;
        ctx.strokeRect(this.x - this.w, this.y, this.w, this.h);
        ctx.setLineDash([]);

        // draw label
        var canvas = new CanvasWrapper(undefined, ctx),
            textParams: TextParams = {
                x: this.x - this.w / 2,
                y: this.y + this.inset,
                dir: 0,
                style: "outputZone",
                text: "Drag here to"
            };
        canvas.drawText(textParams);
        textParams.text = "create an output";
        textParams.y += this.inset;
        canvas.drawText(textParams);
    }

    contains(mx, my): boolean {
        return (
            mx <= this.x &&
            mx >= this.x - this.w &&
            my >= this.y &&
            my <= this.y + this.h
        );
    }

    getVertices() {
        var x, y,
            spacing = 25,
            vertices = [];
        for (var i = 0; !i || y < this.y + this.h; i += spacing) {
            x = this.x - (i % this.w);
            y = (i / this.w >> 0) * spacing + this.y;
            vertices.push({x, y});
        }
        y = this.y + this.h;
        for (x = this.x, i = this.x - this.w; x > i; x -= spacing) {
            vertices.push({x, y});
        }
        x = i;
        for (y = this.y, i = this.y + this.h; y < i; y += spacing) {
            vertices.push({x, y});
        }
        return vertices;
    }

    doDown() {
        // does nothing
    }
    deleteFrom() {
        // can't delete this object
    }
    isOutputZone() {
        return true;
    }
}

export class OutputNode extends CylinderNode implements CNode {
    /*
     CNode representing an output.
     */
    fill = "#d40";
    defaultFill = "#d40";
    found_fill = "blue";
    inset = 12; // distance of magnet from center
    in_magnets: Magnet[];
    run_id: number;
    // Marks whether or not this node
    // was being searched for and was found
    // (when doing an md5 lookup)
    found_md5 = false;

    constructor(x, y, label, public pk?, public status?: string, public md5?, public dataset_id?) {
        super(x, y, label);
        this.in_magnets = [
            new Magnet(this, 5, 2, "white", null, label, undefined, undefined, pk)
        ];
    }

    draw(ctx: CanvasRenderingContext2D) {
        this.fill = this.defaultFill;
        super.draw(ctx);
    }
    highlight(ctx: CanvasRenderingContext2D) {
        super.highlight(ctx);
    }
    deleteFrom(cs: CanvasState): void {
        // deleting an output node is the same as deleting the cable
        var connected_cable = this.in_magnets[0].connected;
        if (connected_cable.length > 0) {
            connected_cable[0].deleteFrom(cs);
        } else {
            removeFromArray(cs.shapes, this);
            removeFromArray(cs.outputs, this);
        }
    }
    setLabel(label: string): void {
        this.label = label;
        this.in_magnets[0].label = label;
    }
    isOutputNode() {
        return true;
    }
}