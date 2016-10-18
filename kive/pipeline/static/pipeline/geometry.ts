import { Point, Rectangle, Ellipse } from "./ShapeTypes";
export { Point, Rectangle, Ellipse } from "./ShapeTypes";
export class Geometry {

    static inEllipse(m:Point, ellipse: Ellipse): boolean;
    static inEllipse(mx:number, my:number, ellipse: Ellipse): boolean;
    static inEllipse(mx:number, my:number, cx:number, cy:number, rx:number, ry:number): boolean;
    static inEllipse(mx, my, cx?, cy?, rx?, ry?): boolean {
        if (typeof mx == "object" && typeof my == "object") {
            return Geometry.inEllipse(mx.x, mx.y, my.x, my.y, my.rx, my.ry);
        } else if (typeof mx == "number" && typeof my == "number" && typeof cx == "object") {
            return Geometry.inEllipse(mx, my, cx.x, cx.y, cx.rx, cx.ry);
        }
        var dx = mx - cx,
            dy = my - cy;
        return (dx*dx) / (rx*rx) + (dy*dy) / (ry*ry) <= 1;
    }

    static inRectangle(m: Point, rect: Rectangle): boolean;
    static inRectangle(mx:number, my:number, rect: Rectangle): boolean;
    static inRectangle(mx:number, my:number, x:number, y:number, w:number, h:number): boolean;
    static inRectangle(mx, my, x?, y?, w?, h?): boolean {
        if (typeof mx == "object" && typeof my == "object") {
            return Geometry.inRectangle(mx.x, mx.y, my.x, my.y, my.width, my.height);
        } else if (typeof mx == "number" && typeof my == "number" && typeof x == "object") {
            return Geometry.inRectangle(mx, my, x.x, x.y, x.width, x.height);
        }
        return mx > x && mx < x + w &&
            my > y && my < y + h;
    }

    static inRectangleFromCentre(mx:number, my:number, cx:number, cy:number, w2:number, h2:number): boolean {
        return Math.abs(mx - cx) < w2 && Math.abs(my - cy) < h2;
    };
    static inCircle(mx:number, my:number, cx:number, cy:number, r:number): boolean {
        var dx = cx - mx,
            dy = cy - my;
        return Math.sqrt(dx*dx + dy*dy) <= r;
    };
    static ltLine(mx:number, my:number, x1:number, y1:number, x2:number, y2:number): boolean {
        return x2 != x1 ? (y2 - y1) / (x2 - x1) * (mx - x1) > my - y1 : null;
    };
    static inPolygon(mx:number, my:number, shape: Point[]): boolean {
        // ray casting algorithm
        // argument 'shape' is an array of objects each with properties x and y
        var o = [ -300, -300 ],
            line, s1_x, s1_y, s, t,
            s2_x = mx - o[0],
            s2_y = my - o[1],
            intersections = 0;

        for (let j = 0; j < shape.length; j++) {
            line = shape.slice(j, j+2);
            if (line.length == 1) line.push(shape[0]);

            s1_x = line[1].x - line[0].x;
            s1_y = line[1].y - line[0].y;
            s = (
                    -s1_y * (line[0].x - o[0]) +
                    s1_x * (line[0].y - o[1])
                ) /
                (-s2_x * s1_y + s1_x * s2_y);
            t = (
                    s2_x * (line[0].y - o[1]) -
                    s2_y * (line[0].x - o[0])
                ) /
                (-s2_x * s1_y + s1_x * s2_y);

            if (s >= 0 && s <= 1 &&
                t >= 0 && t <= 1)
                intersections++;
        }

        return intersections % 2 != 0;
    }

    /**
     * Calculate the average x and y coordinates from an array of node objects.
     *
     * @param points: an array of objects that all have x and y attributes.
     * @return an object with x and y attributes for the average values
     */
    static averagePoint(points: Point[]): Point {
        var sum = points.reduce(
            (a, b) => ({
                x: a.x + b.x,
                y: a.y + b.y
            }),
            { x: 0, y: 0 }
        );
        return {
            x: sum.x / points.length,
            y: sum.y / points.length
        };
    }

    /*
     * Isometric projection calculations
     */
    static isometricXCoord(x:number,y:number): number {
        // isometric x-coordinate is explained in issue #277.
        // using a -30° line that intersects (0,0) and a 30° line that intersects (x,y), find the intersection of the two.
        // then compute the distance from this intersection to (x,y). tan(pi/6) = 1/sqrt(3) ~ 0.577350269
        return x * 0.577350269 - y;
    }
    static isometricYCoord(x:number,y:number): number {
        // isometric y-coordinate is explained in issue #277.
        // using a 30° line that intersects (0,0) and a -30° line that intersects (x,y), find the intersection of the two.
        // then compute the distance from this intersection to (x,y). tan(pi/6) = 1/sqrt(3) ~ 0.577350269
        return x * 0.577350269 + y;

        /*
         * unabridged version:

         var tan30 = Math.tan(Math.PI/6),
         // (x0, y0) is the coordinate of the intersection
         x0 = (x * tan30 - y) / (2 * tan30),
         y0 = - x0 * tan30,
         dx = x - x0,
         dy = y - y0,
         // dh is the distance from (x0,y0) to (x,y). it is a 30° line.
         dh = Math.sqrt(dx*dx + dy*dy);
         return dh;

         */
    }
    static TwoDimensionToIsometric(x: number, y: number): Point {
        return {
            x: Geometry.isometricXCoord(x,y),
            y: Geometry.isometricYCoord(x,y)
        };
    }
    static isoTo2D(iso_x:number, iso_y:number): Point {
        // inverse of [ isometricXCoord, isometricYCoord ]
        return {
            x: (iso_y + iso_x) / 1.154700538,
            y: (iso_y - iso_x) / 2
        };
    };

    static isometricSort(x1:Point, y1:Point): number;
    static isometricSort(x1:number,y1:number,x2:number,y2:number): number;
    static isometricSort(x1, y1, x2?, y2?): number {
        // returns 1 if the first set of coordinates is after the second,
        // -1 if the reverse is true, 0 if it's a tie. order goes left-to-right,
        // top-to-bottom if you sort of rotate your screen 30° clockwise and get
        // in the isometric plane.
        // includes ±7 pixels of fuzziness in the top-to-bottom decision.
        const fuzz = 7;

        if (typeof x1 == "object" && typeof y1 == "object"
                && [ x1.x, x1.y, y1.x, y1.y ].indexOf(undefined) === -1) {
            // transform alternative syntax
            return Geometry.isometricSort(x1.x, x1.y, y1.x, y1.y);
        }

        var y_diff = (x1 - x2) * 0.577350269 + y1 - y2; // tan(pi/6) = 1/sqrt(3) ~ 0.577350269
        if (y_diff > fuzz) {
            return 1;
        } else if (y_diff < -fuzz) {
            return -1;
        } else {
            var x_diff = y_diff + (y2 - y1) * 2;
            if (x_diff > 0) {
                return 1;
            } else if (x_diff < 0) {
                return -1;
            } else {
                return 0;
            }
        }
    }
}