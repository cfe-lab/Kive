/**
 * jsBezier-0.6
 *
 * Copyright (c) 2010 - 2013 Simon Porritt (simon.porritt@gmail.com)
 *
 * licensed under the MIT license.
 *
 * a set of Bezier curve functions that deal with Beziers, used by jsPlumb, and perhaps useful for other people.
 * These functions work with Bezier curves of arbitrary degree.
 *
 * - functions are all in the 'jsBezier' namespace.
 *
 * - all input points should be in the format {x:.., y:..}. all output points are in this format too.
 *
 * - all input curves should be in the format [ {x:.., y:..}, {x:.., y:..}, {x:.., y:..}, {x:.., y:..} ]
 *
 * - 'location' as used as an input here refers to a decimal in the range 0-1 inclusive, which indicates a point
 * some proportion along the length of the curve.  location as output has the same format and meaning.
 *
 *
 * Function List:
 * --------------
 *
 * distanceFromCurve(point, curve)
 *
 *     Calculates the distance that the given point lies from the given Bezier.  Note that it is computed relative to
 * the center of the Bezier, so if you have stroked the curve with a wide pen you may wish to take that into account!
 * The distance returned is relative to the values of the curve and the point - it will most likely be pixels.
 *
 * gradientAtPoint(curve, location)
 *
 *     Calculates the gradient to the curve at the given location, as a decimal between 0 and 1 inclusive.
 *
 * gradientAtPointAlongCurveFrom (curve, location)
 *
 *    Calculates the gradient at the point on the given curve that is 'distance' units from location.
 *
 * nearestPointOnCurve(point, curve)
 *
 *    Calculates the nearest point to the given point on the given curve.  The return value of this is a JS object
 * literal, containing both the point's coordinates and also the 'location' of the point (see above), for example:
 * { point:{x:551,y:150}, location:0.263365 }.
 *
 * pointOnCurve(curve, location)
 *
 *     Calculates the coordinates of the point on the given Bezier curve at the given location.
 *
 * pointAlongCurveFrom(curve, location, distance)
 *
 *     Calculates the coordinates of the point on the given curve that is 'distance' units from location.  'distance'
 * should be in the same coordinate space as that used to construct the Bezier curve.  For an HTML Canvas usage, for
 * example, distance would be a measure of pixels.
 *
 * locationAlongCurveFrom(curve, location, distance)
 *
 *     Calculates the location on the given curve that is 'distance' units from location.  'distance' should be in the
 * same coordinate space as that used to construct the Bezier curve.  For an HTML Canvas usage, for example, distance
 * would be a measure of pixels.
 *
 * perpendicularToCurveAt(curve, location, length, distance)
 *
 *     Calculates the perpendicular to the given curve at the given location.  length is the length of the line you wish
 * for (it will be centered on the point at 'location'). distance is optional, and allows you to specify a point along
 * the path from the given location as the center of the perpendicular returned.  The return value of this is an array
 * of two points: [ {x:...,y:...}, {x:...,y:...} ].
 *
 */

import { Point } from "./ShapeTypes";

var mathSign = x => x === 0 ? 0 : x > 0 ? 1 : -1;

var Vectors = {
        subtract   : (v1, v2) => ({x: v1.x - v2.x, y: v1.y - v2.y }),
        dotProduct : (v1, v2) => (v1.x * v2.x) + (v1.y * v2.y),
        square     : (v) => Math.sqrt((v.x * v.x) + (v.y * v.y)),
        scale      : (v, s) => ({x: v.x * s, y: v.y * s })
    },
    maxRecursion = 64,
    flatnessTolerance = Math.pow(2.0, -maxRecursion - 1);

/**
 * Calculates the distance that the point lies from the curve.
 *
 * @param point a point in the form {x:567, y:3342}
 * @param curve a Bezier curve in the form [{x:..., y:...}, {x:..., y:...}, {x:..., y:...}, {x:..., y:...}].  note that
 * this is currently hardcoded to assume cubiz beziers, but would be better off supporting any degree.
 * @return a JS object literal containing location and distance, for example: {location:0.35, distance:10}.  Location is
 * analogous to the location argument you pass to the pointOnPath function: it is a ratio of distance travelled along
 * the curve.  Distance is the distance in pixels from the point to the curve.
 */
function _distanceFromCurve (point: Point, curve: Point[]): { location: number, distance: number } {
    var candidates = [],
        w = _convertToBezier(point, curve),
        degree = curve.length - 1, higherDegree = (2 * degree) - 1,
        numSolutions = _findRoots(w, higherDegree, candidates, 0),
        v = Vectors.subtract(point, curve[0]), dist = Vectors.square(v), t = 0.0,
        newDist;

    for (var i = 0; i < numSolutions; i++) {
        v = Vectors.subtract(point, _bezier(curve, degree, candidates[i], null, null));
        newDist = Vectors.square(v);
        if (newDist < dist) {
            dist = newDist;
            t = candidates[i];
        }
    }
    v = Vectors.subtract(point, curve[degree]);
    newDist = Vectors.square(v);
    if (newDist < dist) {
        dist = newDist;
        t = 1.0;
    }
    return {location: t, distance: dist};
}
/**
 * finds the nearest point on the curve to the given point.
 */
function _nearestPointOnCurve (point: Point, curve: Point[]): { point: Point, location: number } {
    var td = _distanceFromCurve(point, curve);
    return {point: _bezier(curve, curve.length - 1, td.location, null, null), location: td.location};
}
function _convertToBezier (point: Point, curve: Point[]): Point[] {
    var degree = curve.length - 1, higherDegree = (2 * degree) - 1,
        c = [], d = [], cdTable = [], w = [],
        z = [ [1.0, 0.6, 0.3, 0.1], [0.4, 0.6, 0.6, 0.4], [0.1, 0.3, 0.6, 1.0] ];

    for (var i = 0; i <= degree; i++) c[i] = Vectors.subtract(curve[i], point);
    for (i = 0; i <= degree - 1; i++) {
        d[i] = Vectors.subtract(curve[i + 1], curve[i]);
        d[i] = Vectors.scale(d[i], 3.0);
    }
    for (var row = 0; row <= degree - 1; row++) {
        for (var column = 0; column <= degree; column++) {
            if (!cdTable[row]) cdTable[row] = [];
            cdTable[row][column] = Vectors.dotProduct(d[row], c[column]);
        }
    }
    for (i = 0; i <= higherDegree; i++) {
        if (!w[i]) w[i] = [];
        w[i].y = 0.0;
        w[i].x = i / higherDegree;
    }
    var n = degree, m = degree - 1;
    for (var k = 0; k <= n + m; k++) {
        var lb = Math.max(0, k - m),
            ub = Math.min(k, n);
        for (i = lb; i <= ub; i++) {
            var j = k - i;
            w[i + j].y += cdTable[j][i] * z[j][i];
        }
    }
    return w;
}
/**
 * counts how many roots there are.
 */
function _findRoots (w: Point[], degree: number, t: number[], depth: number): number {
    var left: Point[] = [], right: Point[] = [],
        left_count: number, right_count: number,
        left_t: number[] = [], right_t: number[] = [];

    switch (_getCrossingCount(w, degree)) {
           case 0 : {
               return 0;
           }
           case 1 : {
               if (depth >= maxRecursion) {
                   t[0] = (w[0].x + w[degree].x) / 2.0;
                   return 1;
               }
               if (_isFlatEnough(w, degree)) {
                   t[0] = _computeXIntercept(w, degree);
                   return 1;
               }
               break;
           }
    }
    _bezier(w, degree, 0.5, left, right);
    left_count  = _findRoots(left,  degree, left_t, depth + 1);
    right_count = _findRoots(right, degree, right_t, depth + 1);
    for (var i = 0; i < left_count; i++) t[i] = left_t[i];
    for (i = 0; i < right_count; i++) t[i + left_count] = right_t[i];
    return (left_count + right_count);
}
function _getCrossingCount (curve: Point[], degree: number): number {
    var n_crossings = 0, sign, old_sign;
    sign = old_sign = mathSign(curve[0].y);
    for (var i = 1; i <= degree; i++) {
        sign = mathSign(curve[i].y);
        if (sign !== old_sign) n_crossings++;
        old_sign = sign;
    }
    return n_crossings;
}
function _isFlatEnough (curve: Point[], degree: number): boolean {
    var error,
        intercept_1, intercept_2, left_intercept, right_intercept,
        a, b, c, det, dInv, a1, b1, c1, a2, b2, c2;
    a = curve[0].y - curve[degree].y;
    b = curve[degree].x - curve[0].x;
    c = curve[0].x * curve[degree].y - curve[degree].x * curve[0].y;

    var max_distance_above = 0.0,
        max_distance_below = 0.0;

    for (var i = 1; i < degree; i++) {
        var value = a * curve[i].x + b * curve[i].y + c;
        if (value > max_distance_above)
            max_distance_above = value;
        else if (value < max_distance_below)
            max_distance_below = value;
    }

    a1 = 0.0; b1 = 1.0; c1 = 0.0; a2 = a; b2 = b;
    c2 = c - max_distance_above;
    det = a1 * b2 - a2 * b1;
    dInv = 1.0 / det;
    intercept_1 = (b1 * c2 - b2 * c1) * dInv;
    a2 = a; b2 = b; c2 = c - max_distance_below;
    det = a1 * b2 - a2 * b1;
    dInv = 1.0 / det;
    intercept_2 = (b1 * c2 - b2 * c1) * dInv;
    left_intercept = Math.min(intercept_1, intercept_2);
    right_intercept = Math.max(intercept_1, intercept_2);
    error = right_intercept - left_intercept;
    return (error < flatnessTolerance);
}
function _computeXIntercept (curve: Point[], degree: number): number {
    var XLK = 1.0, YLK = 0.0,
        XNM = curve[degree].x - curve[0].x, YNM = curve[degree].y - curve[0].y,
        XMK = curve[0].x, YMK = curve[0].y,
        det = XNM * YLK - YNM * XLK, detInv = 1.0 / det,
        S = (XNM * YMK - YNM * XMK) * detInv;
    return XLK * S;
}
function _bezier (curve: Point[], degree: number, t: number, left?: Point[], right?: Point[]) {
    var temp = [[]];
    for (var j = 0; j <= degree; j++) temp[0][j] = curve[j];
    for (var i = 1; i <= degree; i++) {
        for (j = 0 ; j <= degree - i; j++) {
            if (!temp[i]) temp[i] = [];
            if (!temp[i][j]) temp[i][j] = {};
            temp[i][j].x = (1.0 - t) * temp[i - 1][j].x + t * temp[i - 1][j + 1].x;
            temp[i][j].y = (1.0 - t) * temp[i - 1][j].y + t * temp[i - 1][j + 1].y;
        }
    }
    if (left)
        for (j = 0; j <= degree; j++) left[j]  = temp[j][0];
    if (right)
        for (j = 0; j <= degree; j++) right[j] = temp[degree - j][j];

    return (temp[degree][0]);
}

var _getCurveFunctions: (order: number) => Function[] = (function() {
    var cache = {};
    var f_term = c  => t  => Math.pow(t, c),
        l_term = c  => t  => Math.pow((1 - t), c),
        c_term = c  => () => c,
        t_term = () => t  => t,
        one_minus_t_term = () => t => 1 - t,
        _termFunc = terms =>
            t => terms.reduce( (prev, curr) => prev *= curr(t), 1)
        ;
    return function(order: number) {
        if (cache[order]) {
            return cache[order];
        }

        var fns = [];
        fns.push(f_term(order));  // first is t to the power of the curve order
        for (let i = 1; i < order; i++) {
            let terms: Function[] = [ c_term(order) ];
            for (let j = 0 ; j < (order - i); j++) terms.push(t_term());
            for (let j = 0 ; j < i; j++) terms.push(one_minus_t_term());
            fns.push(_termFunc(terms));
        }
        fns.push(l_term(order));  // last is (1-t) to the power of the curve order

        cache[order] = fns;
        return fns;
    };
})();


/**
 * calculates a point on the curve, for a Bezier of arbitrary order.
 * @param curve an array of control points, eg [{x:10,y:20}, {x:50,y:50}, {x:100,y:100}, {x:120,y:100}].
 * For a cubic bezier this should have four points.
 * @param location a decimal indicating the distance along the curve the point should be located at.
 * this is the distance along the curve as it travels, taking the way it bends into account.
 * should be a number from 0 to 1, inclusive.
 */
function _pointOnPath (curve: Point[], location: number): Point {
    var cc = _getCurveFunctions(curve.length - 1),
        _x = 0, _y = 0;
    for (var i = 0; i < curve.length ; i++) {
        _x += curve[i].x * cc[i](location);
        _y += curve[i].y * cc[i](location);
    }

    return {x: _x, y: _y};
}

function _dist (p1: Point, p2: Point): number {
    return Math.sqrt(Math.pow(p1.x - p2.x, 2) + Math.pow(p1.y - p2.y, 2));
}
function _isPoint (curve: Point[]): boolean {
    return curve[0].x === curve[1].x && curve[0].y === curve[1].y;
}

/**
 * finds the point that is 'distance' along the path from 'location'.  this method returns both
 * the x,y location of the point and also
 * its 'location' (proportion of travel along the path); the method below - _pointAlongPathFrom -
 * calls this method and just returns the point.
 */
function _pointAlongPath (curve: Point[], location: number, distance: number): {point: Point, location: number} {
    if (_isPoint(curve)) {
        return {
            point: curve[0],
            location
        };
    }

    var prev = _pointOnPath(curve, location),
        tally = 0,
        curLoc = location,
        direction = distance > 0 ? 1 : -1,
        cur = null;

    while (tally < Math.abs(distance)) {
        curLoc += (0.005 * direction);
        cur = _pointOnPath(curve, curLoc);
        tally += _dist(cur, prev);
        prev = cur;
    }
    return {point: cur, location: curLoc};
}

function _length (curve: Point[]): number {
    if (_isPoint(curve)) return 0;

    var prev = _pointOnPath(curve, 0),
        tally = 0,
        curLoc = 0,
        direction = 1,
        cur = null;

    while (curLoc < 1) {
        curLoc += (0.005 * direction);
        cur = _pointOnPath(curve, curLoc);
        tally += _dist(cur, prev);
        prev = cur;
    }
    return tally;
}

/**
 * finds the point that is 'distance' along the path from 'location'.
 */
function _pointAlongPathFrom (curve: Point[], location: number, distance: number): Point {
    return _pointAlongPath(curve, location, distance).point;
}

/**
 * finds the location that is 'distance' along the path from 'location'.
 */
function _locationAlongPathFrom (curve: Point[], location: number, distance: number): number {
    return _pointAlongPath(curve, location, distance).location;
}

/**
 * returns the gradient of the curve at the given location, which is a decimal between 0 and 1 inclusive.
 * thanks // http://bimixual.org/AnimationLibrary/beziertangents.html
 *
 * 1: this function was using pointOnPath() instead of bezier() to compute the point.
 * this was a bug and did not return the correct angle.
 * 2: it was also using the full cubic to compute p1 and a quadratic to compute p2,
 * but it's faster to compute the quadratic for both.
 * 3: it was returning Infinity instead of Math.atan(Infinity) ( == pi/2).
 * -jtn,2014-9-4
 */
function _gradientAtPoint (curve: Point[], location: number): number {
    var p1 = _bezier(curve.slice(1, curve.length),     curve.length - 2, location),
        p2 = _bezier(curve.slice(0, curve.length - 1), curve.length - 2, location),
        dy = p2.y - p1.y, dx = p2.x - p1.x;
    return dx ? Math.atan(dy / dx) : Math.PI / 2;
}

/**
returns the gradient of the curve at the point which is 'distance' from the given location.
if this point is greater than location 1, the gradient at location 1 is returned.
if this point is less than location 0, the gradient at location 0 is returned.
*/
function _gradientAtPointAlongPathFrom(curve: Point[], location: number, distance: number): number {
    var p = _pointAlongPath(curve, location, distance);
    if (p.location > 1) p.location = 1;
    if (p.location < 0) p.location = 0;
    return _gradientAtPoint(curve, p.location);
}

/**
 * calculates a line that is 'length' pixels long, perpendicular to, and centered on, the
 * path at 'distance' pixels from the given location. if distance is not supplied, the perpendicular
 * for the given location is computed (ie. we set distance to zero).
 */
function _perpendicularToPathAt (curve: Point[], location: number, length: number, distance: number): Point[] {
    distance = distance ? distance : 0;
    var p = _pointAlongPath(curve, location, distance),
        m = _gradientAtPoint(curve, p.location),
        _theta2 = Math.atan(-1 / m),
        y =  length / 2 * Math.sin(_theta2),
        x =  length / 2 * Math.cos(_theta2);
    return [{x: p.point.x + x, y: p.point.y + y}, {x: p.point.x - x, y: p.point.y - y}];
}

export var Bezier = {
    distanceFromCurve : _distanceFromCurve,
    gradientAtPoint : _gradientAtPoint,
    gradientAtPointAlongCurveFrom : _gradientAtPointAlongPathFrom,
    nearestPointOnCurve : _nearestPointOnCurve,
    pointOnCurve : _pointOnPath,
    pointAlongCurveFrom : _pointAlongPathFrom,
    perpendicularToCurveAt : _perpendicularToPathAt,
    locationAlongCurveFrom: _locationAlongPathFrom,
    getLength: _length
};
