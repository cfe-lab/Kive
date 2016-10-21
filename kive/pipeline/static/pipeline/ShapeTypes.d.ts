export interface Point {
    x: number;
    y: number;
}
export interface Circle extends Point {
    r: number;
}
export interface Ellipse extends Point {
    rx: number;
    ry: number;
}
export interface Rectangle extends Point {
    height: number;
    width: number;
    r?: number;
    stroke?: boolean;
}
export interface TextParams extends Point {
    dir: number;
    style: string;
    text: string;
}