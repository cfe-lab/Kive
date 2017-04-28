/**
 * Written by James Nakagawa
 * github.com/jamesnakagawa
 */
import CustomMatcherFactories = jasmine.CustomMatcherFactories;
type ImageType = HTMLImageElement | HTMLCanvasElement | CanvasRenderingContext2D | ImageData;

declare namespace jasmine {
    interface Matchers<T> {
        toImageDiffEqual(compareImg: ImageType, tolerance?: number): boolean;
        toBeImageData(): boolean;
    }
}

declare interface imagediff {

    createCanvas(width: number, height: number): HTMLCanvasElement;
    createImageData(width: number, height: number): ImageData;

    isImage(obj: any): obj is HTMLImageElement;
    isCanvas(obj: any): obj is HTMLCanvasElement;
    isContext(obj: any): obj is CanvasRenderingContext2D;
    isImageData(obj: any): obj is ImageData;
    isImageType(obj: any): obj is ImageType;

    toImageData(object: any): ImageData;

    equal(a: ImageType, b: ImageType, tolerance?: number): boolean;
    diff(a: ImageType, b: ImageType, options?: { align?: string }): ImageData;

    jasmine: CustomMatcherFactories;

    // Compatibility
    noConflict(): imagediff;

    imageDataToPNG(imageData: ImageData, outputFile: string, callback: Function): void;
}

declare var imagediff: imagediff;