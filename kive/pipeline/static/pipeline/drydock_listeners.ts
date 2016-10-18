import { CanvasState } from "./drydock";
import {CanvasContextMenu} from "./drydock_contextmenu";

export class CanvasListeners {
    static initMouseListeners(cs: CanvasState): void {
        cs.canvas.addEventListener('selectstart', e => { e.preventDefault(); return false; }, false);
        cs.canvas.addEventListener('mousedown',   e => { cs.doDown(e); }, true);
        cs.canvas.addEventListener('mousemove',   e => { cs.doMove(e); }, true);
        cs.canvas.addEventListener('mouseup',     e => { cs.doUp(e); },   true);
    }
    static initKeyListeners(cs: CanvasState): void {
        document.addEventListener('keydown', function(e: KeyboardEvent) {
            let backspace = e.which === 8;
            let del = e.which === 46;
            let esc = e.which === 27;
        
            // backspace or delete key also removes selected object
            if (backspace || del) {
                // prevent backspace from triggering browser to navigate back one page
                e.preventDefault();
                if (cs.selection && cs.can_edit) {
                    cs.deleteObject();
                }
            } else if (esc) {
                cs.selection = [];
                cs.valid = false;
            }
        });
    }
    static initResizeListeners(cs: CanvasState): void {
        let resize_timeout = 0;
        let canvas = cs.canvas;
        
        function endDocumentResize() {
            cs.valid = false;
            cs.outputZone.alignWithCanvas(canvas.width, canvas.height);
            cs.detectAllCollisions();
        }
            
        window.addEventListener("resize", function() {
            cs.width  = canvas.width  = window.innerWidth;
            cs.height = canvas.height = window.innerHeight - $(canvas).offset().top - 5;
    
            let scale_x = canvas.width  / cs.old_width;
            let scale_y = canvas.height / cs.old_height;
    
            if (scale_x === 1 && scale_y === 1) {
                return;
            }
            if (scale_x !== 1) {
                for (let shape of cs.shapes) {
                    shape.x  *= scale_x;
                    shape.dx *= scale_x;
                }
            }
            if (scale_y !== 1) {
                for (let shape of cs.shapes) {
                    shape.y  *= scale_y;
                    shape.dy *= scale_y;
                }
            }
    
            cs.old_width = canvas.width;
            cs.old_height = canvas.height;
            cs.valid = false;
    
            // Collision detection is computationally expensive, so
            // deferred until 0.5s have passed without further resizing.
            clearTimeout(resize_timeout);
            resize_timeout = setTimeout(endDocumentResize, 500);
        });
    }
    static initContextMenuListener(cs: CanvasState, contextMenu: CanvasContextMenu): void {
        cs.canvas.addEventListener('contextmenu', e => contextMenu.open(e), true);
    }
}