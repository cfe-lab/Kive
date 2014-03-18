
/*
    TODO: if shape (CDT_Node, MethodNode) or Connector selected and user hits backspace, erase object
    TODO: make Connectors selectable by mouse
    TODO: Connector can link to output end-zone
    TODO: submit shape and connector info as POST data
    FIXME: figure out why extra lines are being drawn (non-closed path)?
*/

$(document).ready(function(){ // wait for page to finish loading before executing jQuery code

    // trigger ajax on CR drop-down to populate revision select
    $(document).ajaxSend(function(event, xhr, settings) {
        /*
            from https://docs.djangoproject.com/en/1.3/ref/contrib/csrf/#csrf-ajax
            On each XMLHttpRequest, set a custom X-CSRFToken header to the value of the CSRF token.
            ajaxSend is a function to be executed before an Ajax request is sent.
        */
        //console.log('ajaxSend triggered');

        function getCookie(name) {
            var cookieValue = null;
            if (document.cookie && document.cookie != '') {
                var cookies = document.cookie.split(';');
                for (var i = 0; i < cookies.length; i++) {
                    var cookie = jQuery.trim(cookies[i]);
                    // Does this cookie string begin with the name we want?
                    if (cookie.substring(0, name.length + 1) == (name + '=')) {
                        cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                        break;
                    }
                }
            }
            return cookieValue;
        }
        function sameOrigin(url) {
            // url could be relative or scheme relative or absolute
            var host = document.location.host; // host + port
            var protocol = document.location.protocol;
            var sr_origin = '//' + host;
            var origin = protocol + sr_origin;
            // Allow absolute or scheme relative URLs to same origin
            return (url == origin || url.slice(0, origin.length + 1) == origin + '/') ||
                (url == sr_origin || url.slice(0, sr_origin.length + 1) == sr_origin + '/') ||
                // or any other URL that isn't scheme relative or absolute i.e relative.
                !(/^(\/\/|http:|https:).*/.test(url));
        }
        function safeMethod(method) {
            return (/^(GET|HEAD|OPTIONS|TRACE)$/.test(method));
        }

        if (!safeMethod(settings.type) && sameOrigin(settings.url)) {
            xhr.setRequestHeader("X-CSRFToken", getCookie('csrftoken'));
        }
    });

    // update method drop-down
    $("[id^='id_select_method_family']").on('change',
        function() {
            mf_id = $(this).val();
            if (mf_id != "") {
                $.ajax({
                    type: "POST",
                    url: "get_method_revisions/",
                    data: {mf_id: mf_id}, // specify data as an object
                    datatype: "json", // type of data expected back from server
                    success: function(result) {
                        var options = [];
                        var arr = JSON.parse(result)
                        $.each(arr, function(index,value) {
                            options.push('<option value="', value.pk, '">', value.fields.revision_name, '</option>');
                        });
                        $("#id_select_method").html(options.join(''));
                    }
                })
            }
            else {
                $("#id_select_method").html('<option value="">--- select Method Family first ---</option>');
            }
        }
    ).change() // trigger on load

    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        var $this = $(this);
        $this.wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
    });

    $('a[rel="ctrl"]').on('click', function (e) {
        $(this).siblings('.fulltext').show().css({ top: e.pageY, left: e.pageX });
        setTimeout("$('.fulltext').fadeOut(300);", 2000);
    });


    // initialize animated canvas
    var canvas = document.getElementById('pipeline_canvas');
    var canvasWidth = 800;
    var canvasHeight= 400;
    canvas.width = canvasWidth;
    canvas.height = canvasHeight;

    var canvasState = new CanvasState(canvas);

    $('#id_cdt_button').on('click', function() {
        var choice = $('#id_select_cdt option:selected');
        var cdt = choice.val(); // primary key
        var node_label = choice.text();
        canvasState.addShape(new CDT_Node(x = 100,
            y = 200+50*Math.random(),
            r = 20,
            fill = '#33FF33',
            inset = 10,
            offset = 30,
            pk = cdt,
            label = node_label));
    });

    $('#id_method_button').on('click', function() {
        var selected = $('#id_select_method option:selected');
        if (selected.val() != "") {
            var mid = selected.val();
            var n_inputs = null;
            var n_outputs = null;
            var node_label = selected.text();
            if (mid != '') {
                $.ajax({
                    type: "POST",
                    url: "get_method_io/",
                    data: {mid: mid}, // specify data as an object
                    datatype: "json", // type of data expected back from server
                    success: function(result) {
                        inputs = result['inputs'];
                        outputs = result['outputs'];
                        canvasState.addShape(new MethodNode(200, 200 + 50*Math.random(), 80, 10, 20, '#999999', node_label, 14, inputs, outputs));
                    }
                });
            }
        }
    });

    $('#id_reset_button').on('click', function() {
        // remove all objects from canvas
        canvasState.clear();
        canvasState.shapes = [];
        canvasState.connectors = [];
    });

    $('id_delete_button').on('click', function() {
        // remove selected object from canvas
        // TODO: trigger this event with backspace key also
        canvasState.deleteObject();
    });
});


/*
 The following code is based on canvas interactivity example by Simon Sarris.
 HTML5 Unleashed (2014) Pearson Education Inc.
 */
function CDT_Node (x, y, r, fill, inset, offset, pk, label) {
    // CDT node constructor with default values
    this.x = x || 0; // defaults to top left corner
    this.y = y || 0;
    this.r = r || 10; // radius
    this.fill = fill || "#AAAAAA";
    this.inset = inset || 5; // distance of magnet from center
    this.offset = offset || 12; // distance of label from center
    this.pk = pk;
    this.label = label || '';
    this.in_magnets = []; // for compatibility

    // CDT node always has one magnet
    var magnet = new Magnet(this.x + this.inset, this.y, 5, 2, "white", this.pk, this.label);
    this.out_magnets = [magnet];
}

CDT_Node.prototype.draw = function(ctx) {
    // draw circle
    ctx.fillStyle = this.fill;
    ctx.beginPath();
    ctx.arc(this.x, this.y, this.r, 0, 2 * Math.PI, false);
    ctx.closePath();
    ctx.fill();

    // draw label
    ctx.fillStyle = 'black';
    ctx.textAlign = 'center';
    ctx.font = '12pt Lato, sans-serif';
    ctx.fillText(this.label, this.x, this.y+this.offset);

    // draw magnet
    out_magnet = this.out_magnets[0];
    out_magnet.draw(ctx, this.x+this.inset, this.y);
};

CDT_Node.prototype.contains = function(mx, my) {
    // determine if mouse pointer coordinates (mx, my) are
    // within this shape's bounds - compare length of hypotenuse
    // to radius
    var dx = this.x - mx;
    var dy = this.y - my;
    var hypo = Math.sqrt((dx * dx) + (dy * dy));
    return (hypo <= this.r);
};


function MethodNode (x, y, w, inset, spacing, fill, label, offset, inputs, outputs) {
    /*
    CONSTRUCTOR
    A MethodNode is a rectangle of constant width (w) and varying height (h)
    where h is proportional to the maximum number of xputs (inputs or outputs).
    h = max(n_inputs, n_ouputs) * spacing
    Holes for inputs and outputs are drawn at some (inset) into the left
    and right sides, respectively.  The width must be greater than 2 * inset.
    */
    this.x = x || 0;
    this.y = y || 0;
    this.w = w || 10;
    this.inputs = inputs;
    this.outputs = outputs;

    this.n_inputs = Object.keys(inputs).length;
    this.n_outputs = Object.keys(outputs).length;

    this.inset = inset || 10; // distance from left or right edge to center of hole
    this.offset = offset || 10; // space between bottom of node and label

    this.spacing = spacing || 10; // vertical separation between pins
    this.h = Math.max(this.n_inputs, this.n_outputs) * this.spacing;
    this.fill = fill || "#AAAAAA";
    this.label = label || '';

    this.in_magnets = [];
    for (var key in this.inputs) {
        var this_input = this.inputs[key];
        var magnet = new Magnet(x = this.x + this.inset,
            y = this.y + this.spacing * (this.in_magnets.length+0.5),
            r = 5,
            attract = 2,
            fill = '#FFFFFF',
            cdt = this_input['cdt_pk'],
            label = this_input['cdt_label']
        );
        this.in_magnets.push(magnet);
    }

    this.out_magnets = [];
    for (key in this.outputs) {
        var this_output = this.outputs[key];
        magnet = new Magnet(x = this.x + this.w - this.inset,
            y = this.y + this.spacing * (this.out_magnets.length+0.5),
            r = 5,
            attract = 2,
            fill = '#FFFFFF',
            cdt = this_output['cdt_pk'],
            label = this_output['cdt_label']
        );
        this.out_magnets.push(magnet);
    }
}

MethodNode.prototype.draw = function(ctx) {
    // draw rectangle
    ctx.fillStyle = this.fill;
    ctx.fillRect(this.x, this.y, this.w, this.h);

    // draw magnets
    for (var i = 0; i < this.in_magnets.length; i++) {
        magnet = this.in_magnets[i];
        magnet.draw(ctx, this.x + this.inset, this.y + this.spacing * (i+0.5));
    }
    for (i = 0; i < this.out_magnets.length; i++) {
        magnet = this.out_magnets[i];
        magnet.draw(ctx, this.x + this.w - this.inset, this.y + this.spacing * (i+0.5));
    }

    // draw label
    ctx.fillStyle = 'black';
    ctx.textAlign = 'center';
    ctx.font = '12pt Lato, sans-serif';
    ctx.fillText(this.label, this.x + this.w / 2, this.y+this.h+this.offset);
};

MethodNode.prototype.contains = function(mx, my) {
    return (this.x <= mx) && (this.x + this.w >= mx) && (this.y <= my) && (this.y + this.h >= my);
};


function Magnet (x, y, r, attract, fill, cdt, label) {
    /*
    CONSTRUCTOR
    A Magnet is the attachment point for a Node (shape) given a
    Connector.  It is always contained within a shape.
     */
    this.x = x;
    this.y = y;
    this.r = r; // radius
    this.attract = attract; // radius of Connector attraction
    this.fill = fill || "#FFFFFF";
    this.cdt = cdt; // primary key to CDT
    this.label = label || '';
}

Magnet.prototype.draw = function(ctx, x, y) {
    // update values passed from shape
    this.x = x;
    this.y = y;
    ctx.beginPath();
    ctx.arc(this.x, this.y, this.r, 0, 2 * Math.PI, true);
    ctx.fillStyle = this.fill;
    ctx.fill();
};

Magnet.prototype.contains = function(mx, my) {
    var dx = this.x - mx;
    var dy = this.y - my;
    var hypo = Math.sqrt((dx * dx) + (dy * dy));
    var result = (hypo <= (this.r + this.attract));
    return (result);
};


function Connector (from_x, from_y, out_magnet) {
    /*
    Constructor.
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
    this.in_magnet = null;
    this.out_magnet = out_magnet || null;

    // is this Connector being drawn from an out-magnet?
    if (this.out_magnet == null) {
        this.fromX = from_x;
        this.fromY = from_y;
    } else {
        this.fromX = out_magnet.x;
        this.fromY = out_magnet.y;
    }

    this.x = this.from_x; // for compatibility with shape-based functions
    this.y = this.from_y;
}

Connector.prototype.draw = function(ctx) {
    /*
    Draw a line to represent a Connector originating from a Magnet.
     */
    ctx.strokeStyle = '#AAAAAA';
    ctx.lineWidth = 6;
    ctx.lineCap = 'round';
    if (this.out_magnet == null) {
        ctx.moveTo(this.fromX, this.fromY);
    } else {
        // attachment to out-magnet of a shape that may have moved
        ctx.moveTo(this.out_magnet.x, this.out_magnet.y);
    }

    if (this.in_magnet == null) {
        // being drawn with no attachment
        ctx.lineTo(this.x, this.y);
    } else {
        // attachment to in-magnet of a shape that may have moved
        ctx.lineTo(this.in_magnet.x, this.in_magnet.y);
    }

    ctx.stroke();
};

Connector.prototype.contains = function(mx, my) {
    /*
    Determine if mouse coordinates are on or close to this
    connector.
     */

}


function CanvasState (canvas) {
    /*
    keeps track of canvas state (mouse drag, etc.)
     */

    // initialize "class"
    this.canvas = canvas;
    this.width = canvas.width;
    this.height = canvas.height;
    this.ctx = canvas.getContext('2d');

    // fixes issues with mouse coordinates
    this.stylePaddingLeft = 0;
    this.stylePaddingTop = 0;
    this.styleBorderLeft = 0;
    this.styleBorderTop = 0;
    if (window.getComputedStyle) {
        this.stylePaddingLeft = parseInt(getComputedStyle(canvas, null).getPropertyValue('padding-left'));
        this.stylePaddingTop = parseInt(getComputedStyle(canvas, null).getPropertyValue('padding-top'));
        this.styleBorderLeft = parseInt(getComputedStyle(canvas, null).getPropertyValue('border-left-width'));
        this.styleBorderTop = parseInt(getComputedStyle(canvas, null).getPropertyValue('border-top-width'));
    }

    // adjust for fixed-position bars at top or left of page
    var html = document.body.parentNode;
    this.htmlTop = html.offsetTop;
    this.htmlLeft = html.offsetLeft;

    this.valid = false; // if false, canvas will redraw everything
    this.shapes = []; // collection of shapes to be drawn
    this.connectors = []; // collection of connectors between shapes
    this.dragging = false; // if mouse drag

    this.selection = null; // reference to active (selected) object
    this.dragoffx = 0; // where in the object we clicked
    this.dragoffy = 0;

    // events
    var myState = this; // save reference to this particular CanvasState

    // de-activate double-click selection of text on page
    canvas.addEventListener('selectstart', function(e) {e.preventDefault(); return false; }, false);

    canvas.addEventListener('mousedown', function(e) {
        myState.doDown(e); // listener registered on mousedown event
    }, true);

    canvas.addEventListener('mousemove', function(e) {
        myState.doMove(e);
    }, true);

    canvas.addEventListener('mouseup', function(e) {
        myState.doUp(e);
    }, true);

    // options
    this.selectionColor = '#9999FF';
    this.selectionWidth = 3;
    setInterval(function() { myState.draw(); }, 30); // 30 ms between redraws
}

CanvasState.prototype.doDown = function(e) {
    var pos = this.getPos(e);
    var mx = pos.x;
    var my = pos.y;

    var shapes = this.shapes;
    var connectors = this.connectors;

    // are we in raw data end-zone?
    if (mx < 0.1*this.width) {
        // create Connector from raw data end-zone
        conn = new Connector(from_x = mx, from_y = my);
        connectors.push(conn);
        this.selection = conn;

        this.dragoffx = mx - conn.fromX;
        this.dragoffy = my - conn.fromY;
        this.dragging = true;
        this.valid = false; // activate canvas

        return;
    }

    var l = shapes.length;
    for (var i = l-1; i >= 0; i--) {
        // check shapes in reverse order
        if (shapes[i].contains(mx, my)) {
            var mySel = shapes[i];
            // are we clicking on an out-magnet?
            out_magnets = mySel.out_magnets;
            for (var j = 0; j < out_magnets.length; j++) {
                out_magnet = out_magnets[j];
                if (out_magnet.contains(mx, my)) {
                    // create Connector from this out-magnet
                    conn = new Connector(null, null, out_magnet);
                    connectors.push(conn);
                    this.selection = conn;
                    this.dragoffx = mx - conn.fromX;
                    this.dragoffy = my - conn.fromY;
                    this.dragging = true;
                    this.valid = false; // activate canvas
                    return;
                }
            }

            // otherwise we are moving the shape
            this.dragoffx = mx - mySel.x;
            this.dragoffy = my - mySel.y;
            this.dragging = true;
            this.selection = mySel;
            this.valid = false;
            return;
        }
    }

    // clicking outside of any shape de-selects any currently-selected shape
    if (this.selection) {
        this.selection = null;
        this.valid = false;
    }
};

CanvasState.prototype.doMove = function(e) {
    /*
    event handler for mouse motion over canvas
     */
    var mouse = this.getPos(e);
    var shapes = this.shapes;
    var i = 0;
    var shape = null;

    if (this.dragging) {
        // are we carrying a shape or Connector?
        if (this.selection != null) {

            this.selection.x = mouse.x - this.dragoffx;
            this.selection.y = mouse.y - this.dragoffy;
            this.valid = false; // redraw

            // are we carrying a connector?
            if (this.selection.fromX != undefined) {
                for (i = 0; i < shapes.length; i++) {
                    shape = shapes[i];
                    if (shape.in_magnets == undefined) {
                        // ignore Connectors
                        continue;
                    }
                    var in_magnets = shape.in_magnets;
                    for (var j = 0; j < in_magnets.length; j++) {
                        var in_magnet = in_magnets[j];

                        var connector_carrying_cdt;
                        if (this.selection.out_magnet == null) {
                            connector_carrying_cdt = '__raw__';
                        } else {
                            connector_carrying_cdt = this.selection.out_magnet.cdt;
                        }

                        if (connector_carrying_cdt == in_magnet.cdt) {
                            // light up magnet
                            in_magnet.fill = 'yellow';
                            if (in_magnet.contains(this.selection.x, this.selection.y)) {
                                // jump to magnet
                                this.selection.x = in_magnet.x;
                                this.selection.y = in_magnet.y;
                                this.selection.in_magnet = in_magnet;
                            }
                        }
                    }
                }
            } else {
                // carrying a shape
                if (mouse.x < 0.1*this.width || mouse.x > 0.9*this.width) {
                    // prevent shapes from being carried into end-zones
                    return;
                }
            }


        }
    }
};

CanvasState.prototype.doUp = function(e) {
    this.dragging = false;
    // check if most recent Connector is linked to a magnet
    var l = this.connectors.length;
    if (l == 0) {
        return; // no Connectors!
    }
    connector = this.connectors[l-1];
    if (connector.in_magnet == null) {
        // has connector been carried into output end-zone?
        var mouse = this.getPos(e);
        if (mouse.x < 0.9 * this.canvas.width || connector.out_magnet == undefined) {
            // not connected, delete Connector
            this.connectors.pop();
            this.valid = false; // redraw canvas to remove this Connector
        }
    } else if (connector.in_magnet.parent()) {

    }

    // turn off all in-magnets
    var shapes = this.shapes;
    for (var i = 0; i < shapes.length; i++) {
        var shape = shapes[i];
        if (shape.in_magnets != undefined) {
            var in_magnets = shape.in_magnets;
            for (var j = 0; j < in_magnets.length; j++) {
                var in_magnet = in_magnets[j];
                in_magnet.fill = 'white';
            }
        }
    }
};

CanvasState.prototype.addShape = function(shape) {
    this.shapes.push(shape);
    this.valid = false;
};

CanvasState.prototype.clear = function() {
    // wipe canvas content clean before redrawing
    this.ctx.clearRect(0, 0, this.width, this.height);

    this.ctx.textAlign = 'center';
    this.ctx.font = '12pt Lato, sans-serif';

    // draw raw data end-zone
    this.ctx.fillStyle = '#AAFFAA';
    this.ctx.fillRect(0, 0, this.width * 0.1, this.height);
    this.ctx.fillStyle = 'black';
    this.ctx.fillText('Raw data', this.width * 0.05, 20);

    // draw output end-zone
    this.ctx.fillStyle = '#FFAAAA';
    this.ctx.fillRect(this.width * 0.9, 0, this.width, this.height);
    this.ctx.fillStyle = 'black';
    this.ctx.fillText('Output', this.width * 0.95, 20);
};

CanvasState.prototype.draw = function() {
    if (!this.valid) {
        var ctx = this.ctx;
        var shapes = this.shapes;
        var connectors = this.connectors;
        this.clear();

        // draw all shapes and magnets
        var l = shapes.length;
        for (var i = 0; i < l; i++) {
            var shape = shapes[i];
            // skip shapes moved off the screen
            if (shape.x > this.width || shape.y > this.height || shape.x + 2*shape.r < 0 || shape.y + 2*shape.r < 0) {
                continue;
            }
            shapes[i].draw(ctx);
        }

        // draw all connectors
        var l = connectors.length;
        for (var i = 0; i < l; i++) {
            connectors[i].draw(ctx);
        }

        if (this.selection != null) {
            // draw selection ring
            ctx.strokeStyle = this.selectionColor;
            ctx.lineWidth = this.selectionWidth;
            var mySel = this.selection;
            if (mySel.r == undefined) {
                ctx.beginPath();
                ctx.rect(mySel.x, mySel.y, mySel.w, mySel.h);
                ctx.closePath();
                ctx.stroke();
            } else {
                ctx.beginPath();
                ctx.arc(mySel.x, mySel.y, mySel.r, 0, 2*Math.PI, false);
                ctx.closePath();
                ctx.stroke();
            }
        }

        this.valid = true;
    }
};

CanvasState.prototype.getPos = function(e) {
    // returns a JavaScript object with x, y coordinates defined
    var element = this.canvas, offsetX = 0, offsetY = 0, mx, my;

    if (element.offsetParent !== undefined) {
        do {
            offsetX += element.offsetLeft;
            offsetY += element.offsetTop;
        } while ((element = element.offsetParent));
    }

    offsetX += this.stylePaddingLeft + this.styleBorderLeft + this.htmlLeft;
    offsetY += this.stylePaddingTop + this.styleBorderTop + this.htmlTop;

    mx = e.pageX - offsetX;
    my = e.pageY - offsetY;

    return {x: mx, y: my};
};

CanvasState.prototype.deleteObject = function() {
    // delete selected object
    var mySel = this.selection;
    if (mySel != null) {
        if (mySel.isPrototypeOf(Connector)) {
            // remove selected Connector from list

        } else if (mySel.isPrototypeOf(MethodNode)) {
            // delete Connectors associated with MethodNode
            // remove MethodNode from list
        } else {

        }
    }
}