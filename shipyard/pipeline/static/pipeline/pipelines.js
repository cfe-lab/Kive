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
        var node_label = $('#id_select_cdt option:selected').text();
        canvasState.addShape(new CDT_Node(100, 200+50*Math.random(), 20, '#55FF55', node_label));
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
                        canvasState.addShape(new MethodNode(200, 200, 80, 10, 20, '#999999', node_label, 14, inputs, outputs));
                    }
                });
            }
        }
    });

    $('#id_reset_button').on('click', function() {
        canvasState.clear();
        canvasState.shapes = [];
    });
});


/*
 The following code is based on canvas interactivity example by Simon Sarris.
 HTML5 Unleashed (2014) Pearson Education Inc.
 */
function CDT_Node (x, y, r, fill, label) {
    // CDT node constructor with default values
    this.x = x || 0; // defaults to top left corner
    this.y = y || 0;
    this.r = r || 10; // radius
    this.fill = fill || "#AAAAAA";
    this.label = label || '';
}

CDT_Node.prototype.draw = function(ctx) {
    ctx.fillStyle = this.fill;
    ctx.beginPath();
    ctx.arc(this.x, this.y, this.r, 0, 2 * Math.PI, false);
    ctx.closePath();
    ctx.fillStyle = this.fill;
    ctx.fill();
    ctx.fillStyle = 'black';
    ctx.textAlign = 'center';
    ctx.font = '12pt Lato, sans-serif';
    ctx.fillText(this.label, this.x, this.y+40);
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

    this.inset = inset || 10; // distance in for holes
    this.offset = offset || 10; // space between bottom of node and label

    this.spacing = spacing || 10; // vertical separation between pins
    this.h = Math.max(this.n_inputs, this.n_outputs) * this.spacing;
    this.fill = fill || "#AAAAAA";
    this.label = label || '';

    this.in_magnets = [];
    for (var i = 0; i < this.n_inputs; i++) {
        this_input = inputs[i+1]; // 1-indexed
        magnet = new Magnet(x = this.x + this.inset,
            y = this.y + this.spacing * (i+0.5),
            r = 5,
            attract = 2,
            fill = '#FFFFFF',
            cdt = this_input['cdt_pk'],
            label = this_input['cdt_label']
        );
        this.in_magnets.push(magnet);
    }

    this.out_magnets = [];
    for (var key in this.outputs) {
        var this_output = this.outputs[key];
        magnet = new Magnet(x = this.x + this.w - this.inset,
            y = this.y + this.spacing * (i+0.5),
            r = 5,
            attract = 2,
            fill = '#FFFFFF',
            cdt = this_output['cdt_pk'],
            label = this_output['cdt_label']
        );
        this.out_magnets.push(magnet);
    }

    console.log(this.out_magnets);
}

MethodNode.prototype.draw = function(ctx) {
    ctx.fillStyle = this.fill;
    ctx.fillRect(this.x, this.y, this.w, this.h);

    for (var i = 0; i < this.in_magnets.length; i++) {
        magnet = this.in_magnets[i];
        magnet.draw(ctx, this.x + this.inset, this.y + this.spacing * (i+0.5));
    }
    for (var i = 0; i < this.out_magnets.length; i++) {
        magnet = this.out_magnets[i];
        magnet.draw(ctx, this.x + this.w - this.inset, this.y + this.spacing * (i+0.5));
    }

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
    ctx.beginPath();
    ctx.arc(x, y, this.r, 0, 2 * Math.PI, true);
    ctx.closePath();
    ctx.fillStyle = this.fill;
    ctx.fill();
};

Magnet.prototype.contains = function(mx, my) {
    var dx = this.x - mx;
    var dy = this.y - my;
    var hypo = Math.sqrt((dx * dx) + (dy * dy));
    return (hypo <= this.r + this.attract);
};


function Connector (fromX, fromY, toX, toY) {
    /*
    A Connector is a line drawn between two Magnets.
     */
    this.fromX = fromX;
    this.fromY = fromY;
    this.toX = toX;
    this.toY = toY;
}

Connector.prototype.draw = function(ctx) {
    ctx.strokeStyle = 'black';
    ctx.beginPath();
    ctx.moveTo(this.fromX, this.fromY);
    ctx.lineTo(this.toX, this.toY);
    ctx.closePath();
};


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
    this.connections = []; // collection of connectors between shapes
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
    // are we in an end-zone?
    if (mx < 0.1*this.width) {
        console.log('left endzone');

        this.dragoffx = mx;
        this.dragoffy = my;
        this.dragging = true;
        this.selection = null; // use this to indicate that we are drawing an edge
        this.valid = false; // activate canvas
        return;
    }
    if (mx > 0.9*this.width) {
        console.log('right endzone');
    }

    var shapes = this.shapes;
    var connections = this.connections;
    var l = shapes.length;
    for (var i = l-1; i >= 0; i--) {
        // check shapes in reverse order
        if (shapes[i].contains(mx, my)) {
            var mySel = shapes[i];
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
    if (this.dragging) {
        var mouse = this.getPos(e);

        if (this.selection == null) {
            // draw a line

        } else {
            if (mouse.x < 0.1*this.width || mouse.x > 0.9*this.width) {
                // prevent shapes from moving into end-zones
                return;
            }

            // move a shape
            this.selection.x = mouse.x - this.dragoffx;
            this.selection.y = mouse.y - this.dragoffy;
            this.valid = false; // redraw
        }
    }
};

CanvasState.prototype.doUp = function(e) {
    this.dragging = false;
};

CanvasState.prototype.addShape = function(shape) {
    this.shapes.push(shape);
    this.valid = false;
};

CanvasState.prototype.clear = function() {
    // wipe canvas content clean before redrawing
    this.ctx.clearRect(0, 0, this.width, this.height);

    // draw raw data end-zone
    this.ctx.beginPath();
    this.ctx.rect(0, 0, this.width * 0.1, this.height);
    this.ctx.closePath();
    this.ctx.fillStyle = '#AAFFAA';
    this.ctx.fill();

    // draw output end-zone
    this.ctx.beginPath();
    this.ctx.rect(this.width * 0.9, 0, this.width, this.height);
    this.ctx.closePath();
    this.ctx.fillStyle = '#FFAAAA';
    this.ctx.fill();
};

CanvasState.prototype.draw = function() {
    if (!this.valid) {
        var ctx = this.ctx;
        var shapes = this.shapes;
        this.clear();

        // draw all shapes
        var l = shapes.length;
        for (var i = 0; i < l; i++) {
            var shape = shapes[i];
            // skip shapes moved off the screen
            if (shape.x > this.width || shape.y > this.height || shape.x + 2*shape.r < 0 || shape.y + 2*shape.r < 0) {
                continue;
            }
            shapes[i].draw(ctx);
        }

        if (this.selection == null) {
            // draw Connector
        } else {
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
