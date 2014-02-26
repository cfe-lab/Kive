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
        canvasState.addShape(new CDT_Node(50, 100, 20, '#55FF55', node_label));
    });

    $('#id_method_button').on('click', function() {
        var selected = $('#id_select_method option:selected');
        if (selected.val() != "") {
            var node_label = selected.text();
            canvasState.addShape(new MethodNode(200, 200, 80, 60, '#999999', node_label));
        }
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
    ctx.fillStyle = this.fill;
    ctx.fill();
    ctx.fillStyle = 'black';
    ctx.textAlign = 'center';
    ctx.font = '12pt Lato, sans-serif';
    ctx.fillText(this.label, this.x, this.y+40);
}

CDT_Node.prototype.contains = function(mx, my) {
    // determine if mouse pointer coordinates (mx, my) are
    // within this shape's bounds - compare length of hypotenuse
    // to radius
    var dx = this.x - mx;
    var dy = this.y - my;
    var hypo = Math.sqrt((dx * dx) + (dy * dy));
    return (hypo <= this.r);
}


function MethodNode (x, y, w, h, fill, label) {
    this.x = x || 0;
    this.y = y || 0;
    this.w = w || 10;
    this.h = h || 10;
    this.fill = fill || "#AAAAAA";
    this.label = label || '';
}

MethodNode.prototype.draw = function(ctx) {
    ctx.fillStyle = this.fill;
    ctx.fillRect(this.x, this.y, this.w, this.h);
    ctx.fillStyle = 'black';
    ctx.textAlign = 'center';
    ctx.font = '12pt Lato, sans-serif';
    ctx.fillText(this.label, this.x + this.w / 2, this.y+80);
}

MethodNode.prototype.contains = function(mx, my) {
    return (this.x <= mx) && (this.x + this.w >= mx) && (this.y <= my) && (this.y + this.h >= my);
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
}

CanvasState.prototype.doMove = function(e) {
    if (this.dragging) {
        var mouse = this.getPos(e);
        this.selection.x = mouse.x - this.dragoffx;
        this.selection.y = mouse.y - this.dragoffy;
        this.valid = false; // redraw
    }
}

CanvasState.prototype.doUp = function(e) {
    this.dragging = false;
}

CanvasState.prototype.addShape = function(shape) {
    this.shapes.push(shape);
    this.valid = false;
}

CanvasState.prototype.clear = function() {
    this.ctx.clearRect(0, 0, this.width, this.height);
}

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

        // draw selection ring
        if (this.selection != null) {
            ctx.strokeStyle = this.selectionColor;
            ctx.lineWidth = this.selectionWidth;
            var mySel = this.selection;
            if (mySel.r == undefined) {
                ctx.beginPath();
                ctx.rect(mySel.x, mySel.y, mySel.w, mySel.h);
                ctx.stroke();
            } else {
                ctx.arc(mySel.x, mySel.y, mySel.r, 0, 2*Math.PI, false);
                ctx.stroke();
            }
        }

        this.valid = true;
    }
}

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
}

function init () {

}


