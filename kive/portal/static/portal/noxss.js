/*
from https://docs.djangoproject.com/en/1.7/ref/contrib/csrf/#ajax
On each XMLHttpRequest, set a custom X-CSRFToken header to the value of the CSRF token.
ajaxSend is a function to be executed before an Ajax request is sent.
*/

var installNoXss = function($) {
    "use strict";
    
    var csrftoken;
    var name = 'csrftoken';
    var cookies, cookie, i;
    
    if (document.cookie) {
        cookies = document.cookie.split(';');
        for (i = 0; i < cookies.length; i++) {
            cookie = $.trim(cookies[i]);
            // Does this cookie string begin with the name we want?
            if (cookie.substring(0, name.length + 1) === name + '=') {
                csrftoken = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    
    if (csrftoken) {
        console.log('Cross-origin script protection installed.', 'Key: ' + csrftoken);
        $.ajaxSetup({
            beforeSend: function (xhr, settings) {
                if (!csrfSafeMethod(settings.type) && !this.crossDomain) {
                    xhr.setRequestHeader("X-CSRFToken", csrftoken);
                }
            }
        });
        
        /*
         * Debugger for tracking which jQuery instance we installed this on.
        
        window.mycounter = window.mycounter || 0;
        if ($.mycounter !== undefined) {
            $.mycounter.push(window.mycounter++);
        } else {
            $.mycounter = [ window.mycounter++ ];
        }
        console.log('NoXss instances on this jQuery instance: ', $.mycounter);
        
         */
        
        installNoXss = function() {
            console.log('No-XSS already installed.');
        };
    } else {
        console.error('Warning: no CSRF token found.');
    }
    
    function csrfSafeMethod(method) {
        // these HTTP methods do not require CSRF protection
        return /^(GET|HEAD|OPTIONS|TRACE)$/.test(method);
    }
};

if (typeof jQuery !== 'undefined') {
    installNoXss(jQuery);
}

function noXSS() {
    console.log('Legacy noXSS() called.');
    if (typeof jQuery !== 'undefined') {
        installNoXss(jQuery);
    }
}