/*
from https://docs.djangoproject.com/en/1.7/ref/contrib/csrf/#ajax
On each XMLHttpRequest, set a custom X-CSRFToken header to the value of the CSRF token.
ajaxSend is a function to be executed before an Ajax request is sent.
*/
"use strict";

import 'jquery_raw';
let csrftoken;

if (document.cookie) {
    let name = 'csrftoken';
    let cookies = document.cookie.split(';');
    for (let i = 0; i < cookies.length; i++) {
        let cookie = $.trim(cookies[i]);
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
} else {
    console.error('Warning: no CSRF token found.');
}

function csrfSafeMethod(method) {
    // these HTTP methods do not require CSRF protection
    return /^(GET|HEAD|OPTIONS|TRACE)$/.test(method);
}

export default $;