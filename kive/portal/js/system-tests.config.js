System.config({
    paths: {
        // Unit tests
        'jasmine' : 'portal/js/jasmine-2.3.4/jasmine.js',
        'jasmine-html' : 'portal/js/jasmine-2.3.4/jasmine-html.js',
        'jasmine-boot' : 'portal/js/jasmine-2.3.4/boot.js',
        'jasmine-jquery' : 'portal/js/jasmine-jquery.js',
        'jasmine-ajax' : 'portal/js/mock-ajax.js',
        'imagediff' : 'portal/js/imagediff.js',
        jquery_raw : '/static/portal/jquery-2.0.3.min.js',
        jquery : '/static/portal/noxss_jq.js'
    },
    meta: {
        // meaning [baseURL]/vendor/angular.js when no other rules are present
        // path is normalized using map and paths configuration
        'jasmine': {
            format: 'global', // load this module as a global
            deps: []
        },
        'jasmine-html': {
            format: 'global', // load this module as a global
            deps: [ 'jasmine' ]
        },
        'jasmine-boot': {
            format: 'global', // load this module as a global
            deps: [ 'jasmine', 'jasmine-html' ]
        },
        'jasmine-jquery': {
            format: 'global', // load this module as a global
            deps: [ 'jasmine', 'jasmine-html', 'jasmine-boot' ]
        },
        'imagediff': {
            format: 'global', // load this module as a global
            deps: [ 'jasmine', 'jasmine-html', 'jasmine-boot' ]
        }
    }
    // packages tells the System loader how to load when no filename and/or no extension
    // packages: {
    //     '/static/sandbox': {
    //         format: 'register',
    //         main: 'view_run.js',
    //         defaultExtension: 'js'
    //     },
    //     '/static/pipeline': {
    //         format: 'register',
    //         main: 'pipeline_add.js',
    //         defaultExtension: 'js'
    //     }
    // }
});