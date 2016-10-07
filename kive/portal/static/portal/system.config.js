System.config({
    paths: {
        // Unit tests
        'jasmine' : 'portal/js/jasmine-2.3.4/jasmine.js',
        'jasmine-html' : 'portal/js/jasmine-2.3.4/jasmine-html.js',
        'jasmine-boot' : 'portal/js/jasmine-2.3.4/boot.js'
    },
    // packages tells the System loader how to load when no filename and/or no extension
    packages: {
        '/static/sandbox': {
            format: 'register',
            main: 'view_run.js',
            defaultExtension: 'js'
        },
        '/static/pipeline': {
            format: 'register',
            main: 'pipeline_add.js',
            defaultExtension: 'js'
        }
    }
});