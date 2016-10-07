System.config({
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