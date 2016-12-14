System.config({
    paths: {
        'static/pipeline/pipeline_all': '/static/pipeline/pipeline_all',
        jquery : '/static/portal/jquery-2.0.3.min.js',
        noXss  : '/static/portal/noxss.js'
    },
    bundles: {
        noXss: ['jquery']
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